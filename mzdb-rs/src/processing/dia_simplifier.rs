//! DIA Simplifier - Simplify DIA mzDB files using detected MS2 peakels
//!
//! This module provides functionality to simplify DIA (Data Independent Acquisition) mzDB files
//! by reconstructing MS2 spectra from detected peakels. For each peakel, only the apex and
//! surrounding data points are retained, significantly reducing file size while preserving
//! the essential signal information.
//!
//! # Process
//!
//! 1. Read MS2 peakels from peakeldb (includes full intensity profile per peakel)
//! 2. For each peakel, extract data points around the apex (e.g., apex-1, apex, apex+1)
//! 3. Group all peakel data points by spectrum_id and isolation window
//! 4. Sort peaks by m/z within each spectrum
//! 5. Write simplified mzDB with original MS1 and reconstructed MS2 spectra
//!
//! # Example
//!
//! ```no_run
//! use mzdb::processing::dia_simplifier::{DiaSimplifier, DiaSimplifierConfig};
//!
//! let mzdb_path = "dia_file.mzDB";
//! let peakeldb_path = "peakels.peakeldb";
//! let output_path = std::path::PathBuf::from("simplified.mzDB");
//!
//! let simplifier = DiaSimplifier::new(DiaSimplifierConfig::default());
//! simplifier.simplify(mzdb_path, peakeldb_path, &output_path).unwrap();
//! ```

use std::collections::{BTreeMap, HashMap};
use std::path::Path;

use anyhow::{Context, Result};
use ordered_float::OrderedFloat;
use rmpv::Value;
use rusqlite::{params, Connection};

use crate::processing::dia::IsolationWindow;

// ============================================================================
// Configuration
// ============================================================================

/// Configuration for DIA simplification
#[derive(Clone, Debug)]
pub struct DiaSimplifierConfig {
    /// Number of data points to retain per peakel (centered on apex)
    /// Must be odd (1, 3, 5, etc.)
    pub points_per_peakel: usize,
    /// m/z merge tolerance - peaks within this distance are merged
    pub mz_merge_tolerance: f64,
}

impl Default for DiaSimplifierConfig {
    fn default() -> Self {
        Self {
            points_per_peakel: 3,
            mz_merge_tolerance: 0.001,
        }
    }
}

impl DiaSimplifierConfig {
    /// Create a new config with specified points per peakel
    pub fn with_points(points_per_peakel: usize) -> Result<Self> {
        if points_per_peakel % 2 == 0 {
            anyhow::bail!("points_per_peakel must be odd (1, 3, 5, etc.)");
        }
        Ok(Self {
            points_per_peakel,
            ..Default::default()
        })
    }
}

// ============================================================================
// Peakel Data Structures
// ============================================================================

/// A peakel read from the peakeldb with full peaks data
#[derive(Debug, Clone)]
pub struct SimplifierPeakel {
    pub id: i64,
    pub mz: f64,
    pub elution_time: f32,
    pub duration: f32,
    pub gap_count: i32,
    pub apex_intensity: f32,
    pub area: f32,
    pub amplitude: f32,
    pub peaks_count: i32,
    pub first_spectrum_id: i64,
    pub apex_spectrum_id: i64,
    pub last_spectrum_id: i64,
    pub isolation_window_id: i64,
    pub precursor_mz: f64,
    /// Spectrum IDs at each data point
    pub spectrum_ids: Vec<i64>,
    /// Retention times at each data point (seconds)
    pub elution_times: Vec<f32>,
    /// m/z values at each data point
    pub mz_values: Vec<f64>,
    /// Intensities at each data point
    pub intensities: Vec<f32>,
}

impl SimplifierPeakel {
    /// Get the index of the apex spectrum in this peakel's data arrays
    pub fn apex_index(&self) -> Option<usize> {
        self.spectrum_ids
            .iter()
            .position(|&id| id == self.apex_spectrum_id)
    }
}

/// A data point extracted from a peakel
#[derive(Debug, Clone)]
struct PeakelDataPoint {
    /// The spectrum ID this data point belongs to
    spectrum_id: i64,
    /// The m/z value at this data point
    mz: f64,
    /// The intensity at this data point
    intensity: f32,
    /// The precursor/isolation window target m/z
    precursor_mz: f64,
    /// The isolation window bounds
    isolation_lower: f64,
    isolation_upper: f64,
}

/// A reconstructed simplified spectrum
#[derive(Debug, Clone)]
pub struct SimplifiedSpectrum {
    /// Cycle number
    pub cycle: i32,
    /// Retention time
    pub time: f32,
    /// Precursor m/z (isolation window target)
    pub precursor_mz: f64,
    /// Isolation window lower bound
    pub isolation_lower: f64,
    /// Isolation window upper bound
    pub isolation_upper: f64,
    /// m/z values (sorted)
    pub mz_array: Vec<f64>,
    /// Intensity values
    pub intensity_array: Vec<f32>,
}

/// Spectrum header information (minimal)
#[derive(Debug, Clone)]
pub struct SpectrumHeader {
    pub id: i64,
    pub cycle: i32,
    pub time: f32,
}

// ============================================================================
// Peakeldb Reader
// ============================================================================

/// Reader for peakeldb SQLite files created by mzdb2peakeldb
pub struct PeakelDbReader {
    conn: Connection,
}

impl PeakelDbReader {
    /// Open a peakeldb file
    pub fn open(path: &str) -> Result<Self> {
        let conn =
            Connection::open(path).context("Failed to open peakeldb file")?;
        Ok(Self { conn })
    }

    /// Read all isolation windows from the database
    pub fn read_isolation_windows(&self) -> Result<Vec<IsolationWindow>> {
        let mut stmt = self.conn.prepare(
            "SELECT id, target_mz, lower_mz, upper_mz, spectrum_count
             FROM isolation_window
             ORDER BY id",
        )?;

        let windows = stmt.query_map([], |row| {
            Ok(IsolationWindow {
                id: row.get(0)?,
                target_mz: row.get(1)?,
                lower_mz: row.get(2)?,
                upper_mz: row.get(3)?,
                spectrum_count: row.get::<_, i64>(4)? as usize,
            })
        })?;

        windows
            .collect::<Result<Vec<_>, _>>()
            .context("Failed to read isolation windows")
    }

    /// Read all peakels from the database
    pub fn read_all_peakels(&self) -> Result<Vec<SimplifierPeakel>> {
        let mut stmt = self.conn.prepare(
            "SELECT id, mz, elution_time, duration, gap_count, apex_intensity,
                    area, amplitude, peaks_count, first_spectrum_id, 
                    apex_spectrum_id, last_spectrum_id, isolation_window_id,
                    precursor_mz, peaks
             FROM peakel
             ORDER BY id",
        )?;

        let peakel_iter = stmt.query_map([], |row| {
            let peaks_blob: Vec<u8> = row.get(14)?;
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, f64>(1)?,
                row.get::<_, f32>(2)?,
                row.get::<_, f32>(3)?,
                row.get::<_, i32>(4)?,
                row.get::<_, f32>(5)?,
                row.get::<_, f32>(6)?,
                row.get::<_, f32>(7)?,
                row.get::<_, i32>(8)?,
                row.get::<_, i64>(9)?,
                row.get::<_, i64>(10)?,
                row.get::<_, i64>(11)?,
                row.get::<_, i64>(12)?,
                row.get::<_, f64>(13)?,
                peaks_blob,
            ))
        })?;

        let mut peakels = Vec::new();

        for result in peakel_iter {
            let (
                id,
                mz,
                elution_time,
                duration,
                gap_count,
                apex_intensity,
                area,
                amplitude,
                peaks_count,
                first_spectrum_id,
                apex_spectrum_id,
                last_spectrum_id,
                isolation_window_id,
                precursor_mz,
                peaks_blob,
            ) = result?;

            // Parse the MessagePack peaks blob
            let (spectrum_ids, elution_times, mz_values, intensities) =
                parse_peaks_blob(&peaks_blob)?;

            peakels.push(SimplifierPeakel {
                id,
                mz,
                elution_time,
                duration,
                gap_count,
                apex_intensity,
                area,
                amplitude,
                peaks_count,
                first_spectrum_id,
                apex_spectrum_id,
                last_spectrum_id,
                isolation_window_id,
                precursor_mz,
                spectrum_ids,
                elution_times,
                mz_values,
                intensities,
            });
        }

        log::info!("Loaded {} peakels from peakeldb", peakels.len());
        Ok(peakels)
    }
}

/// Parse the MessagePack-encoded peaks blob
///
/// The format is an array of 4 arrays:
/// [spectrum_ids (Long), elution_times (Float), mz_values (Double), intensity_values (Float)]
fn parse_peaks_blob(data: &[u8]) -> Result<(Vec<i64>, Vec<f32>, Vec<f64>, Vec<f32>)> {
    let value: Value = rmpv::decode::read_value(&mut &data[..])
        .context("Failed to decode MessagePack data")?;

    if let Value::Array(arrays) = value {
        if arrays.len() != 4 {
            anyhow::bail!(
                "Expected 4 arrays in peaks blob, got {}",
                arrays.len()
            );
        }

        let spectrum_ids = extract_i64_array(&arrays[0])?;
        let elution_times = extract_f32_array(&arrays[1])?;
        let mz_values = extract_f64_array(&arrays[2])?;
        let intensities = extract_f32_array(&arrays[3])?;

        Ok((spectrum_ids, elution_times, mz_values, intensities))
    } else {
        anyhow::bail!("Expected array in peaks blob, got {:?}", value);
    }
}

fn extract_i64_array(value: &Value) -> Result<Vec<i64>> {
    if let Value::Array(arr) = value {
        arr.iter()
            .map(|v| match v {
                Value::Integer(i) => Ok(i.as_i64().unwrap_or(0)),
                _ => Ok(0),
            })
            .collect()
    } else {
        anyhow::bail!("Expected array, got {:?}", value);
    }
}

fn extract_f32_array(value: &Value) -> Result<Vec<f32>> {
    if let Value::Array(arr) = value {
        arr.iter()
            .map(|v| match v {
                Value::F32(f) => Ok(*f),
                Value::F64(f) => Ok(*f as f32),
                Value::Integer(i) => Ok(i.as_f64().unwrap_or(0.0) as f32),
                _ => Ok(0.0),
            })
            .collect()
    } else {
        anyhow::bail!("Expected array, got {:?}", value);
    }
}

fn extract_f64_array(value: &Value) -> Result<Vec<f64>> {
    if let Value::Array(arr) = value {
        arr.iter()
            .map(|v| match v {
                Value::F64(f) => Ok(*f),
                Value::F32(f) => Ok(*f as f64),
                Value::Integer(i) => Ok(i.as_f64().unwrap_or(0.0)),
                _ => Ok(0.0),
            })
            .collect()
    } else {
        anyhow::bail!("Expected array, got {:?}", value);
    }
}

// ============================================================================
// DIA Simplifier
// ============================================================================

/// DIA Simplifier - Simplifies DIA mzDB files using detected MS2 peakels
pub struct DiaSimplifier {
    config: DiaSimplifierConfig,
}

impl DiaSimplifier {
    /// Create a new simplifier with default configuration
    pub fn new(config: DiaSimplifierConfig) -> Self {
        Self { config }
    }

    /// Simplify a DIA mzDB file using peakel data
    ///
    /// # Arguments
    /// * `mzdb_path` - Path to the input DIA mzDB file
    /// * `peakeldb_path` - Path to the peakeldb file
    /// * `output_path` - Path for the output simplified mzDB file
    pub fn simplify(
        &self,
        mzdb_path: &str,
        peakeldb_path: &str,
        output_path: &Path,
    ) -> Result<SimplificationStats> {
        log::info!("DIA Simplifier");
        log::info!("Input mzDB: {}", mzdb_path);
        log::info!("Input peakeldb: {}", peakeldb_path);
        log::info!("Output: {:?}", output_path);
        log::info!("Points per peakel: {}", self.config.points_per_peakel);

        // Open input files
        log::info!("Opening input files...");
        let mzdb_conn = Connection::open(mzdb_path)
            .context("Failed to open mzDB file")?;
        let peakeldb = PeakelDbReader::open(peakeldb_path)
            .context("Failed to open peakeldb file")?;

        // Read isolation windows
        log::info!("Reading isolation windows...");
        let isolation_windows = peakeldb.read_isolation_windows()?;
        log::info!("Found {} isolation windows", isolation_windows.len());

        // Build isolation window lookup
        let window_lookup: HashMap<i64, IsolationWindow> = isolation_windows
            .into_iter()
            .map(|w| (w.id, w))
            .collect();

        // Read peakels
        log::info!("Reading peakels...");
        let peakels = peakeldb.read_all_peakels()?;
        let peakel_count = peakels.len();
        log::info!("Loaded {} peakels", peakel_count);

        // Read MS2 spectrum headers to get cycle and time info
        log::info!("Reading spectrum headers...");
        let ms2_headers = get_ms2_spectrum_headers(&mzdb_conn)?;
        let original_ms2_count = ms2_headers.len();
        log::info!("Found {} MS2 spectra", original_ms2_count);

        // Build spectrum_id -> (cycle, time) lookup
        let spectrum_info: HashMap<i64, (i32, f32)> = ms2_headers
            .iter()
            .map(|h| (h.id, (h.cycle, h.time)))
            .collect();

        // Extract data points from peakels
        log::info!("Extracting peakel data points...");
        let half_window = self.config.points_per_peakel / 2;
        let data_points =
            extract_peakel_data_points(&peakels, &window_lookup, half_window)?;
        let data_point_count = data_points.len();
        log::info!("Extracted {} data points", data_point_count);

        // Group data points into simplified spectra
        log::info!("Grouping data points by spectrum...");
        let simplified_spectra = group_into_spectra(
            data_points,
            &spectrum_info,
            self.config.mz_merge_tolerance,
        );
        let simplified_ms2_count = simplified_spectra.len();
        log::info!("Created {} simplified spectra", simplified_ms2_count);

        // Write output
        log::info!("Writing simplified DIA mzDB file...");
        write_simplified_dia_mzdb(
            mzdb_path,
            &simplified_spectra,
            output_path,
        )?;

        let stats = SimplificationStats {
            peakel_count,
            original_ms2_count,
            simplified_ms2_count,
            data_point_count,
        };

        log::info!("Done! Stats: {:?}", stats);

        Ok(stats)
    }
}

impl Default for DiaSimplifier {
    fn default() -> Self {
        Self::new(DiaSimplifierConfig::default())
    }
}

/// Statistics from the simplification process
#[derive(Debug, Clone)]
pub struct SimplificationStats {
    pub peakel_count: usize,
    pub original_ms2_count: usize,
    pub simplified_ms2_count: usize,
    pub data_point_count: usize,
}

// ============================================================================
// Core Processing Functions
// ============================================================================

/// Get MS2 spectrum headers (id, cycle, time)
fn get_ms2_spectrum_headers(conn: &Connection) -> Result<Vec<SpectrumHeader>> {
    let mut stmt = conn.prepare(
        "SELECT id, cycle, time
         FROM spectrum
         WHERE ms_level = 2
         ORDER BY id",
    )?;

    let headers = stmt.query_map([], |row| {
        Ok(SpectrumHeader {
            id: row.get(0)?,
            cycle: row.get(1)?,
            time: row.get(2)?,
        })
    })?;

    headers
        .collect::<Result<Vec<_>, _>>()
        .context("Failed to read MS2 spectrum headers")
}

/// Extract data points from peakels using the peakel's own intensity data
fn extract_peakel_data_points(
    peakels: &[SimplifierPeakel],
    window_lookup: &HashMap<i64, IsolationWindow>,
    half_window: usize,
) -> Result<Vec<PeakelDataPoint>> {
    let mut data_points = Vec::new();

    for (i, peakel) in peakels.iter().enumerate() {
        if i % 10000 == 0 && i > 0 {
            log::debug!("Processing peakel {}/{}", i, peakels.len());
        }

        // Get isolation window
        let window = match window_lookup.get(&peakel.isolation_window_id) {
            Some(w) => w,
            None => continue,
        };

        // Find apex index in the peakel's data arrays
        let apex_idx = match peakel.apex_index() {
            Some(idx) => idx,
            None => {
                // Fallback: use middle of array
                peakel.spectrum_ids.len() / 2
            }
        };

        // Calculate range of indices to include (apex ± half_window)
        let start_idx = apex_idx.saturating_sub(half_window);
        let end_idx =
            (apex_idx + half_window).min(peakel.spectrum_ids.len().saturating_sub(1));

        // Extract data points from the selected indices
        for idx in start_idx..=end_idx {
            if idx < peakel.spectrum_ids.len() {
                data_points.push(PeakelDataPoint {
                    spectrum_id: peakel.spectrum_ids[idx],
                    mz: peakel.mz_values[idx],
                    intensity: peakel.intensities[idx],
                    precursor_mz: window.target_mz,
                    isolation_lower: window.lower_mz,
                    isolation_upper: window.upper_mz,
                });
            }
        }
    }

    Ok(data_points)
}

/// Group data points into simplified spectra
fn group_into_spectra(
    data_points: Vec<PeakelDataPoint>,
    spectrum_info: &HashMap<i64, (i32, f32)>,
    mz_merge_tolerance: f64,
) -> Vec<SimplifiedSpectrum> {
    // Group by (spectrum_id, precursor_mz)
    let mut groups: BTreeMap<(i64, OrderedFloat<f64>), Vec<PeakelDataPoint>> =
        BTreeMap::new();

    for dp in data_points {
        let key = (dp.spectrum_id, OrderedFloat(dp.precursor_mz));
        groups.entry(key).or_default().push(dp);
    }

    // Convert to simplified spectra
    let mut spectra = Vec::new();

    for ((spectrum_id, _precursor_mz), mut points) in groups {
        // Sort by m/z
        points.sort_by(|a, b| a.mz.partial_cmp(&b.mz).unwrap());

        // Merge duplicate m/z values (sum intensities)
        let mut merged_mz: Vec<f64> = Vec::new();
        let mut merged_intensity: Vec<f32> = Vec::new();

        for dp in &points {
            if merged_mz.is_empty()
                || (dp.mz - merged_mz.last().unwrap()).abs() > mz_merge_tolerance
            {
                merged_mz.push(dp.mz);
                merged_intensity.push(dp.intensity);
            } else {
                // Same m/z, sum intensities
                *merged_intensity.last_mut().unwrap() += dp.intensity;
            }
        }

        // Get cycle and time from spectrum info
        let (cycle, time) = match spectrum_info.get(&spectrum_id) {
            Some(&info) => info,
            None => continue,
        };

        let first_point = &points[0];

        spectra.push(SimplifiedSpectrum {
            cycle,
            time,
            precursor_mz: first_point.precursor_mz,
            isolation_lower: first_point.isolation_lower,
            isolation_upper: first_point.isolation_upper,
            mz_array: merged_mz,
            intensity_array: merged_intensity,
        });
    }

    // Sort by cycle then by precursor_mz
    spectra.sort_by(|a, b| match a.cycle.cmp(&b.cycle) {
        std::cmp::Ordering::Equal => {
            a.precursor_mz.partial_cmp(&b.precursor_mz).unwrap()
        }
        other => other,
    });

    spectra
}

// ============================================================================
// DIA Writer
// ============================================================================

/// Run slice info for mapping spectra to bounding boxes
struct RunSlice {
    id: i64,
    begin_mz: f64,
    end_mz: f64,
}

/// Write a simplified DIA mzDB file
fn write_simplified_dia_mzdb(
    source_mzdb_path: &str,
    simplified_spectra: &[SimplifiedSpectrum],
    output_path: &Path,
) -> Result<()> {
    // Copy the original database as a starting point
    std::fs::copy(source_mzdb_path, output_path)
        .context("Failed to copy source database")?;

    // Open the copy for modification
    let conn = Connection::open(output_path)
        .context("Failed to open output database")?;

    // Disable foreign key checks
    conn.execute_batch("PRAGMA foreign_keys = OFF;")?;

    // Begin transaction for bulk operations
    conn.execute_batch("BEGIN TRANSACTION;")?;

    // Delete all MS2 spectra from the original
    conn.execute("DELETE FROM spectrum WHERE ms_level = 2", [])?;
    log::info!("Deleted original MS2 spectra");

    // Delete MS2 bounding boxes
    conn.execute(
        "DELETE FROM bounding_box WHERE run_slice_id IN 
         (SELECT id FROM run_slice WHERE ms_level = 2)",
        [],
    )?;
    log::info!("Deleted MS2 bounding boxes");

    // Delete MS2 entries from R-tree if exists
    let has_msn_rtree: bool = conn.query_row(
        "SELECT COUNT(*) > 0 FROM sqlite_master WHERE type='table' AND name='bounding_box_msn_rtree'",
        [],
        |row| row.get(0),
    )?;

    if has_msn_rtree {
        conn.execute(
            "DELETE FROM bounding_box_msn_rtree WHERE min_ms_level = 2",
            [],
        )?;
        log::info!("Deleted MS2 R-tree entries");
    }

    // Get the next spectrum ID (start after all MS1 spectra)
    let max_ms1_id: i64 = conn.query_row(
        "SELECT COALESCE(MAX(id), 0) FROM spectrum",
        [],
        |row| row.get(0),
    )?;

    let mut next_spectrum_id = max_ms1_id + 1;

    // Get or create data encoding for MS2
    let data_encoding_id = get_or_create_data_encoding(&conn)?;

    // Get run_id
    let run_id: i64 =
        conn.query_row("SELECT id FROM run LIMIT 1", [], |row| row.get(0))?;

    // Get reference metadata from MS1 spectra
    let (instr_config_id, source_file_id, data_proc_id): (i64, i64, i64) = conn
        .query_row(
            "SELECT instrument_configuration_id, source_file_id, data_processing_id 
             FROM spectrum WHERE ms_level = 1 LIMIT 1",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .unwrap_or((1, 1, 1));

    // Get run_slice mapping by m/z range
    let run_slices = get_run_slices_for_ms2(&conn)?;
    log::info!("Found {} MS2 run slices", run_slices.len());

    // Group simplified spectra by run_slice (isolation window)
    let mut spectra_by_run_slice: HashMap<i64, Vec<&SimplifiedSpectrum>> =
        HashMap::new();

    for spectrum in simplified_spectra {
        // Find matching run_slice
        let run_slice_id =
            find_run_slice_for_precursor(spectrum.precursor_mz, &run_slices);

        if let Some(rs_id) = run_slice_id {
            spectra_by_run_slice.entry(rs_id).or_default().push(spectrum);
        }
    }

    // Insert bounding boxes and spectra for each run_slice
    for (run_slice_id, spectra) in &spectra_by_run_slice {
        if spectra.is_empty() {
            continue;
        }

        // Create bounding box data
        let (bb_data, spectrum_ids, first_spectrum_id) =
            create_bounding_box_data(spectra, next_spectrum_id)?;

        let last_spectrum_id =
            *spectrum_ids.last().unwrap_or(&first_spectrum_id);

        // Insert bounding box
        conn.execute(
            "INSERT INTO bounding_box (data, run_slice_id, first_spectrum_id, last_spectrum_id)
             VALUES (?1, ?2, ?3, ?4)",
            params![bb_data, run_slice_id, first_spectrum_id, last_spectrum_id],
        )?;
        let bb_id = conn.last_insert_rowid();

        // Insert spectra
        for (i, spectrum) in spectra.iter().enumerate() {
            let spectrum_id = spectrum_ids[i];

            // Generate XML metadata
            let param_tree = generate_param_tree_xml(spectrum.time);
            let precursor_list = generate_precursor_list_xml(
                spectrum.precursor_mz,
                spectrum.isolation_lower,
                spectrum.isolation_upper,
            );

            let title = format!(
                "cycle={} msLevel=2 simplified window={:.1}-{:.1}",
                spectrum.cycle, spectrum.isolation_lower, spectrum.isolation_upper
            );

            // Calculate TIC and base peak
            let tic: f64 = spectrum
                .intensity_array
                .iter()
                .map(|&i| i as f64)
                .sum();

            let (base_peak_mz, base_peak_intensity) = spectrum
                .mz_array
                .iter()
                .zip(spectrum.intensity_array.iter())
                .max_by(|a, b| a.1.partial_cmp(b.1).unwrap())
                .map(|(&mz, &int)| (mz, int))
                .unwrap_or((0.0, 0.0));

            conn.execute(
                "INSERT INTO spectrum (
                    id, initial_id, title, cycle, time, ms_level, activation_type,
                    tic, base_peak_mz, base_peak_intensity, main_precursor_mz,
                    main_precursor_charge, data_points_count, param_tree,
                    scan_list, precursor_list, product_list, 
                    shared_param_tree_id, instrument_configuration_id, source_file_id,
                    run_id, data_processing_id, data_encoding_id, bb_first_spectrum_id
                ) VALUES (
                    ?1, ?2, ?3, ?4, ?5, 2, 'HCD',
                    ?6, ?7, ?8, ?9,
                    NULL, ?10, ?11,
                    NULL, ?12, NULL,
                    NULL, ?13, ?14,
                    ?15, ?16, ?17, ?18
                )",
                params![
                    spectrum_id,
                    spectrum_id, // initial_id = id for new spectra
                    title,
                    spectrum.cycle,
                    spectrum.time,
                    tic,
                    base_peak_mz,
                    base_peak_intensity,
                    spectrum.precursor_mz,
                    spectrum.mz_array.len() as i32,
                    param_tree,
                    precursor_list,
                    instr_config_id,
                    source_file_id,
                    run_id,
                    data_proc_id,
                    data_encoding_id,
                    first_spectrum_id,
                ],
            )?;
        }

        next_spectrum_id += spectra.len() as i64;

        // Insert R-tree entry for this bounding box
        if has_msn_rtree {
            let min_time = spectra
                .iter()
                .map(|s| s.time)
                .fold(f32::INFINITY, f32::min);
            let max_time = spectra
                .iter()
                .map(|s| s.time)
                .fold(f32::NEG_INFINITY, f32::max);
            let min_mz = spectra
                .iter()
                .flat_map(|s| s.mz_array.iter())
                .fold(f64::INFINITY, |a, &b| a.min(b));
            let max_mz = spectra
                .iter()
                .flat_map(|s| s.mz_array.iter())
                .fold(f64::NEG_INFINITY, |a, &b| a.max(b));
            let min_parent_mz = spectra
                .iter()
                .map(|s| s.isolation_lower)
                .fold(f64::INFINITY, f64::min);
            let max_parent_mz = spectra
                .iter()
                .map(|s| s.isolation_upper)
                .fold(f64::NEG_INFINITY, f64::max);

            conn.execute(
                "INSERT INTO bounding_box_msn_rtree (id, min_ms_level, max_ms_level, min_parent_mz, max_parent_mz, min_mz, max_mz, min_time, max_time)
                 VALUES (?1, 2, 2, ?2, ?3, ?4, ?5, ?6, ?7)",
                params![
                    bb_id,
                    min_parent_mz,
                    max_parent_mz,
                    min_mz,
                    max_mz,
                    min_time as f64,
                    max_time as f64
                ],
            )?;
        }
    }

    // Update spectrum count in mzdb table if exists
    let total_spectra: i64 = conn.query_row(
        "SELECT COUNT(*) FROM spectrum",
        [],
        |row| row.get(0),
    )?;

    let _ = conn.execute(
        "UPDATE mzdb SET value = ?1 WHERE name = 'spectrum_count'",
        params![total_spectra.to_string()],
    );

    // Commit transaction
    conn.execute_batch("COMMIT;")?;

    // Reclaim disk space and optimize
    log::info!("Running VACUUM to reclaim disk space...");
    conn.execute_batch("VACUUM;")?;
    conn.execute_batch("PRAGMA optimize;")?;

    log::info!(
        "Successfully wrote simplified DIA file: {:?}",
        output_path
    );
    log::info!(
        "Total spectra: {} (MS1 + {} simplified MS2)",
        total_spectra,
        simplified_spectra.len()
    );

    Ok(())
}

/// Get run slices for MS2
fn get_run_slices_for_ms2(conn: &Connection) -> Result<Vec<RunSlice>> {
    let mut stmt = conn
        .prepare("SELECT id, begin_mz, end_mz FROM run_slice WHERE ms_level = 2")?;

    let slices = stmt.query_map([], |row| {
        Ok(RunSlice {
            id: row.get(0)?,
            begin_mz: row.get(1)?,
            end_mz: row.get(2)?,
        })
    })?;

    slices
        .collect::<Result<Vec<_>, _>>()
        .context("Failed to read run slices")
}

/// Find the run_slice ID for a given precursor m/z
fn find_run_slice_for_precursor(
    precursor_mz: f64,
    run_slices: &[RunSlice],
) -> Option<i64> {
    for rs in run_slices {
        if precursor_mz >= rs.begin_mz && precursor_mz <= rs.end_mz {
            return Some(rs.id);
        }
    }
    // Try to find by center
    for rs in run_slices {
        let center = (rs.begin_mz + rs.end_mz) / 2.0;
        if (precursor_mz - center).abs() < 1.0 {
            return Some(rs.id);
        }
    }
    None
}

/// Create bounding box binary data from simplified spectra
fn create_bounding_box_data(
    spectra: &[&SimplifiedSpectrum],
    start_spectrum_id: i64,
) -> Result<(Vec<u8>, Vec<i64>, i64)> {
    let mut data = Vec::new();
    let mut spectrum_ids = Vec::new();
    let first_spectrum_id = start_spectrum_id;

    for (i, spectrum) in spectra.iter().enumerate() {
        let spectrum_id = start_spectrum_id + i as i64;
        spectrum_ids.push(spectrum_id);

        // Write spectrum slice header
        // Format: spectrum_id (i32) + peaks_count (i32)
        data.extend_from_slice(&(spectrum_id as i32).to_le_bytes());
        data.extend_from_slice(&(spectrum.mz_array.len() as i32).to_le_bytes());

        // Write peaks: mz (f64) + intensity (f32) for each peak
        for (mz, intensity) in spectrum
            .mz_array
            .iter()
            .zip(spectrum.intensity_array.iter())
        {
            data.extend_from_slice(&mz.to_le_bytes());
            data.extend_from_slice(&intensity.to_le_bytes());
        }
    }

    Ok((data, spectrum_ids, first_spectrum_id))
}

/// Get or create a data encoding entry for 64-bit m/z, 32-bit intensity
fn get_or_create_data_encoding(conn: &Connection) -> Result<i64> {
    // Try to find an existing compatible encoding
    let existing: Option<i64> = conn
        .query_row(
            "SELECT id FROM data_encoding WHERE mode = 'centroid' LIMIT 1",
            [],
            |row| row.get(0),
        )
        .ok();

    if let Some(id) = existing {
        return Ok(id);
    }

    // Create a new data encoding
    conn.execute(
        "INSERT INTO data_encoding (mode, compression, byte_order, mz_precision, intensity_precision)
         VALUES ('centroid', 'none', 'little_endian', 64, 32)",
        [],
    )?;

    Ok(conn.last_insert_rowid())
}

/// Generate param_tree XML for a spectrum
fn generate_param_tree_xml(time: f32) -> String {
    format!(
        r#"<params>
  <cvParam cvRef="MS" accession="MS:1000511" value="2" name="ms level"/>
  <cvParam cvRef="MS" accession="MS:1000580" value="" name="MSn spectrum"/>
  <cvParam cvRef="MS" accession="MS:1000127" value="" name="centroid spectrum"/>
  <cvParam cvRef="MS" accession="MS:1000016" value="{:.4}" name="scan start time" unitCvRef="UO" unitAccession="UO:0000031" unitName="minute"/>
</params>"#,
        time / 60.0
    )
}

/// Generate precursor_list XML for a DIA spectrum
fn generate_precursor_list_xml(
    target_mz: f64,
    lower_mz: f64,
    upper_mz: f64,
) -> String {
    let lower_offset = target_mz - lower_mz;
    let upper_offset = upper_mz - target_mz;

    format!(
        r#"<precursorList count="1">
  <precursor>
    <isolationWindow>
      <cvParam cvRef="MS" accession="MS:1000827" value="{:.4}" name="isolation window target m/z" unitCvRef="MS" unitAccession="MS:1000040" unitName="m/z"/>
      <cvParam cvRef="MS" accession="MS:1000828" value="{:.4}" name="isolation window lower offset" unitCvRef="MS" unitAccession="MS:1000040" unitName="m/z"/>
      <cvParam cvRef="MS" accession="MS:1000829" value="{:.4}" name="isolation window upper offset" unitCvRef="MS" unitAccession="MS:1000040" unitName="m/z"/>
    </isolationWindow>
    <activation>
      <cvParam cvRef="MS" accession="MS:1000422" value="" name="beam-type collision-induced dissociation"/>
    </activation>
  </precursor>
</precursorList>"#,
        target_mz, lower_offset, upper_offset
    )
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_defaults() {
        let config = DiaSimplifierConfig::default();
        assert_eq!(config.points_per_peakel, 3);
        assert_eq!(config.mz_merge_tolerance, 0.001);
    }

    #[test]
    fn test_config_validation() {
        assert!(DiaSimplifierConfig::with_points(3).is_ok());
        assert!(DiaSimplifierConfig::with_points(5).is_ok());
        assert!(DiaSimplifierConfig::with_points(2).is_err());
        assert!(DiaSimplifierConfig::with_points(4).is_err());
    }

    #[test]
    fn test_simplified_spectrum() {
        let spectrum = SimplifiedSpectrum {
            cycle: 1,
            time: 100.0,
            precursor_mz: 500.0,
            isolation_lower: 475.0,
            isolation_upper: 525.0,
            mz_array: vec![200.0, 300.0, 400.0],
            intensity_array: vec![1000.0, 2000.0, 1500.0],
        };

        assert_eq!(spectrum.cycle, 1);
        assert_eq!(spectrum.mz_array.len(), 3);
        assert_eq!(spectrum.intensity_array.len(), 3);
    }

    #[test]
    fn test_peakel_apex_index() {
        let peakel = SimplifierPeakel {
            id: 1,
            mz: 500.0,
            elution_time: 100.0,
            duration: 30.0,
            gap_count: 0,
            apex_intensity: 10000.0,
            area: 50000.0,
            amplitude: 10.0,
            peaks_count: 5,
            first_spectrum_id: 100,
            apex_spectrum_id: 102,
            last_spectrum_id: 104,
            isolation_window_id: 1,
            precursor_mz: 500.0,
            spectrum_ids: vec![100, 101, 102, 103, 104],
            elution_times: vec![98.0, 99.0, 100.0, 101.0, 102.0],
            mz_values: vec![500.0, 500.1, 500.0, 500.1, 500.0],
            intensities: vec![1000.0, 5000.0, 10000.0, 5000.0, 1000.0],
        };

        assert_eq!(peakel.apex_index(), Some(2));
    }

    #[test]
    fn test_param_tree_xml() {
        let xml = generate_param_tree_xml(120.0);
        assert!(xml.contains("ms level"));
        assert!(xml.contains("2.0000")); // 120/60 = 2 minutes
    }

    #[test]
    fn test_precursor_list_xml() {
        let xml = generate_precursor_list_xml(500.0, 475.0, 525.0);
        assert!(xml.contains("500.0000"));
        assert!(xml.contains("25.0000")); // offset
    }
}
