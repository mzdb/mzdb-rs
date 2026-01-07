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

use anyhow_ext::{bail, Context, Result};
use ordered_float::OrderedFloat;
use rusqlite::{params, Connection};

use crate::processing::dia::IsolationWindow;
use crate::processing::peakeldb::{Ms2PeakelDbReader, ExtendedPeakel};
use crate::writer::{
    DiaWriteContext, DiaSpectrumParams,
    calculate_time_bounds, calculate_mz_bounds, calculate_mz_bounds_from_arrays, find_base_peak,
    insert_msn_rtree_entry,
    xml_builder::{generate_ms2_param_tree_xml, generate_dia_precursor_list_xml_asymmetric},
};

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
            bail!("points_per_peakel must be odd (1, 3, 5, etc.)");
        }
        Ok(Self {
            points_per_peakel,
            ..Default::default()
        })
    }
}

// ============================================================================
// Peakel Data Structures (uses ExtendedPeakel from peakeldb::common)
// ============================================================================

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

// Note: PeakelDbReader is now available as Ms2PeakelDbReader from peakeldb module
// Kept as type alias for backward compatibility
pub type PeakelDbReader = Ms2PeakelDbReader;

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
    peakels: &[ExtendedPeakel],
    window_lookup: &HashMap<i64, IsolationWindow>,
    half_window: usize,
) -> Result<Vec<PeakelDataPoint>> {
    let mut data_points = Vec::new();

    for (i, peakel) in peakels.iter().enumerate() {
        if i % 10000 == 0 && i > 0 {
            log::debug!("Processing peakel {}/{}", i, peakels.len());
        }

        // Get isolation window (ExtendedPeakel has Option<i64>)
        let isolation_window_id = match peakel.isolation_window_id {
            Some(id) => id,
            None => continue, // Skip non-DIA peakels
        };
        
        let window = match window_lookup.get(&isolation_window_id) {
            Some(w) => w,
            None => continue,
        };

        // Find apex index in the peakel's data arrays
        let spectrum_ids = peakel.data.spectrum_ids.as_slice();
        let apex_idx = match peakel.apex_data_index() {
            Some(idx) => idx,
            None => {
                // Fallback: use middle of array
                spectrum_ids.len() / 2
            }
        };

        // Calculate range of indices to include (apex ± half_window)
        let start_idx = apex_idx.saturating_sub(half_window);
        let end_idx = (apex_idx + half_window).min(spectrum_ids.len().saturating_sub(1));

        // Extract data points from the selected indices
        for idx in start_idx..=end_idx {
            if idx < spectrum_ids.len() {
                data_points.push(PeakelDataPoint {
                    spectrum_id: peakel.data.spectrum_ids[idx],
                    mz: peakel.data.mz_values[idx],
                    intensity: peakel.data.intensities[idx],
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

    // Initialize write context from database
    let mut ctx = DiaWriteContext::from_connection(&conn)?;

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
            create_bounding_box_data(spectra, ctx.next_spectrum_id)?;

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
            let param_tree = generate_ms2_param_tree_xml(spectrum.time);
            let precursor_list = generate_dia_precursor_list_xml_asymmetric(
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

            let (base_peak_mz, base_peak_intensity) = find_base_peak(
                &spectrum.mz_array,
                &spectrum.intensity_array,
            );

            let params = DiaSpectrumParams {
                spectrum_id,
                title,
                cycle: spectrum.cycle,
                time: spectrum.time,
                tic,
                base_peak_mz,
                base_peak_intensity,
                precursor_mz: spectrum.precursor_mz,
                data_points_count: spectrum.mz_array.len() as i32,
                param_tree,
                precursor_list,
                instr_config_id: ctx.instr_config_id,
                source_file_id: ctx.source_file_id,
                run_id: ctx.run_id,
                data_proc_id: ctx.data_proc_id,
                data_encoding_id: ctx.data_encoding_id,
                bb_first_spectrum_id: first_spectrum_id,
            };
            params.insert(&conn)?;
        }

        ctx.advance_spectrum_id(spectra.len() as i64);

        // Insert R-tree entry for this bounding box
        if ctx.has_msn_rtree {
            let (min_time, max_time) = calculate_time_bounds(spectra, |s| s.time);
            let (min_mz, max_mz) = calculate_mz_bounds_from_arrays(spectra, |s| s.mz_array.as_slice());
            let (min_parent_mz, _) = calculate_mz_bounds(spectra, |s| s.isolation_lower);
            let (_, max_parent_mz) = calculate_mz_bounds(spectra, |s| s.isolation_upper);

            insert_msn_rtree_entry(
                &conn,
                bb_id,
                2, // ms_level
                min_parent_mz,
                max_parent_mz,
                min_mz,
                max_mz,
                min_time as f64,
                max_time as f64,
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
        use crate::processing::peakeldb::PeakelData;
        
        let data = PeakelData::from_vectors(
            vec![100, 101, 102, 103, 104],
            vec![98.0, 99.0, 100.0, 101.0, 102.0],
            vec![500.0, 500.1, 500.0, 500.1, 500.0],
            vec![1000.0, 5000.0, 10000.0, 5000.0, 1000.0],
        );
        
        let peakel = ExtendedPeakel::new_ms2_dia(
            1,          // id
            500.0,      // mz
            100.0,      // elution_time
            30.0,       // duration
            0,          // gap_count
            10000.0,    // apex_intensity
            50000.0,    // area
            10.0,       // amplitude
            5,          // peaks_count
            100,        // first_spectrum_id
            102,        // apex_spectrum_id
            104,        // last_spectrum_id
            1,          // isolation_window_id
            500.0,      // precursor_mz
            data,
        );

        assert_eq!(peakel.apex_data_index(), Some(2));
    }

    // XML generation tests are now in writer/xml_builder.rs
}
