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
use rusqlite::Connection;

use crate::processing::dia::IsolationWindow;
use crate::processing::peakeldb::{Ms2PeakelDbReader, ExtendedPeakel};
use crate::processing::staggered::{StaggeredDiaDetector, StaggeredDiaInfo};
use crate::model::{SpectrumHeader as ModelSpectrumHeader, Spectrum, SpectrumData, DataEncoding};
use crate::writer::{
    xml_builder::generate_dia_precursor_list_xml_asymmetric,
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
// Bounding Box Configuration
// ============================================================================

/// Bounding box size configuration from mzDB
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

/// A reconstructed simplified spectrum (1-to-1 with original spectrum)
#[derive(Debug, Clone)]
pub struct SimplifiedSpectrum {
    /// Original spectrum ID (for metadata lookup)
    pub original_spectrum_id: i64,
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

/// Spectrum header information with metadata
#[derive(Debug, Clone)]
pub struct SpectrumHeader {
    pub id: i64,
    pub cycle: i32,
    pub time: f32,
    pub param_tree: Option<String>,
    pub scan_list: Option<String>,
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
        let original_window_count = isolation_windows.len();
        log::info!("Found {} isolation windows", original_window_count);

        // Detect staggered DIA mode
        log::info!("Checking for staggered DIA acquisition...");
        let stagger_info = detect_staggered_from_mzdb(&mzdb_conn)?;

        // Build isolation window lookup - use unstaggered windows if staggered DIA detected
        let (window_lookup, staggered_detected, staggered_offset, unstaggered_window_count) =
            if stagger_info.is_staggered {
                log::info!("╔══════════════════════════════════════════════════════════════╗");
                log::info!("║              STAGGERED DIA DETECTED                          ║");
                log::info!("╠══════════════════════════════════════════════════════════════╣");
                log::info!("║  Window offset: {:.2} Da                                      ", stagger_info.window_offset);
                log::info!("║  Window width:  {:.2} Da                                      ", stagger_info.window_width);
                log::info!("║  Cycle A windows (odd):  {}                                   ", stagger_info.cycle_a_windows.len());
                log::info!("║  Cycle B windows (even): {}                                   ", stagger_info.cycle_b_windows.len());
                log::info!("║  Unstaggered windows:    {}                                   ", stagger_info.unstaggered_windows.len());
                log::info!("╚══════════════════════════════════════════════════════════════╝");

                // Log sample windows
                if !stagger_info.cycle_a_windows.is_empty() {
                    let sample_a: Vec<_> = stagger_info.cycle_a_windows.iter().take(5).map(|w| format!("{:.1}", w.target_mz)).collect();
                    log::info!("  Cycle A sample: {} ...", sample_a.join(", "));
                }
                if !stagger_info.cycle_b_windows.is_empty() {
                    let sample_b: Vec<_> = stagger_info.cycle_b_windows.iter().take(5).map(|w| format!("{:.1}", w.target_mz)).collect();
                    log::info!("  Cycle B sample: {} ...", sample_b.join(", "));
                }
                if !stagger_info.unstaggered_windows.is_empty() {
                    let sample_u: Vec<_> = stagger_info.unstaggered_windows.iter().take(5).map(|w| format!("{:.1}-{:.1}", w.lower_mz, w.upper_mz)).collect();
                    log::info!("  Unstaggered sample: {} ...", sample_u.join(", "));
                }

                // Build a lookup from original window ID to the unstaggered window that best matches it
                // We match by checking which unstaggered window contains the original window's target m/z
                let window_lookup: HashMap<i64, IsolationWindow> = isolation_windows
                    .iter()
                    .filter_map(|orig_win| {
                        // Find the unstaggered window that contains this original window's target m/z
                        for unstag_win in &stagger_info.unstaggered_windows {
                            // Use slightly relaxed bounds for matching (0.1 Da tolerance)
                            if orig_win.target_mz >= unstag_win.lower_mz - 0.1
                               && orig_win.target_mz <= unstag_win.upper_mz + 0.1 {
                                return Some((orig_win.id, IsolationWindow {
                                    id: orig_win.id, // Keep original ID for peakel lookup
                                    target_mz: unstag_win.center_mz,
                                    lower_mz: unstag_win.lower_mz,
                                    upper_mz: unstag_win.upper_mz,
                                    spectrum_count: orig_win.spectrum_count,
                                }));
                            }
                        }
                        // Fallback: use original window if no unstaggered mapping found
                        log::warn!("No unstaggered window found for original window {} (m/z {:.2})",
                                   orig_win.id, orig_win.target_mz);
                        Some((orig_win.id, orig_win.clone()))
                    })
                    .collect();

                (window_lookup, true, stagger_info.window_offset, stagger_info.unstaggered_windows.len())
            } else {
                log::info!("Standard (non-staggered) DIA mode");
                let window_lookup: HashMap<i64, IsolationWindow> = isolation_windows
                    .into_iter()
                    .map(|w| (w.id, w))
                    .collect();
                (window_lookup, false, 0.0, 0)
            };

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

        // Build spectrum_id -> header lookup
        let spectrum_info: HashMap<i64, &SpectrumHeader> = ms2_headers
            .iter()
            .map(|h| (h.id, h))
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

        // Write output using generic writer
        log::info!("Writing simplified DIA mzDB file...");
        write_simplified_dia_mzdb_v2(
            mzdb_path,
            &simplified_spectra,
            output_path,
            &ms2_headers,
        )?;

        let stats = SimplificationStats {
            peakel_count,
            original_ms2_count,
            simplified_ms2_count,
            data_point_count,
            staggered_detected,
            staggered_offset,
            original_window_count,
            unstaggered_window_count,
        };

        log::info!("Done! Stats: {:?}", stats);

        Ok(stats)
    }
}

/// Detect staggered DIA from mzDB connection
fn detect_staggered_from_mzdb(conn: &Connection) -> Result<StaggeredDiaInfo> {
    let detector = StaggeredDiaDetector::new();
    detector.detect(conn)
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
    /// Whether staggered DIA was detected
    pub staggered_detected: bool,
    /// The detected window offset (0.0 if not staggered)
    pub staggered_offset: f64,
    /// Number of original isolation windows
    pub original_window_count: usize,
    /// Number of unstaggered windows (if staggered was detected)
    pub unstaggered_window_count: usize,
}

// ============================================================================
// Core Processing Functions
// ============================================================================

/// Get MS2 spectrum headers with metadata
fn get_ms2_spectrum_headers(conn: &Connection) -> Result<Vec<SpectrumHeader>> {
    let mut stmt = conn.prepare(
        "SELECT id, cycle, time, param_tree, scan_list
         FROM spectrum
         WHERE ms_level = 2
         ORDER BY id",
    )?;

    let headers = stmt.query_map([], |row| {
        Ok(SpectrumHeader {
            id: row.get(0)?,
            cycle: row.get(1)?,
            time: row.get(2)?,
            param_tree: row.get(3)?,
            scan_list: row.get(4)?,
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
    spectrum_info: &HashMap<i64, &SpectrumHeader>,
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

        // Get header info including metadata
        let header = match spectrum_info.get(&spectrum_id) {
            Some(&h) => h,
            None => continue,
        };

        let first_point = &points[0];

        spectra.push(SimplifiedSpectrum {
            original_spectrum_id: spectrum_id,
            cycle: header.cycle,
            time: header.time,
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
/// Write simplified DIA spectra using MzDbWriter (interleaved MS1+MS2)
fn write_simplified_dia_mzdb_v2(
    source_mzdb_path: &str,
    simplified_spectra: &[SimplifiedSpectrum],
    output_path: &Path,
    _ms2_headers: &[SpectrumHeader],
) -> Result<()> {
    use crate::writer::{MzDbWriterBuilder, WriterMetadata};
    use crate::MzDbReaderBuilder;
    use crate::model::{DataEncoding, DataMode, PeakEncoding, ByteOrder};
    use fallible_iterator::FallibleIterator;
    use std::collections::HashMap;

    log::info!("Opening source mzDB for reading...");
    let source_reader = MzDbReaderBuilder::new(source_mzdb_path).build()?;

    // Get BB sizes from entity cache
    log::info!("Reading BB configuration...");
    let bb_sizes = source_reader.entity_cache().bb_sizes.clone();
    log::info!("  BB sizes: MS1 {:.1}x{:.1} Da·s, MS2 {:.1}x{:.1} Da·s",
               bb_sizes.bb_mz_height_ms1, bb_sizes.bb_rt_width_ms1,
               bb_sizes.bb_mz_height_msn, bb_sizes.bb_rt_width_msn);

    // Build lookup: MS2 spectrum_id -> SimplifiedSpectrum
    let simplified_map: HashMap<i64, &SimplifiedSpectrum> = simplified_spectra
        .iter()
        .map(|s| (s.original_spectrum_id, s))
        .collect();
    log::info!("  {} MS2 spectra have simplified data", simplified_map.len());

    // Create writer with minimal metadata
    log::info!("Creating output mzDB...");
    let metadata = WriterMetadata::with_defaults();

    let mut writer = MzDbWriterBuilder::new(output_path)
        .metadata(metadata)
        .bb_sizes(bb_sizes)
        .is_dia(true)
        .build()?;

    writer.open()?;

    // Create data encoding
    let encoding = DataEncoding {
        id: 1,
        mode: DataMode::Centroid,
        peak_encoding: PeakEncoding::HighRes,
        byte_order: ByteOrder::LittleEndian,
        compression: "none".to_string(),
    };

    // Iterate through all spectra in time order
    log::info!("Inserting spectra (interleaved MS1+MS2)...");
    let mut ms1_count = 0;
    let mut ms2_with_data = 0;
    let mut ms2_empty = 0;
    let mut total_count = 0;

    let mut iter = source_reader.iter_spectra(None)?;
    while let Some(spectrum) = iter.next()? {
        total_count += 1;

        if total_count % 1000 == 0 {
            log::info!("  Progress: {} spectra", total_count);
        }

        if spectrum.header.ms_level == 1 {
            // MS1: insert as-is
            writer.insert_spectrum(&spectrum, &encoding)?;
            ms1_count += 1;
        } else if spectrum.header.ms_level == 2 {
            // MS2: check if simplified data exists
            if let Some(simplified) = simplified_map.get(&spectrum.header.id) {
                // Has data: create spectrum from simplified data
                let simplified_spectrum = convert_simplified_to_spectrum_simple(simplified, &spectrum.header, &encoding);
                writer.insert_spectrum(&simplified_spectrum, &encoding)?;
                ms2_with_data += 1;
            } else {
                // Empty: create empty spectrum preserving header
                let empty_spectrum = create_empty_ms2_spectrum_from_header(&spectrum.header, &encoding);
                writer.insert_spectrum_allow_empty(&empty_spectrum, &encoding)?;
                ms2_empty += 1;
            }
        }
    }

    log::info!("Inserted spectra:");
    log::info!("  MS1: {}", ms1_count);
    log::info!("  MS2 with data: {}", ms2_with_data);
    log::info!("  MS2 empty: {}", ms2_empty);
    log::info!("  Total: {}", ms1_count + ms2_with_data + ms2_empty);

    // Close writer (flushes BBs, creates indexes)
    log::info!("Finalizing output file...");
    writer.close()?;

    log::info!("Output file written successfully!");

    Ok(())
}

// ============================================================================
// Old Writer Implementation (deprecated, to be removed later)
// ============================================================================


// ============================================================================
// Helper Functions for Writer Integration
// ============================================================================

/// Convert SimplifiedSpectrum to model Spectrum (using existing header)
fn convert_simplified_to_spectrum_simple(
    simplified: &SimplifiedSpectrum,
    source_header: &ModelSpectrumHeader,
    data_encoding: &DataEncoding,
) -> Spectrum {
    let tic: f32 = simplified.intensity_array.iter().sum();
    let (base_peak_mz, base_peak_intensity) = simplified.mz_array.iter()
        .zip(&simplified.intensity_array)
        .max_by(|a, b| a.1.partial_cmp(b.1).unwrap())
        .map(|(&mz, &intensity)| (mz, intensity))
        .unwrap_or((0.0, 0.0));

    let precursor_list = generate_dia_precursor_list_xml_asymmetric(
        simplified.precursor_mz,
        simplified.isolation_lower,
        simplified.isolation_upper,
    );

    Spectrum {
        header: ModelSpectrumHeader {
            id: simplified.original_spectrum_id,
            initial_id: source_header.initial_id,
            title: format!("cycle={} msLevel=2 simplified", simplified.cycle),
            cycle: simplified.cycle as i64,
            time: simplified.time,
            ms_level: 2,
            activation_type: Some("HCD".to_string()),
            tic,
            base_peak_mz,
            base_peak_intensity,
            precursor_mz: Some(simplified.precursor_mz),
            precursor_charge: None,
            peaks_count: simplified.mz_array.len() as i64,
            param_tree_str: source_header.param_tree_str.clone(),
            scan_list_str: source_header.scan_list_str.clone(),
            precursor_list_str: Some(precursor_list),
            product_list_str: None,
            shared_param_tree_id: source_header.shared_param_tree_id,
            instrument_configuration_id: source_header.instrument_configuration_id,
            source_file_id: source_header.source_file_id,
            run_id: source_header.run_id,
            data_processing_id: source_header.data_processing_id,
            data_encoding_id: source_header.data_encoding_id,
            bb_first_spectrum_id: 0,
        },
        data: SpectrumData {
            data_encoding: data_encoding.clone(),
            peaks_count: simplified.mz_array.len(),
            mz_array: simplified.mz_array.clone(),
            intensity_array: simplified.intensity_array.clone(),
            lwhm_array: vec![0.0; simplified.mz_array.len()],
            rwhm_array: vec![0.0; simplified.mz_array.len()],
        },
    }
}

/// Create empty MS2 spectrum from header
fn create_empty_ms2_spectrum_from_header(source_header: &ModelSpectrumHeader, data_encoding: &DataEncoding) -> Spectrum {
    Spectrum {
        header: ModelSpectrumHeader {
            id: source_header.id,
            initial_id: source_header.initial_id,
            title: source_header.title.clone(),
            cycle: source_header.cycle,
            time: source_header.time,
            ms_level: 2,
            activation_type: source_header.activation_type.clone(),
            tic: 0.0,
            base_peak_mz: 0.0,
            base_peak_intensity: 0.0,
            precursor_mz: source_header.precursor_mz,
            precursor_charge: source_header.precursor_charge,
            peaks_count: 0,
            param_tree_str: source_header.param_tree_str.clone(),
            scan_list_str: source_header.scan_list_str.clone(),
            precursor_list_str: source_header.precursor_list_str.clone(),
            product_list_str: None,
            shared_param_tree_id: source_header.shared_param_tree_id,
            instrument_configuration_id: source_header.instrument_configuration_id,
            source_file_id: source_header.source_file_id,
            run_id: source_header.run_id,
            data_processing_id: source_header.data_processing_id,
            data_encoding_id: source_header.data_encoding_id,
            bb_first_spectrum_id: 0,
        },
        data: SpectrumData {
            data_encoding: data_encoding.clone(),
            peaks_count: 0,
            mz_array: vec![],
            intensity_array: vec![],
            lwhm_array: vec![],
            rwhm_array: vec![],
        },
    }
}

// ============================================================================