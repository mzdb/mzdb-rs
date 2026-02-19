//! DIA Simplifier - Simplify DIA mzDB files using detected MS2 peakels
//!
//! This module provides functionality to simplify DIA (Data Independent Acquisition) mzDB files
//! by reconstructing MS2 spectra from detected peakels. For each peakel, only the apex and
//! surrounding data points are retained, significantly reducing file size while preserving
//! the essential signal information.
//!
//! # Staggered DIA
//!
//! For staggered DIA acquisitions, peakels are already deduplicated and merged across
//! overlapping windows during detection. The simplifier performs:
//! - **Peakel-level signal dispatch**: classifies each peakel by the number of contributing
//!   original isolation windows (1-window, 2-window, 3+-window) and dispatches accordingly.
//! - **Spectrum doubling**: each original MS2 spectrum produces two output spectra (one per
//!   unstaggered sub-window), ensuring no RT gaps in the output.
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

use crate::processing::signal::ms2_detection::IsolationWindow;
use crate::processing::peakeldb::{Ms2PeakelDbReader, ExtendedPeakel};
use crate::processing::staggered::{
    StaggeredDiaDetector, StaggeredDiaInfo, UnstaggeredWindow, UnstaggeredWindowType,
    SingleObservationStrategy,
};
use crate::model::{SpectrumHeader as ModelSpectrumHeader, Spectrum, SpectrumData, DataEncoding};
use crate::writer::xml_builder::generate_dia_precursor_list_xml_asymmetric;

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
    pub mz_merge_tolerance: f32,
    /// Whether to unstagger staggered DIA acquisitions.
    /// When false (simplify mode), original isolation windows are preserved and
    /// spectra are simplified 1:1. When true (unstagger mode), staggered windows
    /// are split into non-overlapping sub-windows with signal dispatch.
    pub unstagger: bool,
    /// Strategy for handling peakels observed in only one cycle's isolation window
    /// (cannot determine precise unstaggered sub-window assignment).
    /// Only used when `unstagger` is true.
    pub single_observation_strategy: SingleObservationStrategy,
}

impl Default for DiaSimplifierConfig {
    fn default() -> Self {
        Self {
            points_per_peakel: 3,
            mz_merge_tolerance: 0.001,
            unstagger: false,
            single_observation_strategy: SingleObservationStrategy::Duplicate,
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
// Data Structures
// ============================================================================

/// A data point extracted from a peakel
#[derive(Debug, Clone)]
struct PeakelDataPoint {
    /// The spectrum ID this data point belongs to
    spectrum_id: i64,
    /// The m/z value at this data point (32-bit for centroid data)
    mz: f32,
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
    /// m/z values (sorted) - 32-bit for centroid data
    pub mz_array: Vec<f32>,
    /// Intensity values
    pub intensity_array: Vec<f32>,
}

/// Spectrum header information with metadata
#[derive(Debug, Clone)]
pub struct SpectrumHeader {
    pub id: i64,
    pub initial_id: i64,
    pub cycle: i32,
    pub time: f32,
    pub title: String,
    pub scan_list: Option<String>,
    pub activation_type: Option<String>,
    pub shared_param_tree_id: Option<i64>,
    pub instrument_configuration_id: i64,
    pub source_file_id: i64,
    pub run_id: i64,
    pub data_processing_id: i64,
    pub data_encoding_id: i64,
    pub main_precursor_mz: Option<f64>,
}

// Note: PeakelDbReader is now available as Ms2PeakelDbReader from peakeldb module
pub type PeakelDbReader = Ms2PeakelDbReader;

// ============================================================================
// Dispatch Safety Thresholds
// ============================================================================

/// Minimum total data points (both windows) to consider single-window assignment.
/// Below this, the peakel is duplicated across sub-windows as a safeguard.
const MIN_PEAKS_FOR_SINGLE_ASSIGN: usize = 5;

/// Maximum allowed difference in peak counts between the two windows.
/// If |n_A - n_B| exceeds this, the peakel is duplicated.
const MAX_PEAKS_DIFF_FOR_SINGLE_ASSIGN: usize = 2;

// ============================================================================
// Peakel Window Classification
// ============================================================================

/// Classification of a peakel based on how many original isolation windows
/// contribute spectra to it.
#[derive(Debug, Clone)]
enum PeakelWindowClassification {
    /// All spectra from a single original IW — cannot determine precise sub-window.
    /// Contains the original IW id.
    SingleWindow(i64),
    /// Spectra from exactly 2 original IWs.
    /// Contains (iw_a_id, n_peaks_a, iw_b_id, n_peaks_b) sorted by count descending.
    TwoWindow(i64, usize, i64, usize),
    /// Spectra from 3+ original IWs — top-2 by peak count used for overlap lookup,
    /// all (iw_id, count) pairs retained sorted by count descending.
    MultiWindow(Vec<(i64, usize)>),
}

/// Statistics about signal dispatch decisions
#[derive(Debug, Clone, Default)]
pub struct DispatchStats {
    /// Peakels with signal from exactly 2 IWs, confidently assigned to the overlap
    /// sub-window (total_peaks >= MIN_PEAKS_FOR_SINGLE_ASSIGN and balanced)
    pub two_window_single_assign: usize,
    /// Peakels with signal from exactly 2 IWs, duplicated because too few peaks
    /// or imbalanced peak counts between windows
    pub two_window_duplicated: usize,
    /// Peakels with signal from only 1 IW → strategy-dependent dispatch
    pub single_window_peakels: usize,
    /// Peakels with signal from 3+ IWs → resolved to overlap window (adjacent top-2)
    pub multi_window_resolved: usize,
    /// Peakels with signal from 3+ IWs → duplicated (non-adjacent top-2)
    pub multi_window_duplicated: usize,
    /// 1-window peakels that were removed by strategy
    pub single_window_removed: usize,
    /// 1-window peakels that were duplicated across sub-windows
    pub single_window_duplicated: usize,
    /// Total data points dispatched to exactly one target window
    pub data_points_unique: usize,
    /// Total data points duplicated (emitted to multiple target windows)
    pub data_points_duplicated: usize,
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

        // Only activate the staggered pipeline when unstaggering is requested
        let use_staggered_pipeline = stagger_info.is_staggered && self.config.unstagger;

        if stagger_info.is_staggered && !self.config.unstagger {
            log::info!("Staggered DIA detected but unstaggering not requested — using simple simplification");
        }

        // Build isolation window lookup - use unstaggered windows if staggered DIA detected
        let (window_lookup, staggered_detected, staggered_offset, unstaggered_window_count) =
            if use_staggered_pipeline {
                log::info!("╔══════════════════════════════════════════════╗");
                log::info!("║        STAGGERED DIA DETECTED                ║");
                log::info!("╠══════════════════════════════════════════════╣");
                log::info!("║  Window offset: {:<7.2} Da                   ║", stagger_info.window_offset);
                log::info!("║  Window width:  {:<7.2} Da                   ║", stagger_info.window_width);
                log::info!("║  Cycle A windows (odd):  {:<20}║", stagger_info.cycle_a_windows.len());
                log::info!("║  Cycle B windows (even): {:<20}║", stagger_info.cycle_b_windows.len());
                log::info!("║  Unstaggered windows:    {:<20}║", stagger_info.unstaggered_windows.len());
                log::info!("╚══════════════════════════════════════════════╝");

                if !stagger_info.unstaggered_windows.is_empty() {
                    let sample_u: Vec<_> = stagger_info.unstaggered_windows.iter().take(5)
                        .map(|w| format!("{:.1}-{:.1}", w.lower_mz, w.upper_mz)).collect();
                    log::info!("  Unstaggered sample: {} ...", sample_u.join(", "));
                }

                // Build a lookup from original window ID to itself (identity for non-staggered path compat)
                let window_lookup: HashMap<i64, IsolationWindow> = isolation_windows
                    .iter()
                    .map(|w| (w.id, w.clone()))
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

        // Read MS2 spectrum headers to get cycle, time, and metadata
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
        let half_window = self.config.points_per_peakel / 2;
        let (data_points, dispatch_stats) = if staggered_detected {
            // Staggered DIA: peakel-level classification and dispatch
            log::info!("Building spectrum-to-IW mapping for staggered dispatch...");
            let spectrum_to_orig_iw = build_spectrum_to_orig_iw_map(
                &mzdb_conn,
                &stagger_info,
            )?;
            log::info!("  Mapped {} MS2 spectra to original IWs", spectrum_to_orig_iw.len());

            log::info!("Extracting staggered peakel data points (peakel-level dispatch)...");
            extract_staggered_data_points(
                &peakels,
                &spectrum_to_orig_iw,
                &stagger_info,
                half_window,
                self.config.single_observation_strategy,
            )?
        } else {
            // Non-staggered: direct extraction
            log::info!("Extracting peakel data points (non-staggered)...");
            let points = extract_peakel_data_points(&peakels, &window_lookup, half_window)?;
            (points, DispatchStats::default())
        };
        let data_point_count = data_points.len();
        log::info!("Extracted {} data points", data_point_count);

        if staggered_detected {
            log::info!("╔══════════════════════════════════════════════════════╗");
            log::info!("║       SIGNAL DISPATCH STATISTICS                     ║");
            log::info!("╠══════════════════════════════════════════════════════╣");
            log::info!("║  2-window → single assign (confident):  {:>8}     ║", dispatch_stats.two_window_single_assign);
            log::info!("║  2-window → duplicated (safeguard):     {:>8}     ║", dispatch_stats.two_window_duplicated);
            log::info!("║  1-window peakels (strategy-dep.):      {:>8}     ║", dispatch_stats.single_window_peakels);
            log::info!("║  3+-window peakels (resolved to ovlp):  {:>8}     ║", dispatch_stats.multi_window_resolved);
            log::info!("║  3+-window peakels (duplicated):        {:>8}     ║", dispatch_stats.multi_window_duplicated);
            log::info!("║  1-window removed:                      {:>8}     ║", dispatch_stats.single_window_removed);
            log::info!("║  1-window duplicated:                   {:>8}     ║", dispatch_stats.single_window_duplicated);
            log::info!("║  Data points → unique window:           {:>8}     ║", dispatch_stats.data_points_unique);
            log::info!("║  Data points → duplicated:              {:>8}     ║", dispatch_stats.data_points_duplicated);
            log::info!("╚══════════════════════════════════════════════════════╝");
        }

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
        let write_stats = write_simplified_dia_mzdb(
            mzdb_path,
            &simplified_spectra,
            output_path,
            &ms2_headers,
            &stagger_info,
            staggered_detected,
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
            dispatch_stats,
            output_ms2_count: write_stats.ms2_with_data + write_stats.ms2_empty,
            companion_spectra_created: write_stats.companion_spectra_created,
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
    pub staggered_detected: bool,
    pub staggered_offset: f64,
    pub original_window_count: usize,
    pub unstaggered_window_count: usize,
    /// Signal dispatch statistics (staggered only)
    pub dispatch_stats: DispatchStats,
    /// Total MS2 spectra in output (including companions)
    pub output_ms2_count: usize,
    /// Number of companion spectra created for spectrum doubling
    pub companion_spectra_created: usize,
}

/// Statistics from the writer
struct WriteStats {
    ms2_with_data: usize,
    ms2_empty: usize,
    companion_spectra_created: usize,
}

// ============================================================================
// Staggered Detection Helper
// ============================================================================

fn detect_staggered_from_mzdb(conn: &Connection) -> Result<StaggeredDiaInfo> {
    let detector = StaggeredDiaDetector::new();
    detector.detect(conn)
}

// ============================================================================
// Spectrum Header Loading
// ============================================================================

/// Get MS2 spectrum headers with metadata needed for the plan-based writer
fn get_ms2_spectrum_headers(conn: &Connection) -> Result<Vec<SpectrumHeader>> {
    let mut stmt = conn.prepare(
        "SELECT id, initial_id, cycle, time, title, scan_list, activation_type,
                shared_param_tree_id, instrument_configuration_id, source_file_id,
                run_id, data_processing_id, data_encoding_id, main_precursor_mz
         FROM spectrum
         WHERE ms_level = 2
         ORDER BY id",
    )?;

    let headers = stmt.query_map([], |row| {
        Ok(SpectrumHeader {
            id: row.get(0)?,
            initial_id: row.get(1)?,
            cycle: row.get(2)?,
            time: row.get(3)?,
            title: row.get(4)?,
            scan_list: row.get(5)?,
            activation_type: row.get(6)?,
            shared_param_tree_id: row.get(7)?,
            instrument_configuration_id: row.get(8)?,
            source_file_id: row.get(9)?,
            run_id: row.get(10)?,
            data_processing_id: row.get(11)?,
            data_encoding_id: row.get(12)?,
            main_precursor_mz: row.get(13)?,
        })
    })?;

    headers
        .collect::<Result<Vec<_>, _>>()
        .context("Failed to read MS2 spectrum headers")
}

// ============================================================================
// Non-Staggered Extraction (unchanged)
// ============================================================================

/// Extract data points from peakels using the peakel's own intensity data (non-staggered)
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

        let isolation_window_id = match peakel.isolation_window_id {
            Some(id) => id,
            None => continue,
        };

        let window = match window_lookup.get(&isolation_window_id) {
            Some(w) => w,
            None => continue,
        };

        let spectrum_ids = peakel.data.spectrum_ids.as_slice();
        let apex_idx = match peakel.apex_index() {
            Some(idx) => idx,
            None => spectrum_ids.len() / 2,
        };

        let start_idx = apex_idx.saturating_sub(half_window);
        let end_idx = (apex_idx + half_window).min(spectrum_ids.len().saturating_sub(1));

        for idx in start_idx..=end_idx {
            if idx < spectrum_ids.len() {
                data_points.push(PeakelDataPoint {
                    spectrum_id: peakel.data.spectrum_ids[idx],
                    mz: peakel.data.mz_values[idx],
                    intensity: peakel.data.intensity_values[idx],
                    precursor_mz: window.target_mz,
                    isolation_lower: window.lower_mz,
                    isolation_upper: window.upper_mz,
                });
            }
        }
    }

    Ok(data_points)
}

// ============================================================================
// Staggered Extraction — Peakel-Level Dispatch
// ============================================================================

/// Build a mapping from MS2 spectrum_id to its original isolation window ID.
///
/// Uses `main_precursor_mz` from the mzDB spectrum table to identify the original window.
fn build_spectrum_to_orig_iw_map(
    mzdb_conn: &Connection,
    stagger_info: &StaggeredDiaInfo,
) -> Result<HashMap<i64, i64>> {
    // Build precursor_mz key → original window ID lookup
    let mut precursor_to_orig_id: HashMap<i64, i64> = HashMap::new();
    for w in stagger_info.cycle_a_windows.iter().chain(stagger_info.cycle_b_windows.iter()) {
        let key = (w.target_mz * 100.0).round() as i64;
        precursor_to_orig_id.insert(key, w.id);
    }

    let mut stmt = mzdb_conn.prepare(
        "SELECT id, main_precursor_mz FROM spectrum WHERE ms_level = 2 AND main_precursor_mz IS NOT NULL"
    )?;

    let mut spectrum_to_iw: HashMap<i64, i64> = HashMap::new();
    let mut unmapped = 0usize;

    let rows = stmt.query_map([], |row| {
        Ok((row.get::<_, i64>(0)?, row.get::<_, f64>(1)?))
    })?;

    for row in rows {
        let (spec_id, precursor_mz) = row?;
        let key = (precursor_mz * 100.0).round() as i64;

        match precursor_to_orig_id.get(&key) {
            Some(&orig_id) => { spectrum_to_iw.insert(spec_id, orig_id); }
            None => { unmapped += 1; }
        }
    }

    if unmapped > 0 {
        log::warn!("  {} MS2 spectra could not be mapped to original IWs", unmapped);
    }

    Ok(spectrum_to_iw)
}

/// Build a mapping from original IW id to the unstaggered sub-windows it contributes to.
fn build_orig_iw_to_unstaggered_map(
    stagger_info: &StaggeredDiaInfo,
) -> HashMap<i64, Vec<&UnstaggeredWindow>> {
    let mut map: HashMap<i64, Vec<&UnstaggeredWindow>> = HashMap::new();
    for uw in &stagger_info.unstaggered_windows {
        if let Some(id) = uw.cycle_a_source_id {
            map.entry(id).or_default().push(uw);
        }
        if let Some(id) = uw.cycle_b_source_id {
            map.entry(id).or_default().push(uw);
        }
    }
    map
}

/// Find the Overlap-type unstaggered window at the intersection of two original IWs.
fn find_overlap_window<'a>(
    iw_a_id: i64,
    iw_b_id: i64,
    unstaggered_windows: &'a [UnstaggeredWindow],
) -> Option<&'a UnstaggeredWindow> {
    let result = unstaggered_windows.iter().find(|uw| {
        uw.window_type == UnstaggeredWindowType::Overlap
            && ((uw.cycle_a_source_id == Some(iw_a_id) && uw.cycle_b_source_id == Some(iw_b_id))
                || (uw.cycle_a_source_id == Some(iw_b_id) && uw.cycle_b_source_id == Some(iw_a_id)))
    });
    if result.is_none() {
        // Debug: find what unstaggered windows reference these IDs
        let refs_a: Vec<_> = unstaggered_windows.iter()
            .filter(|uw| uw.cycle_a_source_id == Some(iw_a_id) || uw.cycle_b_source_id == Some(iw_a_id))
            .map(|uw| format!("[{:.1}-{:.1} {:?} A={:?} B={:?}]", uw.lower_mz, uw.upper_mz, uw.window_type, uw.cycle_a_source_id, uw.cycle_b_source_id))
            .collect();
        let refs_b: Vec<_> = unstaggered_windows.iter()
            .filter(|uw| uw.cycle_a_source_id == Some(iw_b_id) || uw.cycle_b_source_id == Some(iw_b_id))
            .map(|uw| format!("[{:.1}-{:.1} {:?} A={:?} B={:?}]", uw.lower_mz, uw.upper_mz, uw.window_type, uw.cycle_a_source_id, uw.cycle_b_source_id))
            .collect();
        log::debug!("find_overlap_window({}, {}): MISS", iw_a_id, iw_b_id);
        log::debug!("  Windows referencing {}: {:?}", iw_a_id, refs_a);
        log::debug!("  Windows referencing {}: {:?}", iw_b_id, refs_b);
    }
    result
}

/// Classify a peakel by counting distinct original IWs contributing spectra.
///
/// Uses **all** data points in the peakel (not just apex ± half_window) to ensure
/// accurate classification. With sparse interleaved sampling (~3 points total),
/// restricting to the apex window would miss contributions from the neighbor window.
fn classify_peakel(
    peakel: &ExtendedPeakel,
    spectrum_to_orig_iw: &HashMap<i64, i64>,
) -> Option<PeakelWindowClassification> {
    let spectrum_ids = peakel.data.spectrum_ids.as_slice();

    // Count peaks per original IW across the full peakel extent
    let mut iw_counts: HashMap<i64, usize> = HashMap::new();
    for &spec_id in spectrum_ids {
        if let Some(&iw_id) = spectrum_to_orig_iw.get(&spec_id) {
            *iw_counts.entry(iw_id).or_default() += 1;
        }
    }

    if iw_counts.is_empty() {
        return None;
    }

    let mut iw_list: Vec<(i64, usize)> = iw_counts.into_iter().collect();
    iw_list.sort_by(|a, b| b.1.cmp(&a.1)); // Sort by count descending

    match iw_list.len() {
        1 => Some(PeakelWindowClassification::SingleWindow(iw_list[0].0)),
        2 => Some(PeakelWindowClassification::TwoWindow(
            iw_list[0].0, iw_list[0].1,
            iw_list[1].0, iw_list[1].1,
        )),
        _ => Some(PeakelWindowClassification::MultiWindow(iw_list)),
    }
}

/// Extract data points from peakels for staggered DIA using peakel-level dispatch.
///
/// Each peakel is classified by its contributing original IWs using all data points:
/// - 2-window with strong evidence (≥5 peaks, balanced): single-assign to overlap sub-window
/// - 2-window without strong evidence: duplicated across all sub-windows of both IWs
/// - 1-window: strategy-dependent (Duplicate to all sub-windows, or Remove)
/// - 3+-window: top-2 IWs resolved to overlap if adjacent, otherwise duplicated
fn extract_staggered_data_points(
    peakels: &[ExtendedPeakel],
    spectrum_to_orig_iw: &HashMap<i64, i64>,
    stagger_info: &StaggeredDiaInfo,
    half_window: usize,
    single_obs_strategy: SingleObservationStrategy,
) -> Result<(Vec<PeakelDataPoint>, DispatchStats)> {
    let mut data_points = Vec::new();
    let mut stats = DispatchStats::default();

    let orig_iw_to_unstag = build_orig_iw_to_unstaggered_map(stagger_info);

    for (i, peakel) in peakels.iter().enumerate() {
        if i % 10000 == 0 && i > 0 {
            log::debug!("Processing peakel {}/{}", i, peakels.len());
        }

        if peakel.isolation_window_id.is_none() {
            continue;
        }

        let classification = match classify_peakel(peakel, spectrum_to_orig_iw) {
            Some(c) => c,
            None => continue,
        };

        // Determine target unstaggered windows based on classification
        let target_windows: Vec<(f64, f64, f64)> = match &classification {
            PeakelWindowClassification::TwoWindow(iw_a, n_a, iw_b, n_b) => {
                let total_peaks = n_a + n_b;
                let n_peaks_diff = if *n_a > *n_b { n_a - n_b } else { n_b - n_a };

                // Single-assign only with strong evidence: enough peaks AND balanced
                let confident = total_peaks >= MIN_PEAKS_FOR_SINGLE_ASSIGN
                    && n_peaks_diff <= MAX_PEAKS_DIFF_FOR_SINGLE_ASSIGN;

                if confident {
                    stats.two_window_single_assign += 1;

                    // Find the Overlap window at the intersection
                    if let Some(uw) = find_overlap_window(*iw_a, *iw_b, &stagger_info.unstaggered_windows) {
                        vec![(uw.center_mz, uw.lower_mz, uw.upper_mz)]
                    } else {
                        // Non-adjacent IWs: fall back to duplication
                        collect_all_unstag_windows(&[*iw_a, *iw_b], &orig_iw_to_unstag)
                    }
                } else {
                    stats.two_window_duplicated += 1;

                    // Duplicate across all sub-windows of both contributing IWs
                    collect_all_unstag_windows(&[*iw_a, *iw_b], &orig_iw_to_unstag)
                }
            }

            PeakelWindowClassification::MultiWindow(iw_counts) => {
                let (iw_a, n_a) = iw_counts[0];
                let (iw_b, n_b) = iw_counts[1];
                let total_top2 = n_a + n_b;
                let diff_top2 = if n_a > n_b { n_a - n_b } else { n_b - n_a };

                let confident = total_top2 >= MIN_PEAKS_FOR_SINGLE_ASSIGN
                    && diff_top2 <= MAX_PEAKS_DIFF_FOR_SINGLE_ASSIGN;

                if confident {
                    if let Some(uw) = find_overlap_window(iw_a, iw_b, &stagger_info.unstaggered_windows) {
                        stats.multi_window_resolved += 1;
                        vec![(uw.center_mz, uw.lower_mz, uw.upper_mz)]
                    } else {
                        stats.multi_window_duplicated += 1;
                        let all_iws: Vec<i64> = iw_counts.iter().map(|(id, _)| *id).collect();
                        collect_all_unstag_windows(&all_iws, &orig_iw_to_unstag)
                    }
                } else {
                    stats.multi_window_duplicated += 1;
                    let all_iws: Vec<i64> = iw_counts.iter().map(|(id, _)| *id).collect();
                    collect_all_unstag_windows(&all_iws, &orig_iw_to_unstag)
                }
            }

            PeakelWindowClassification::SingleWindow(iw_id) => {
                stats.single_window_peakels += 1;

                match single_obs_strategy {
                    SingleObservationStrategy::Remove => {
                        stats.single_window_removed += 1;
                        continue;
                    }
                    SingleObservationStrategy::Duplicate | SingleObservationStrategy::KeepOriginal => {
                        stats.single_window_duplicated += 1;
                        // Emit to all unstaggered sub-windows of the original IW
                        orig_iw_to_unstag.get(iw_id)
                            .map(|uws| uws.iter().map(|uw| (uw.center_mz, uw.lower_mz, uw.upper_mz)).collect())
                            .unwrap_or_default()
                    }
                }
            }
        };

        if target_windows.is_empty() {
            continue;
        }

        let is_duplicated = target_windows.len() > 1;

        // Extract data points around apex and emit to all target windows
        let spectrum_ids = peakel.data.spectrum_ids.as_slice();
        let apex_idx = peakel.apex_index().unwrap_or(spectrum_ids.len() / 2);
        let start_idx = apex_idx.saturating_sub(half_window);
        let end_idx = (apex_idx + half_window).min(spectrum_ids.len().saturating_sub(1));

        for &(center_mz, lower_mz, upper_mz) in &target_windows {
            for idx in start_idx..=end_idx {
                if idx >= spectrum_ids.len() {
                    continue;
                }

                data_points.push(PeakelDataPoint {
                    spectrum_id: peakel.data.spectrum_ids[idx],
                    mz: peakel.data.mz_values[idx],
                    intensity: peakel.data.intensity_values[idx],
                    precursor_mz: center_mz,
                    isolation_lower: lower_mz,
                    isolation_upper: upper_mz,
                });

                if is_duplicated {
                    stats.data_points_duplicated += 1;
                } else {
                    stats.data_points_unique += 1;
                }
            }
        }
    }

    log::info!("Extracted {} data points from {} peakels (peakel-level dispatch)",
               data_points.len(), peakels.len());
    Ok((data_points, stats))
}

/// Collect all unstaggered sub-windows for a set of original IW ids, deduplicated.
fn collect_all_unstag_windows(
    iw_ids: &[i64],
    orig_iw_to_unstag: &HashMap<i64, Vec<&UnstaggeredWindow>>,
) -> Vec<(f64, f64, f64)> {
    let mut windows = Vec::new();
    let mut seen = std::collections::HashSet::new();
    for iw_id in iw_ids {
        if let Some(uws) = orig_iw_to_unstag.get(iw_id) {
            for uw in uws {
                let key = uw.center_mz.to_bits();
                if seen.insert(key) {
                    windows.push((uw.center_mz, uw.lower_mz, uw.upper_mz));
                }
            }
        }
    }
    windows
}

// ============================================================================
// Grouping Data Points into Simplified Spectra
// ============================================================================

/// Group data points into simplified spectra, keyed by (spectrum_id, precursor_mz)
fn group_into_spectra(
    data_points: Vec<PeakelDataPoint>,
    spectrum_info: &HashMap<i64, &SpectrumHeader>,
    mz_merge_tolerance: f32,
) -> Vec<SimplifiedSpectrum> {
    // Group by (spectrum_id, precursor_mz)
    let mut groups: BTreeMap<(i64, OrderedFloat<f64>), Vec<PeakelDataPoint>> = BTreeMap::new();

    for dp in data_points {
        let key = (dp.spectrum_id, OrderedFloat(dp.precursor_mz));
        groups.entry(key).or_default().push(dp);
    }

    let mut spectra = Vec::new();

    for ((spectrum_id, _precursor_mz), mut points) in groups {
        points.sort_by(|a, b| a.mz.total_cmp(&b.mz));

        // Merge duplicate m/z values (sum intensities)
        let mut merged_mz: Vec<f32> = Vec::new();
        let mut merged_intensity: Vec<f32> = Vec::new();

        for dp in &points {
            let dp_mz_f32 = dp.mz as f32;
            if merged_mz.is_empty()
                || (dp_mz_f32 - merged_mz.last().unwrap()).abs() > mz_merge_tolerance
            {
                merged_mz.push(dp_mz_f32);
                merged_intensity.push(dp.intensity);
            } else {
                *merged_intensity.last_mut().unwrap() += dp.intensity;
            }
        }

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

    spectra.sort_by(|a, b| match a.cycle.cmp(&b.cycle) {
        std::cmp::Ordering::Equal => a.precursor_mz.total_cmp(&b.precursor_mz),
        other => other,
    });

    spectra
}

// ============================================================================
// Title / Filter String Update
// ============================================================================

/// Update the precursor m/z in a Thermo filter string.
///
/// Replaces the float value immediately before `@` with the new precursor m/z.
/// E.g., "FTMS + c NSI Full ms2 812.62@hcd30.00 [140.00-2518.85]"
///     → "FTMS + c NSI Full ms2 806.37@hcd30.00 [140.00-2518.85]"
///
/// If the title doesn't contain `@`, falls back to a generic title.
fn update_filter_string(title: &str, new_precursor_mz: f64, cycle: i32) -> String {
    if let Some(at_pos) = title.find('@') {
        // Walk backwards from @ to find the start of the float
        let prefix_bytes = title[..at_pos].as_bytes();
        let mut float_start = at_pos;
        for i in (0..at_pos).rev() {
            let ch = prefix_bytes[i];
            if ch == b'.' || ch.is_ascii_digit() {
                float_start = i;
            } else {
                break;
            }
        }

        let before = &title[..float_start];
        let after = &title[at_pos..];
        format!("{}{:.2}{}", before, new_precursor_mz, after)
    } else {
        // Fallback: no @ found, use generic title
        format!("cycle={} msLevel=2 simplified", cycle)
    }
}

// ============================================================================
// Plan-Based Writer with Spectrum Doubling
// ============================================================================

/// A planned output MS2 spectrum in the unstaggered output.
#[derive(Debug)]
struct PlannedOutputSpectrum {
    /// Target unstaggered window bounds
    center_mz: f64,
    lower_mz: f64,
    upper_mz: f64,
}

/// Write simplified DIA spectra using MzDbWriter.
///
/// For staggered DIA, this performs spectrum doubling: each original MS2 spectrum
/// produces one output spectrum per unstaggered sub-window derived from its original
/// isolation window. This ensures no RT gaps in the output.
fn write_simplified_dia_mzdb(
    source_mzdb_path: &str,
    simplified_spectra: &[SimplifiedSpectrum],
    output_path: &Path,
    ms2_headers: &[SpectrumHeader],
    stagger_info: &StaggeredDiaInfo,
    staggered_detected: bool,
) -> Result<WriteStats> {
    use crate::writer::{MzDbWriterBuilder, WriterMetadata};
    use crate::MzDbReaderBuilder;
    use crate::model::{DataMode, PeakEncoding, ByteOrder};
    use fallible_iterator::FallibleIterator;

    log::info!("Opening source mzDB for reading...");
    let source_reader = MzDbReaderBuilder::new(source_mzdb_path).build()?;

    // Get BB sizes from entity cache
    let bb_sizes = source_reader.entity_cache().bb_sizes.clone();
    log::info!("  BB sizes: MS1 {:.1}x{:.1} Da·s, MS2 {:.1}x{:.1} Da·s",
               bb_sizes.bb_mz_height_ms1, bb_sizes.bb_rt_width_ms1,
               bb_sizes.bb_mz_height_msn, bb_sizes.bb_rt_width_msn);

    // Build lookup: (original_spectrum_id, OrderedFloat(precursor_mz)) → SimplifiedSpectrum
    let mut simplified_map: HashMap<(i64, OrderedFloat<f64>), &SimplifiedSpectrum> = HashMap::new();
    for s in simplified_spectra {
        simplified_map.insert((s.original_spectrum_id, OrderedFloat(s.precursor_mz)), s);
    }
    log::info!("  {} simplified spectrum entries", simplified_map.len());

    // Build header lookup
    let header_map: HashMap<i64, &SpectrumHeader> = ms2_headers
        .iter()
        .map(|h| (h.id, h))
        .collect();

    // Pre-build the output plan for staggered mode:
    // For each original MS2 spectrum, determine which unstaggered sub-windows it maps to.
    let orig_iw_to_unstag: HashMap<i64, Vec<&UnstaggeredWindow>> = if staggered_detected {
        build_orig_iw_to_unstaggered_map(stagger_info)
    } else {
        HashMap::new()
    };

    // Build precursor_mz → original IW ID (for finding sub-windows of a source spectrum)
    let precursor_to_orig_id: HashMap<i64, i64> = if staggered_detected {
        stagger_info.cycle_a_windows.iter().chain(stagger_info.cycle_b_windows.iter())
            .map(|w| ((w.target_mz * 100.0).round() as i64, w.id))
            .collect()
    } else {
        HashMap::new()
    };

    // Create writer
    log::info!("Creating output mzDB...");
    let metadata = WriterMetadata::with_defaults();

    let mut writer = MzDbWriterBuilder::new(output_path)
        .metadata(metadata)
        .bb_sizes(bb_sizes)
        .is_dia(true)
        .build()?;

    writer.open()?;

    let encoding = DataEncoding {
        id: 1,
        mode: DataMode::Centroid,
        peak_encoding: PeakEncoding::LowRes,
        byte_order: ByteOrder::LittleEndian,
        compression: "none".to_string(),
    };

    log::info!("Inserting spectra...");
    let mut ms1_count = 0usize;
    let mut ms2_with_data = 0usize;
    let mut ms2_empty = 0usize;
    let mut companion_spectra_created = 0usize;
    let mut next_id = 1i64;

    // Iterate through all source spectra in order
    let mut iter = source_reader.iter_spectra(None)?;
    while let Some(spectrum) = iter.next()? {
        if spectrum.header.ms_level == 1 {
            // MS1: insert with new sequential ID
            let mut ms1 = spectrum;
            ms1.header.id = next_id;
            next_id += 1;
            writer.insert_spectrum(&ms1, &encoding)?;
            ms1_count += 1;
            continue;
        }

        if spectrum.header.ms_level != 2 {
            continue;
        }

        let source_id = spectrum.header.id;
        let source_header = match header_map.get(&source_id) {
            Some(&h) => h,
            None => continue,
        };

        if !staggered_detected {
            // Non-staggered: 1:1 mapping
            let key = (source_id, OrderedFloat(spectrum.header.precursor_mz.unwrap_or(0.0)));
            if let Some(simplified) = simplified_map.get(&key) {
                let out = build_output_spectrum(next_id, source_header, simplified, &encoding);
                writer.insert_spectrum(&out, &encoding)?;
                ms2_with_data += 1;
            } else {
                let out = build_empty_output_spectrum(next_id, source_header, None, &encoding);
                writer.insert_spectrum_allow_empty(&out, &encoding)?;
                ms2_empty += 1;
            }
            next_id += 1;
            continue;
        }

        // Staggered: find unstaggered sub-windows for this source spectrum's original IW
        let precursor_mz = source_header.main_precursor_mz.unwrap_or(0.0);
        let precursor_key = (precursor_mz * 100.0).round() as i64;

        let sub_windows: Vec<PlannedOutputSpectrum> = match precursor_to_orig_id.get(&precursor_key) {
            Some(&orig_iw_id) => {
                orig_iw_to_unstag.get(&orig_iw_id)
                    .map(|uws| {
                        let mut plans: Vec<_> = uws.iter()
                            .map(|uw| PlannedOutputSpectrum {
                                center_mz: uw.center_mz,
                                lower_mz: uw.lower_mz,
                                upper_mz: uw.upper_mz,
                            })
                            .collect();
                        plans.sort_by(|a, b| a.center_mz.total_cmp(&b.center_mz));
                        plans
                    })
                    .unwrap_or_default()
            }
            None => Vec::new(),
        };

        if sub_windows.is_empty() {
            // Fallback: emit single spectrum with original bounds
            let key = (source_id, OrderedFloat(precursor_mz));
            if let Some(simplified) = simplified_map.get(&key) {
                let out = build_output_spectrum(next_id, source_header, simplified, &encoding);
                writer.insert_spectrum(&out, &encoding)?;
                ms2_with_data += 1;
            } else {
                let out = build_empty_output_spectrum(next_id, source_header, None, &encoding);
                writer.insert_spectrum_allow_empty(&out, &encoding)?;
                ms2_empty += 1;
            }
            next_id += 1;
            continue;
        }

        // Emit one output spectrum per unstaggered sub-window
        let is_companion = sub_windows.len() > 1;
        for plan in &sub_windows {
            let key = (source_id, OrderedFloat(plan.center_mz));

            if let Some(simplified) = simplified_map.get(&key) {
                let out = build_output_spectrum(next_id, source_header, simplified, &encoding);
                writer.insert_spectrum(&out, &encoding)?;
                ms2_with_data += 1;
            } else {
                let bounds = Some((plan.center_mz, plan.lower_mz, plan.upper_mz));
                let out = build_empty_output_spectrum(next_id, source_header, bounds, &encoding);
                writer.insert_spectrum_allow_empty(&out, &encoding)?;
                ms2_empty += 1;
            }

            next_id += 1;
        }

        if is_companion {
            // One of them is the "original", the rest are companions
            companion_spectra_created += sub_windows.len() - 1;
        }
    }

    log::info!("Inserted spectra:");
    log::info!("  MS1: {}", ms1_count);
    log::info!("  MS2 with data: {}", ms2_with_data);
    log::info!("  MS2 empty: {}", ms2_empty);
    log::info!("  Companion spectra created: {}", companion_spectra_created);
    log::info!("  Total: {}", ms1_count + ms2_with_data + ms2_empty);

    log::info!("Finalizing output file...");
    writer.close()?;

    log::info!("Output file written successfully!");

    Ok(WriteStats {
        ms2_with_data,
        ms2_empty,
        companion_spectra_created,
    })
}

// ============================================================================
// Output Spectrum Builders
// ============================================================================

/// Build an output spectrum with simplified data
fn build_output_spectrum(
    new_id: i64,
    source_header: &SpectrumHeader,
    simplified: &SimplifiedSpectrum,
    data_encoding: &DataEncoding,
) -> Spectrum {
    let tic: f32 = simplified.intensity_array.iter().sum();
    let (base_peak_mz, base_peak_intensity) = simplified.mz_array.iter()
        .zip(&simplified.intensity_array)
        .max_by(|a, b| a.1.total_cmp(b.1))
        .map(|(&mz, &intensity)| (mz as f64, intensity))
        .unwrap_or((0.0, 0.0));

    let precursor_list = generate_dia_precursor_list_xml_asymmetric(
        simplified.precursor_mz,
        simplified.isolation_lower,
        simplified.isolation_upper,
    );

    let title = update_filter_string(&source_header.title, simplified.precursor_mz, source_header.cycle);

    Spectrum {
        header: ModelSpectrumHeader {
            id: new_id,
            initial_id: source_header.initial_id,
            title,
            cycle: source_header.cycle as i64,
            time: source_header.time,
            ms_level: 2,
            activation_type: source_header.activation_type.clone(),
            tic,
            base_peak_mz,
            base_peak_intensity,
            precursor_mz: Some(simplified.precursor_mz),
            precursor_charge: None,
            peaks_count: simplified.mz_array.len() as i64,
            param_tree_str: None, // Deleted per plan
            scan_list_str: source_header.scan_list.clone(),
            precursor_list_str: Some(precursor_list),
            product_list_str: None,
            shared_param_tree_id: source_header.shared_param_tree_id,
            instrument_configuration_id: source_header.instrument_configuration_id,
            source_file_id: source_header.source_file_id,
            run_id: source_header.run_id,
            data_processing_id: source_header.data_processing_id,
            data_encoding_id: source_header.data_encoding_id,
            bb_first_spectrum_id: None,
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

/// Build an empty output spectrum (no peakel data dispatched here)
fn build_empty_output_spectrum(
    new_id: i64,
    source_header: &SpectrumHeader,
    window_bounds: Option<(f64, f64, f64)>,
    data_encoding: &DataEncoding,
) -> Spectrum {
    let (precursor_mz, precursor_list_str, title) = if let Some((center, lower, upper)) = window_bounds {
        let xml = generate_dia_precursor_list_xml_asymmetric(center, lower, upper);
        let t = update_filter_string(&source_header.title, center, source_header.cycle);
        (Some(center), Some(xml), t)
    } else {
        (source_header.main_precursor_mz, None, source_header.title.clone())
    };

    Spectrum {
        header: ModelSpectrumHeader {
            id: new_id,
            initial_id: source_header.initial_id,
            title,
            cycle: source_header.cycle as i64,
            time: source_header.time,
            ms_level: 2,
            activation_type: source_header.activation_type.clone(),
            tic: 0.0,
            base_peak_mz: 0.0,
            base_peak_intensity: 0.0,
            precursor_mz,
            precursor_charge: None,
            peaks_count: 0,
            param_tree_str: None, // Deleted per plan
            scan_list_str: source_header.scan_list.clone(),
            precursor_list_str,
            product_list_str: None,
            shared_param_tree_id: source_header.shared_param_tree_id,
            instrument_configuration_id: source_header.instrument_configuration_id,
            source_file_id: source_header.source_file_id,
            run_id: source_header.run_id,
            data_processing_id: source_header.data_processing_id,
            data_encoding_id: source_header.data_encoding_id,
            bb_first_spectrum_id: None,
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
