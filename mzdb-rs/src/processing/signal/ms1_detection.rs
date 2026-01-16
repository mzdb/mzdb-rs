//! MS1 Peakel Detection using Three-Slice Sliding Window Algorithm
//!
//! This module implements MS1 peakel detection following the reference Scala implementation
//! from mzdb-processing (MzDbFeatureDetector). The algorithm uses a sliding window of three
//! consecutive run slices [previous, current, next] to properly track peaks across boundaries.
//!
//! # Algorithm Overview
//!
//! 1. **Producer (Run Slice Loading)**:
//!    - Maintains a sliding window of [prev, current, next] run slices
//!    - Uses Arc-wrapped PeakListRef for efficient data sharing (no copying between windows)
//!    - Enqueues work items for parallel processing
//!
//! 2. **Consumer (Peakel Detection)**:
//!    - Sorts peaks from the CURRENT slice by descending intensity using index-based sorting
//!    - Uses PeakListTriplet for efficient cross-slice peak lookup (Scala optimization)
//!    - Walks across ALL THREE slices to extract complete XICs
//!    - Filters results to only peakels with apex in current slice's m/z range
//!
//! 3. **Optimizations**:
//!    - Arc-based data sharing avoids copying when sliding the window
//!    - Index-based sorting is 2-3x faster than tuple sorting for large peak counts
//!    - PeakListTriplet searches center slice first, then edges only when needed
//!
//! # Example
//!
//! ```no_run
//! use mzdb::MzDbReader;
//! use mzdb::processing::signal::ms1_detection::{Ms1PeakelDetector, Ms1PeakelConfig};
//!
//! let reader = MzDbReader::open("file.mzDB").unwrap();
//! let detector = Ms1PeakelDetector::new();
//! let peakels = detector.detect_peakels_with_threads(&reader, 4).unwrap();
//! println!("Detected {} MS1 peakels", peakels.len());
//! ```

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use anyhow_ext::Result;
use smallvec::SmallVec;

use crate::MzDbReader;
use crate::model::{SpectrumHeader, RunSliceHeader};
use crate::processing::Peakel;
use crate::iterator::RunSliceIterator;
use super::finder::{
    BasicPeakelFinder, PeakelFinder, SmartPeakelFinder, SmartPeakelFinderConfig,
};
use super::detection::{
    find_nearest_peak_from_slices, is_target_mz_within_range, sort_indices_by_descending_f32_value,
};

// ============================================================================
// Configuration
// ============================================================================

/// Configuration for MS1 peakel detection
#[derive(Clone, Debug)]
pub struct Ms1PeakelConfig {
    /// m/z tolerance in PPM for XIC extraction
    pub mz_tol_ppm: f64,
    /// Minimum intensity threshold for peak detection
    pub min_intensity: f32,
    /// Minimum number of points per peakel
    pub min_peaks: usize,
    /// Maximum consecutive gaps before stopping walk
    pub max_consecutive_gaps: usize,
    /// Maximum total gaps across both directions (use usize::MAX for unlimited)
    pub max_total_gaps: usize,
    /// Maximum RT window in seconds
    pub max_time_window: f32,
    /// Intensity percentile for peak filtering (0.0-1.0)
    pub intensity_percentile: f32,
    /// Minimum peakel amplitude (apex/min intensity ratio)
    pub min_peakel_amplitude: f32,
    /// Minimum peakel duration in seconds
    pub min_peakel_duration: f32,
    /// Algorithm to use: "basic" or "smart"
    pub algorithm: String,
}

impl Default for Ms1PeakelConfig {
    fn default() -> Self {
        Self {
            mz_tol_ppm: 10.0,
            min_intensity: 0.0,
            min_peaks: 5,
            max_consecutive_gaps: 3,
            max_total_gaps: usize::MAX,
            max_time_window: 1200.0,
            intensity_percentile: 0.9,
            min_peakel_amplitude: 1.5,
            min_peakel_duration: 0.0,
            algorithm: "smart".to_string(),
        }
    }
}

impl Ms1PeakelConfig {
    /// Create a PeakelFinder based on the algorithm configuration
    fn create_finder(&self) -> Box<dyn PeakelFinder + Send + Sync> {
        match self.algorithm.as_str() {
            "smart" => {
                let mut config = SmartPeakelFinderConfig::default();
                config.min_peaks_count = self.min_peaks;
                config.use_smoothing = true;
                config.use_baseline_remover = false;
                Box::new(SmartPeakelFinder::with_config(config))
            }
            _ => {
                Box::new(BasicPeakelFinder::new(2, self.min_peaks))
            }
        }
    }
}

// ============================================================================
// Data Structures for Three-Slice Sliding Window
// ============================================================================

/// A peak list for a single spectrum slice using Arc for efficient sharing.
///
/// Analogous to Scala's PeakList class - holds Arc-wrapped references to
/// m/z and intensity arrays. When the sliding window moves, only Arc references
/// are cloned (cheap ref count increment), not the actual data.
#[derive(Clone)]
struct PeakListRef {
    #[allow(dead_code)] // Kept for debugging and Scala compatibility
    spectrum_id: i64,
    time: f32,
    /// m/z values (Arc-wrapped for efficient sharing across run slices)
    mz_values: Arc<Vec<f64>>,
    /// Intensity values (Arc-wrapped)
    intensity_values: Arc<Vec<f32>>,
    /// Cached min/max m/z for fast range checks
    min_mz: f64,
    max_mz: f64,
}

impl PeakListRef {
    fn new(spectrum_id: i64, time: f32, mz_values: Vec<f64>, intensity_values: Vec<f32>) -> Self {
        let min_mz = mz_values.first().copied().unwrap_or(0.0);
        let max_mz = mz_values.last().copied().unwrap_or(0.0);
        Self {
            spectrum_id,
            time,
            mz_values: Arc::new(mz_values),
            intensity_values: Arc::new(intensity_values),
            min_mz,
            max_mz,
        }
    }

    #[inline]
    fn peaks_count(&self) -> usize {
        self.mz_values.len()
    }

    #[inline]
    fn is_empty(&self) -> bool {
        self.mz_values.is_empty()
    }

    /// Find nearest peak using binary search.
    /// Returns (mz, intensity, peak_idx) if found within tolerance.
    fn find_nearest_peak(&self, target_mz: f64, mz_tol_da: f64) -> Option<(f64, f32, usize)> {
        if self.is_empty() {
            return None;
        }

        // Quick range check using cached bounds
        if !is_target_mz_within_range(target_mz, mz_tol_da, self.min_mz, self.max_mz) {
            return None;
        }

        // Use common binary search implementation
        find_nearest_peak_from_slices(&self.mz_values, &self.intensity_values, target_mz, mz_tol_da)
    }
}

/// A triplet of peak lists for [prev, current, next] run slices of the same spectrum.
///
/// Analogous to Scala's PeakListTriplet - enables efficient cross-slice peak lookup.
/// The search is optimized to check the center slice first, then only search
/// adjacent slices when the found peak is at an edge.
struct PeakListTriplet {
    /// Peak lists from up to 3 run slices [prev, current, next]
    peak_lists: [Option<PeakListRef>; 3],
}

impl PeakListTriplet {
    fn new(prev: Option<PeakListRef>, current: Option<PeakListRef>, next: Option<PeakListRef>) -> Self {
        Self {
            peak_lists: [prev, current, next],
        }
    }

    /// Find nearest peak across all peak lists in the triplet.
    ///
    /// Optimized to search center first, then edges based on result (Scala optimization).
    /// Returns (mz, intensity, peaklist_idx, peak_idx) if found.
    fn find_nearest_peak(&self, target_mz: f64, mz_tol_da: f64) -> Option<(f64, f32, usize, usize)> {
        let mut min_mz_diff = mz_tol_da;
        let mut result: Option<(f64, f32, usize, usize)> = None;

        // Scala optimization: search center (index 1) first
        if let Some(ref pkl) = self.peak_lists[1] {
            if let Some((mz, intensity, peak_idx)) = pkl.find_nearest_peak(target_mz, mz_tol_da) {
                let mz_diff = (mz - target_mz).abs();
                if mz_diff < min_mz_diff {
                    min_mz_diff = mz_diff;
                    result = Some((mz, intensity, 1, peak_idx));

                    // If peak is at edge, also search adjacent slice
                    let peaks_count = pkl.peaks_count();
                    if peak_idx == 0 {
                        // At left edge, search prev slice too
                        if let Some(ref prev_pkl) = self.peak_lists[0] {
                            if let Some((mz2, int2, idx2)) = prev_pkl.find_nearest_peak(target_mz, mz_tol_da) {
                                let diff2 = (mz2 - target_mz).abs();
                                if diff2 < min_mz_diff {
                                    result = Some((mz2, int2, 0, idx2));
                                }
                            }
                        }
                    } else if peak_idx == peaks_count - 1 {
                        // At right edge, search next slice too
                        if let Some(ref next_pkl) = self.peak_lists[2] {
                            if let Some((mz2, int2, idx2)) = next_pkl.find_nearest_peak(target_mz, mz_tol_da) {
                                let diff2 = (mz2 - target_mz).abs();
                                if diff2 < min_mz_diff {
                                    result = Some((mz2, int2, 2, idx2));
                                }
                            }
                        }
                    }
                    return result;
                }
            }
        }

        // If center not found or empty, search all slices
        for (pkl_idx, pkl_opt) in self.peak_lists.iter().enumerate() {
            if let Some(pkl) = pkl_opt {
                if let Some((mz, intensity, peak_idx)) = pkl.find_nearest_peak(target_mz, mz_tol_da) {
                    let mz_diff = (mz - target_mz).abs();
                    if mz_diff < min_mz_diff {
                        min_mz_diff = mz_diff;
                        result = Some((mz, intensity, pkl_idx, peak_idx));
                    }
                }
            }
        }

        result
    }
}

/// Spectrum with its associated peak list triplet for cross-slice lookup
struct SpectrumWithTriplet {
    spectrum_id: i64,
    time: f32,
    triplet: PeakListTriplet,
}

/// Peak coordinate for index-based sorting.
#[derive(Clone, Copy, Debug)]
struct PeakCoord {
    spectrum_idx: usize,
    peak_idx: usize,
}

/// Peak data for a run slice window, ready for peakel detection.
struct RunSlicePeakData {
    /// Current run slice header (for m/z range filtering)
    current_rs_header: RunSliceHeader,
    /// Spectra with their triplets, sorted by time
    spectra: Vec<SpectrumWithTriplet>,
    /// Peak coordinates from CURRENT slice
    peak_coords: Vec<PeakCoord>,
    /// Peak metadata: (mz, intensity, time)
    peak_metadata: Vec<(f64, f32, f32)>,
    /// Indices sorted by descending intensity
    sorted_indices: Vec<usize>,
}

// ============================================================================
// MS1 Peakel Detector
// ============================================================================

/// MS1 Peakel Detector using three-slice sliding window algorithm
pub struct Ms1PeakelDetector {
    config: Ms1PeakelConfig,
}

impl Ms1PeakelDetector {
    /// Create a new detector with default configuration
    pub fn new() -> Self {
        Self {
            config: Ms1PeakelConfig::default(),
        }
    }

    /// Create with custom configuration
    pub fn with_config(config: Ms1PeakelConfig) -> Self {
        log::info!(
            "Ms1PeakelDetector config: min_peaks={}, mz_tol={} ppm, max_gaps={}, intensity_pct={}",
            config.min_peaks, config.mz_tol_ppm, config.max_consecutive_gaps, config.intensity_percentile
        );
        Self { config }
    }

    /// Detect peakels with single thread
    pub fn detect_peakels(&self, reader: &MzDbReader) -> Result<Vec<Peakel>> {
        self.detect_peakels_with_threads(reader, 1)
    }

    /// Detect peakels with configurable parallelism
    pub fn detect_peakels_with_threads(
        &self,
        reader: &MzDbReader,
        num_threads: usize,
    ) -> Result<Vec<Peakel>> {
        self.detect_peakels_impl(reader, num_threads)
    }

    /// Main detection implementation
    fn detect_peakels_impl(
        &self,
        reader: &MzDbReader,
        num_threads: usize,
    ) -> Result<Vec<Peakel>> {
        use crate::cache::create_entity_cache;

        log::info!("Starting MS1 peakel detection");

        let connection = reader.connection();
        let entity_cache = create_entity_cache(connection)?;

        // Get MS1 spectrum headers
        let ms1_headers: HashMap<i64, SpectrumHeader> = entity_cache.spectrum_headers
            .iter()
            .filter(|h| h.ms_level == 1)
            .map(|h| (h.id, h.clone()))
            .collect();

        log::info!("Found {} MS1 spectra", ms1_headers.len());

        if ms1_headers.is_empty() {
            return Ok(Vec::new());
        }

        if num_threads > 1 {
            #[cfg(feature = "processing-parallel")]
            {
                let db_path = reader.connection().path()
                    .ok_or_else(|| anyhow_ext::anyhow!("Cannot get database path"))?
                    .to_string();
                self.detect_parallel(&db_path, &ms1_headers, num_threads)
            }
            #[cfg(not(feature = "processing-parallel"))]
            {
                log::warn!("Parallel processing not enabled, using sequential");
                let rs_iter = RunSliceIterator::new(connection, &entity_cache)?;
                self.detect_sequential(rs_iter, &ms1_headers)
            }
        } else {
            let rs_iter = RunSliceIterator::new(connection, &entity_cache)?;
            self.detect_sequential(rs_iter, &ms1_headers)
        }
    }

    /// Sequential detection
    fn detect_sequential<'a>(
        &self,
        mut rs_iter: RunSliceIterator<'a>,
        ms1_headers: &HashMap<i64, SpectrumHeader>,
    ) -> Result<Vec<Peakel>> {
        use fallible_iterator::FallibleIterator;

        let mut all_peakels = Vec::new();
        let finder = self.config.create_finder();

        // Sliding window cache
        let mut peak_lists_cache: HashMap<i64, HashMap<i64, PeakListRef>> = HashMap::new();
        let mut prev_rs_number = 0i64;
        let mut current_rs_opt = rs_iter.next()?;
        let mut rs_count = 0;

        while current_rs_opt.is_some() {
            let current_rs = current_rs_opt.take().unwrap();
            let rs_header = current_rs.header.clone();
            let rs_number = rs_header.number;
            let next_rs_opt = rs_iter.next()?;
            let next_rs_number = rs_number + 1;

            rs_count += 1;
            log::debug!(
                "Processing run slice {}: m/z {:.2}-{:.2}",
                rs_number, rs_header.begin_mz, rs_header.end_mz
            );

            let current_peak_lists = extract_peak_lists(&current_rs, ms1_headers);

            if current_peak_lists.is_empty() {
                log::warn!("Run slice {} is empty, skipping", rs_number);
                prev_rs_number = rs_number;
                current_rs_opt = next_rs_opt;
                continue;
            }

            // Slide window
            peak_lists_cache.retain(|&rsn, _| {
                rsn == prev_rs_number || rsn == rs_number || rsn == next_rs_number
            });

            peak_lists_cache.insert(rs_number, current_peak_lists.clone());

            if let Some(ref next_rs) = next_rs_opt {
                let next_peak_lists = extract_peak_lists(next_rs, ms1_headers);
                peak_lists_cache.insert(next_rs_number, next_peak_lists);
            }

            let peak_data = build_run_slice_peak_data(
                &rs_header,
                prev_rs_number,
                rs_number,
                next_rs_number,
                &peak_lists_cache,
                &current_peak_lists,
                ms1_headers,
            )?;

            let mut peakels = detect_peakels_in_slice(peak_data, finder.as_ref(), &self.config);

            log::debug!("Run slice {}: detected {} peakels", rs_number, peakels.len());
            all_peakels.append(&mut peakels);

            prev_rs_number = rs_number;
            current_rs_opt = next_rs_opt;
        }

        log::info!("Detection complete: {} run slices, {} peakels", rs_count, all_peakels.len());
        Ok(all_peakels)
    }

    /// Parallel detection
    #[cfg(feature = "processing-parallel")]
    fn detect_parallel(
        &self,
        db_path: &str,
        ms1_headers: &HashMap<i64, SpectrumHeader>,
        num_threads: usize,
    ) -> Result<Vec<Peakel>> {
        use crossbeam_channel::{bounded, unbounded};
        use rusqlite::Connection;
        use crate::cache::create_entity_cache;

        let queue_size = num_threads * 2;
        log::info!("Parallel detection: {} threads", num_threads);

        let (work_tx, work_rx) = bounded::<Option<RunSlicePeakData>>(queue_size);
        let (results_tx, results_rx) = unbounded::<Vec<Peakel>>();

        let config = self.config.clone();
        let ms1_headers = Arc::new(ms1_headers.clone());
        let db_path = db_path.to_string();

        // Producer thread
        let ms1_headers_producer = Arc::clone(&ms1_headers);
        let producer_handle = std::thread::spawn(move || -> Result<()> {
            let connection = Connection::open(&db_path)?;
            let entity_cache = create_entity_cache(&connection)?;
            let rs_iter = RunSliceIterator::new(&connection, &entity_cache)?;
            produce_run_slice_peak_data(rs_iter, work_tx, &ms1_headers_producer, num_threads)
        });

        // Consumer threads
        let mut consumer_handles = Vec::new();
        for thread_id in 0..num_threads {
            let work_rx = work_rx.clone();
            let results_tx = results_tx.clone();
            let config = config.clone();

            let handle = std::thread::spawn(move || -> Result<()> {
                let finder = config.create_finder();
                while let Ok(Some(peak_data)) = work_rx.recv() {
                    let peakels = detect_peakels_in_slice(peak_data, finder.as_ref(), &config);
                    results_tx.send(peakels).ok();
                }
                log::debug!("Consumer {} finished", thread_id);
                Ok(())
            });
            consumer_handles.push(handle);
        }

        drop(work_rx);
        drop(results_tx);

        let mut all_peakels = Vec::new();
        while let Ok(peakels) = results_rx.recv() {
            all_peakels.extend(peakels);
        }

        if let Err(e) = producer_handle.join() {
            log::error!("Producer panicked: {:?}", e);
        }

        for handle in consumer_handles {
            if let Err(e) = handle.join() {
                log::error!("Consumer panicked: {:?}", e);
            }
        }

        log::info!("Parallel detection complete: {} peakels", all_peakels.len());
        Ok(all_peakels)
    }
}

impl Default for Ms1PeakelDetector {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// Helper Functions
// ============================================================================

/// Extract peak lists from a RunSlice as Arc-wrapped PeakListRefs.
fn extract_peak_lists(
    run_slice: &crate::model::RunSlice,
    ms1_headers: &HashMap<i64, SpectrumHeader>,
) -> HashMap<i64, PeakListRef> {
    let mut peak_lists = HashMap::new();

    for spectrum_slice in &run_slice.data.spectrum_slices {
        let spectrum_id = spectrum_slice.spectrum.header.id;
        let spectrum_data = &spectrum_slice.spectrum.data;

        if !spectrum_data.mz_array.is_empty() {
            if let Some(header) = ms1_headers.get(&spectrum_id) {
                let peak_list_ref = PeakListRef::new(
                    spectrum_id,
                    header.time,
                    spectrum_data.mz_array.clone(),
                    spectrum_data.intensity_array.clone(),
                );
                peak_lists.insert(spectrum_id, peak_list_ref);
            }
        }
    }

    peak_lists
}

/// Build peak data for a three-slice window.
fn build_run_slice_peak_data(
    rs_header: &RunSliceHeader,
    prev_rs_number: i64,
    current_rs_number: i64,
    next_rs_number: i64,
    peak_lists_cache: &HashMap<i64, HashMap<i64, PeakListRef>>,
    current_peak_lists: &HashMap<i64, PeakListRef>,
    ms1_headers: &HashMap<i64, SpectrumHeader>,
) -> Result<RunSlicePeakData> {
    // Collect all spectrum IDs
    let mut all_spectrum_ids: HashSet<i64> = HashSet::new();
    for peak_lists in peak_lists_cache.values() {
        all_spectrum_ids.extend(peak_lists.keys());
    }

    // Build triplets
    let mut spectra = Vec::with_capacity(all_spectrum_ids.len());
    let mut spectrum_idx_map: HashMap<i64, usize> = HashMap::new();

    for &spectrum_id in &all_spectrum_ids {
        if let Some(header) = ms1_headers.get(&spectrum_id) {
            let prev_pkl = peak_lists_cache
                .get(&prev_rs_number)
                .and_then(|m| m.get(&spectrum_id))
                .cloned();
            let current_pkl = peak_lists_cache
                .get(&current_rs_number)
                .and_then(|m| m.get(&spectrum_id))
                .cloned();
            let next_pkl = peak_lists_cache
                .get(&next_rs_number)
                .and_then(|m| m.get(&spectrum_id))
                .cloned();

            let triplet = PeakListTriplet::new(prev_pkl, current_pkl, next_pkl);

            let idx = spectra.len();
            spectrum_idx_map.insert(spectrum_id, idx);

            spectra.push(SpectrumWithTriplet {
                spectrum_id,
                time: header.time,
                triplet,
            });
        }
    }

    // Sort by time
    spectra.sort_by(|a, b| a.time.partial_cmp(&b.time).unwrap_or(std::cmp::Ordering::Equal));

    // Rebuild index
    spectrum_idx_map.clear();
    for (idx, info) in spectra.iter().enumerate() {
        spectrum_idx_map.insert(info.spectrum_id, idx);
    }

    // Build peak coords and metadata for CURRENT slice
    let mut peak_coords = Vec::new();
    let mut peak_metadata = Vec::new();
    let mut intensities = Vec::new();

    for (&spectrum_id, peak_list_ref) in current_peak_lists.iter() {
        if let Some(&spectrum_idx) = spectrum_idx_map.get(&spectrum_id) {
            for peak_idx in 0..peak_list_ref.peaks_count() {
                let mz = peak_list_ref.mz_values[peak_idx];
                let intensity = peak_list_ref.intensity_values[peak_idx];
                let time = peak_list_ref.time;

                peak_coords.push(PeakCoord { spectrum_idx, peak_idx });
                peak_metadata.push((mz, intensity, time));
                intensities.push(intensity);
            }
        }
    }

    // Index-based sorting (2-3x faster than tuple sorting)
    let sorted_indices = sort_indices_by_descending_f32_value(&intensities);

    Ok(RunSlicePeakData {
        current_rs_header: rs_header.clone(),
        spectra,
        peak_coords,
        peak_metadata,
        sorted_indices,
    })
}

/// Detect peakels within a run slice window.
fn detect_peakels_in_slice(
    peak_data: RunSlicePeakData,
    finder: &dyn PeakelFinder,
    config: &Ms1PeakelConfig,
) -> Vec<Peakel> {
    let rs_header = &peak_data.current_rs_header;
    let mz_tol_ppm = config.mz_tol_ppm;
    let min_peaks = config.min_peaks;
    let max_consecutive_gaps = config.max_consecutive_gaps;
    let half_of_max_total_gaps = 1 + (config.max_total_gaps / 2);
    let max_half_duration = config.max_time_window / 2.0;

    let mut peakels = Vec::new();
    let mut used_peaks: HashSet<(usize, usize, usize)> = HashSet::new();

    // Intensity threshold (Scala: intensityDescPeakCoords at 90th percentile)
    let intensity_threshold = if peak_data.sorted_indices.len() > 10 {
        let pos = (peak_data.sorted_indices.len() as f32 * config.intensity_percentile) as usize;
        let pos = pos.min(peak_data.sorted_indices.len() - 1);
        peak_data.peak_metadata[peak_data.sorted_indices[pos]].1
    } else {
        0.0
    };

    // Process peaks in descending intensity order
    for &original_idx in &peak_data.sorted_indices {
        let coord = peak_data.peak_coords[original_idx];
        let (apex_mz, apex_intensity, apex_time) = peak_data.peak_metadata[original_idx];
        let apex_spectrum_idx = coord.spectrum_idx;
        let apex_peak_idx = coord.peak_idx;

        // Skip if apex peak already used
        if used_peaks.contains(&(apex_spectrum_idx, 1, apex_peak_idx)) {
            continue;
        }

        // Stop if we reach the intensity threshold
        if apex_intensity < intensity_threshold {
            break;
        }

        // Skip if outside run slice m/z range
        if apex_mz < rs_header.begin_mz || apex_mz > rs_header.end_mz {
            continue;
        }

        // Extract XIC using walking algorithm
        let mz_tol_da = apex_mz * mz_tol_ppm * 1e-6;
        let mut xic_data: Vec<(f32, f64)> = Vec::new();
        let mut xic_mz_values: Vec<f64> = Vec::new();
        let mut xic_peak_indices: Vec<(usize, usize, usize)> = Vec::new();

        // Walk both directions (Scala: isRightDirection loop)
        for direction in [1i32, -1] {
            let mut gap_count = 0;
            let mut half_gaps_count = 0;
            let mut offset = if direction > 0 { 1 } else { 0 };

            loop {
                // Check half gaps limit (Scala: halfGapsCount <= halfOfMaxTotalGaps)
                if half_gaps_count > half_of_max_total_gaps {
                    break;
                }

                let target_idx = apex_spectrum_idx as i32 + direction * offset;
                if target_idx < 0 || target_idx as usize >= peak_data.spectra.len() {
                    break;
                }
                let target_idx = target_idx as usize;

                let spectrum = &peak_data.spectra[target_idx];

                // Check time window (Scala: Math.abs(curTime - apexTime) > this.maxHalfDuration)
                if (spectrum.time - apex_time).abs() > max_half_duration {
                    break;
                }

                if let Some((mz, intensity, pkl_idx, peak_idx)) =
                    spectrum.triplet.find_nearest_peak(apex_mz, mz_tol_da)
                {
                    // Scala: nearestPeakIdx != -1 && usedPeakMap(...).get(nearestPeakIdx) == false
                    if !used_peaks.contains(&(target_idx, pkl_idx, peak_idx)) {
                        if direction > 0 {
                            xic_data.push((spectrum.time, intensity as f64));
                            xic_mz_values.push(mz);
                            xic_peak_indices.push((target_idx, pkl_idx, peak_idx));
                        } else {
                            xic_data.insert(0, (spectrum.time, intensity as f64));
                            xic_mz_values.insert(0, mz);
                            xic_peak_indices.insert(0, (target_idx, pkl_idx, peak_idx));
                        }
                        gap_count = 0;
                    } else {
                        // Peak exists but already used - counts as gap
                        gap_count += 1;
                        half_gaps_count += 1;
                    }
                } else {
                    // No peak found - counts as gap
                    gap_count += 1;
                    half_gaps_count += 1;
                }

                // Scala: consecutiveGapCount <= maxConsecutiveGaps
                if gap_count > max_consecutive_gaps {
                    break;
                }

                offset += 1;
            }
        }

        // Add apex peak
        let apex_pos = xic_data.partition_point(|&(t, _)| t < apex_time);
        xic_data.insert(apex_pos, (apex_time, apex_intensity as f64));
        xic_mz_values.insert(apex_pos, apex_mz);
        xic_peak_indices.insert(apex_pos, (apex_spectrum_idx, 1, apex_peak_idx));

        // Detect peakels using finder
        if xic_data.len() >= min_peaks {
            let ranges = finder.find_peakels_indices(&xic_data);

            // Find the peakel containing the apex (Scala: matchingPeakelIdxOpt)
            let mut matching_range: Option<(usize, usize)> = None;
            let mut detected_peak_indices: HashSet<usize> = HashSet::new();

            for (start, end) in &ranges {
                // Check if apex is within this peakel's time range
                let start_time = xic_data[*start].0;
                let end_time = xic_data[*end].0;
                if apex_time >= start_time && apex_time <= end_time {
                    matching_range = Some((*start, *end));
                }
                // Track all detected peak indices
                for i in *start..=*end {
                    detected_peak_indices.insert(i);
                }
            }

            // Mark noisy peaks as used (Scala: peaks not in any detected peakel)
            for i in 0..xic_data.len() {
                if !detected_peak_indices.contains(&i) {
                    used_peaks.insert(xic_peak_indices[i]);
                }
            }

            // Process matching peakel if found
            if let Some((start, end)) = matching_range {
                let xic = &xic_data[start..=end];

                // Skip if not enough peaks
                if xic.len() < min_peaks {
                    continue;
                }

                // Find apex index within peakel
                let apex_index = (apex_pos as i32 - start as i32) as usize;

                // Validation 1: apex must not be first or last peak (Scala check)
                if apex_index == 0 || apex_index == xic.len() - 1 {
                    continue;
                }

                // Validation 2: check amplitude (Scala: intensityAmplitude < minPeakelAmplitude)
                let intensities: Vec<f32> = xic.iter().map(|(_, i)| *i as f32).collect();
                let min_intensity = intensities.iter().cloned().fold(f32::INFINITY, f32::min);
                let max_intensity = intensities.iter().cloned().fold(f32::NEG_INFINITY, f32::max);
                let amplitude = if min_intensity > 0.0 { max_intensity / min_intensity } else { 2.0 };

                if amplitude < config.min_peakel_amplitude {
                    continue;
                }

                // Validation 3: check duration (Scala: peakel.calcDuration() < minPeakelDuration)
                let first_time = xic.first().unwrap().0;
                let last_time = xic.last().unwrap().0;
                let duration = last_time - first_time;

                if duration < config.min_peakel_duration {
                    continue;
                }

                // All validations passed - now mark peaks as used (Scala: after validation)
                for i in start..=end {
                    used_peaks.insert(xic_peak_indices[i]);
                }

                // Build the peakel
                let spectrum_ids: SmallVec<[i64; 16]> = xic_peak_indices[start..=end]
                    .iter()
                    .map(|(idx, _, _)| peak_data.spectra[*idx].spectrum_id)
                    .collect();

                let elution_times: SmallVec<[f32; 16]> = xic.iter().map(|(t, _)| *t).collect();
                let mz_values: SmallVec<[f64; 16]> = xic_mz_values[start..=end].iter().copied().collect();
                let intensity_values: SmallVec<[f32; 16]> = intensities.into();

                peakels.push(Peakel::new(spectrum_ids, elution_times, mz_values, intensity_values, None, None));
            }
        }
    }

    peakels
}

/// Producer for parallel processing
#[cfg(feature = "processing-parallel")]
fn produce_run_slice_peak_data<'a>(
    mut rs_iter: RunSliceIterator<'a>,
    work_tx: crossbeam_channel::Sender<Option<RunSlicePeakData>>,
    ms1_headers: &HashMap<i64, SpectrumHeader>,
    num_consumers: usize,
) -> Result<()> {
    use fallible_iterator::FallibleIterator;

    let mut peak_lists_cache: HashMap<i64, HashMap<i64, PeakListRef>> = HashMap::new();
    let mut prev_rs_number = 0i64;
    let mut current_rs_opt = rs_iter.next()?;
    let mut rs_count = 0;

    while current_rs_opt.is_some() {
        let current_rs = current_rs_opt.take().unwrap();
        let rs_header = current_rs.header.clone();
        let rs_number = rs_header.number;
        let next_rs_opt = rs_iter.next()?;
        let next_rs_number = rs_number + 1;

        rs_count += 1;

        let current_peak_lists = extract_peak_lists(&current_rs, ms1_headers);

        if current_peak_lists.is_empty() {
            prev_rs_number = rs_number;
            current_rs_opt = next_rs_opt;
            continue;
        }

        peak_lists_cache.retain(|&rsn, _| {
            rsn == prev_rs_number || rsn == rs_number || rsn == next_rs_number
        });

        peak_lists_cache.insert(rs_number, current_peak_lists.clone());

        if let Some(ref next_rs) = next_rs_opt {
            peak_lists_cache.insert(next_rs_number, extract_peak_lists(next_rs, ms1_headers));
        }

        let peak_data = build_run_slice_peak_data(
            &rs_header, prev_rs_number, rs_number, next_rs_number,
            &peak_lists_cache, &current_peak_lists, ms1_headers,
        )?;

        if work_tx.send(Some(peak_data)).is_err() {
            break;
        }

        prev_rs_number = rs_number;
        current_rs_opt = next_rs_opt;
    }

    for _ in 0..num_consumers {
        work_tx.send(None).ok();
    }

    log::debug!("Producer finished: {} run slices", rs_count);
    Ok(())
}