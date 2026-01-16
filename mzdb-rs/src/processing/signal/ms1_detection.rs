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
//!    - Loads peaks from all three slices into a unified data structure
//!    - Enqueues work items for parallel processing
//!
//! 2. **Consumer (Peakel Detection)**:
//!    - Sorts peaks from the CURRENT slice by descending intensity
//!    - Uses these as starting points for the walking algorithm
//!    - Walks across ALL THREE slices to extract complete XICs
//!    - Filters results to only peakels with apex in current slice's m/z range
//!
//! 3. **Parallelization Strategy**:
//!    - Producer-consumer pattern with bounded queue
//!    - Multiple consumer threads process different run slices in parallel
//!    - Memory is bounded by queue size (typically num_threads * 2)
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

use anyhow_ext::Result;
use smallvec::SmallVec;

use crate::MzDbReader;
use crate::model::{SpectrumHeader, RunSliceHeader};
use crate::processing::Peakel;
use crate::iterator::RunSliceIterator;
use super::detection::{
    BasicPeakelFinder, PeakelFinder, SmartPeakelFinder, SmartPeakelFinderConfig,
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
    /// Maximum RT window in seconds
    pub max_time_window: f32,
    /// Intensity percentile for peak filtering (0.0-1.0)
    pub intensity_percentile: f32,
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
            max_time_window: 1200.0,
            intensity_percentile: 0.9,
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
// Data Structures for Three-Slice Algorithm
// ============================================================================

/// Indexed spectrum for fast m/z lookup
struct IndexedSpectrum {
    spectrum_id: i64,
    time: f32,
    /// Peaks sorted by m/z: (mz, intensity, peak_idx)
    peaks: Vec<(f64, f32, usize)>,
}

impl IndexedSpectrum {
    /// Find nearest peak within m/z tolerance using binary search
    fn find_nearest_peak(&self, target_mz: f64, mz_tol_da: f64) -> Option<(f64, f32, usize)> {
        if self.peaks.is_empty() {
            return None;
        }
        
        let min_mz = target_mz - mz_tol_da;
        let max_mz = target_mz + mz_tol_da;
        
        // Binary search for start position
        let start = self.peaks.partition_point(|p| p.0 < min_mz);
        
        // Find nearest peak within range
        let mut best: Option<(f64, f32, usize)> = None;
        let mut best_diff = mz_tol_da;
        
        for i in start..self.peaks.len() {
            let (mz, intensity, peak_idx) = self.peaks[i];
            if mz > max_mz {
                break;
            }
            let diff = (mz - target_mz).abs();
            if diff < best_diff {
                best_diff = diff;
                best = Some((mz, intensity, peak_idx));
            }
        }
        
        best
    }
}

/// Work item containing three run slices for peakel detection
struct RunSliceWork {
    /// Current run slice header (for m/z range filtering)
    current_slice: RunSliceHeader,
    /// All spectra from [prev, current, next] run slices indexed for fast access
    all_spectra: Vec<IndexedSpectrum>,
    /// Peaks from CURRENT slice only, sorted by descending intensity
    /// Format: (mz, intensity, rt, spectrum_idx_in_all_spectra, peak_idx_in_spectrum)
    current_peaks_sorted: Vec<(f64, f32, f32, usize, usize)>,
}

// ============================================================================
// MS1 Peakel Detector
// ============================================================================

/// MS1 Peakel Detector using three-slice sliding window algorithm
///
/// This detector follows the reference Scala implementation (MzDbFeatureDetector)
/// which uses a producer-consumer pattern with [prev, current, next] run slices.
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
        // Use RunSliceIterator-based implementation (mirrors Scala MzDbFeatureDetector)
        self.detect_peakels_with_run_slice_iterator(reader, num_threads)
    }

    /// Detect peakels using RunSliceIterator (Scala MzDbFeatureDetector algorithm)
    ///
    /// This implementation mirrors the Scala reference exactly:
    /// 1. Uses RunSliceIterator to load run slices in m/z order
    /// 2. Maintains sliding window of [prev, current, next] slices
    /// 3. Builds PeakListTree from three-slice window
    /// 4. Processes in parallel using producer-consumer pattern
    /// 5. Filters results to apex in current slice m/z range
    fn detect_peakels_with_run_slice_iterator(
        &self,
        reader: &MzDbReader,
        num_threads: usize,
    ) -> Result<Vec<Peakel>> {
        use crate::cache::create_entity_cache;

        log::info!("Starting MS1 peakel detection with RunSliceIterator (Scala algorithm)");

        // Get database path for opening new connection in producer thread
        let db_path = reader.connection().path()
            .ok_or_else(|| anyhow_ext::anyhow!("Cannot get database path from connection"))?
            .to_string();

        // Pre-fetch all required metadata using the main connection
        let connection = reader.connection();
        let entity_cache = create_entity_cache(connection)?;

        // Get MS1 spectrum headers for metadata
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
                self.detect_with_iterator_parallel(&db_path, &ms1_headers, num_threads)
            }
            #[cfg(not(feature = "processing-parallel"))]
            {
                log::warn!("Parallel processing requested but 'processing-parallel' feature not enabled, using sequential");
                let rs_iter = crate::iterator::RunSliceIterator::new(connection, &entity_cache)?;
                self.detect_with_iterator_sequential(rs_iter, &ms1_headers)
            }
        } else {
            let rs_iter = crate::iterator::RunSliceIterator::new(connection, &entity_cache)?;
            self.detect_with_iterator_sequential(rs_iter, &ms1_headers)
        }
    }

    /// Sequential detection using RunSliceIterator
    /// Parallel detection using RunSliceIterator

    /// Sequential detection using RunSliceIterator
    fn detect_with_iterator_sequential<'a>(
        &self,
        mut rs_iter: RunSliceIterator<'a>,
        ms1_headers: &HashMap<i64, SpectrumHeader>,
    ) -> Result<Vec<Peakel>> {
        use fallible_iterator::FallibleIterator;

        let mut all_peakels = Vec::new();
        let finder = self.config.create_finder();

        // Sliding window state
        let mut peak_lists_by_rs_number: HashMap<i64, HashMap<i64, Vec<(f64, f32)>>> = HashMap::new();
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
                "Processing run slice {}: m/z {:.2}-{:.2}, {} spectra",
                rs_number, rs_header.begin_mz, rs_header.end_mz,
                current_rs.data.spectrum_slices.len()
            );

            // Extract peak lists from current slice
            let current_peak_lists = extract_peak_lists_from_run_slice(&current_rs);

            if current_peak_lists.is_empty() {
                log::warn!("Run slice {} is empty, skipping", rs_number);
                prev_rs_number = rs_number;
                current_rs_opt = next_rs_opt;
                continue;
            }

            // Remove obsolete run slices (keep only prev, current, next)
            peak_lists_by_rs_number.retain(|&rsn, _| {
                rsn == prev_rs_number || rsn == rs_number || rsn == next_rs_number
            });

            // Add current slice
            peak_lists_by_rs_number.insert(rs_number, current_peak_lists.clone());

            // Add next slice if present
            if let Some(ref next_rs) = next_rs_opt {
                let next_peak_lists = extract_peak_lists_from_run_slice(next_rs);
                peak_lists_by_rs_number.insert(next_rs_number, next_peak_lists);
            }

            // Build work item from three-slice window
            let work = build_work_item_from_window(
                &rs_header,
                &peak_lists_by_rs_number,
                &current_peak_lists,
                ms1_headers,
            )?;

            // Process this run slice
            let mut peakels = process_work_item_sequential(work, finder.as_ref(), &self.config);

            log::debug!("Run slice {}: detected {} peakels", rs_number, peakels.len());
            all_peakels.append(&mut peakels);

            prev_rs_number = rs_number;
            current_rs_opt = next_rs_opt;
        }

        log::info!("Sequential detection complete: processed {} run slices, detected {} peakels",
                   rs_count, all_peakels.len());
        Ok(all_peakels)
    }

    /// Parallel detection using RunSliceIterator
    #[cfg(feature = "processing-parallel")]
    fn detect_with_iterator_parallel(
        &self,
        db_path: &str,
        ms1_headers: &HashMap<i64, SpectrumHeader>,
        num_threads: usize,
    ) -> Result<Vec<Peakel>> {
        use crossbeam_channel::{bounded, unbounded};
        use std::sync::Arc;
        use rusqlite::Connection;
        use crate::cache::create_entity_cache;

        let queue_size = num_threads * 2;
        log::info!("Starting parallel detection: {} threads, queue size {}", num_threads, queue_size);

        // Create channels
        let (work_tx, work_rx) = bounded::<Option<RunSliceWork>>(queue_size);
        let (results_tx, results_rx) = unbounded::<Vec<Peakel>>();

        // Clone data needed by threads
        let config = self.config.clone();
        let ms1_headers = Arc::new(ms1_headers.clone());
        let db_path = db_path.to_string();

        // Spawn producer thread - opens its own connection
        let ms1_headers_producer = Arc::clone(&ms1_headers);
        let producer_handle = std::thread::spawn(move || -> Result<()> {
            // Open new connection in this thread
            let connection = Connection::open(&db_path)?;
            let entity_cache = create_entity_cache(&connection)?;
            let rs_iter = crate::iterator::RunSliceIterator::new(&connection, &entity_cache)?;

            produce_work_items_from_iterator(
                rs_iter,
                work_tx,
                &ms1_headers_producer,
                num_threads,
            )
        });

        // Spawn consumer threads
        let mut consumer_handles = Vec::new();
        for thread_id in 0..num_threads {
            let work_rx = work_rx.clone();
            let results_tx = results_tx.clone();
            let config = config.clone();

            let handle = std::thread::spawn(move || -> Result<()> {
                let finder = config.create_finder();

                while let Ok(Some(work)) = work_rx.recv() {
                    let peakels = process_work_item(work, finder.as_ref(), &config);
                    results_tx.send(peakels).ok();
                }

                log::debug!("Consumer {} finished", thread_id);
                Ok(())
            });
            consumer_handles.push(handle);
        }

        // Drop original senders so channels close properly
        drop(work_rx);
        drop(results_tx);

        // Collect results
        let mut all_peakels = Vec::new();
        while let Ok(peakels) = results_rx.recv() {
            all_peakels.extend(peakels);
        }

        // Wait for producer
        if let Err(e) = producer_handle.join() {
            log::error!("Producer thread panicked: {:?}", e);
        }

        // Wait for consumers
        for handle in consumer_handles {
            if let Err(e) = handle.join() {
                log::error!("Consumer thread panicked: {:?}", e);
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
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_defaults() {
        let config = Ms1PeakelConfig::default();
        assert_eq!(config.mz_tol_ppm, 10.0);
        assert_eq!(config.min_peaks, 5);
        assert_eq!(config.max_consecutive_gaps, 3);
        assert_eq!(config.algorithm, "smart");
    }

    #[test]
    fn test_detector_creation() {
        let detector = Ms1PeakelDetector::new();
        assert_eq!(detector.config.min_peaks, 5);

        let config = Ms1PeakelConfig {
            min_peaks: 3,
            ..Default::default()
        };
        let detector = Ms1PeakelDetector::with_config(config);
        assert_eq!(detector.config.min_peaks, 3);
    }
}

// ============================================================================
// Helper Functions for RunSliceIterator-based Detection
// ============================================================================

/// Process work item for sequential execution
fn process_work_item_sequential(
    work: RunSliceWork,
    finder: &dyn PeakelFinder,
    config: &Ms1PeakelConfig,
) -> Vec<Peakel> {
    #[cfg(feature = "processing-parallel")]
    {
        process_work_item(work, finder, config)
    }
    #[cfg(not(feature = "processing-parallel"))]
    {
        // Simpler inline implementation for sequential case
        let rs_header = &work.current_slice;
        let mz_tol_ppm = config.mz_tol_ppm;
        let min_peaks = config.min_peaks;
        let max_consecutive_gaps = config.max_consecutive_gaps;
        let max_half_duration = config.max_time_window / 2.0;

        let mut peakels = Vec::new();
        let mut used_peaks: HashSet<(usize, usize)> = HashSet::new();

        // Apply intensity percentile threshold
        let intensity_threshold = if work.current_peaks_sorted.len() > 10 {
            let threshold_idx = (work.current_peaks_sorted.len() as f32 * config.intensity_percentile) as usize;
            let threshold_idx = threshold_idx.min(work.current_peaks_sorted.len() - 1);
            work.current_peaks_sorted[threshold_idx].1
        } else {
            0.0
        };

        // Process peaks in descending intensity order
        for &(apex_mz, apex_intensity, apex_time, apex_spectrum_idx, apex_peak_idx) in &work.current_peaks_sorted {
            // Check if already used
            if used_peaks.contains(&(apex_spectrum_idx, apex_peak_idx)) {
                continue;
            }

            // Check intensity threshold
            if apex_intensity < intensity_threshold {
                break;
            }

            // Check if apex is in current slice m/z range
            if apex_mz < rs_header.begin_mz || apex_mz > rs_header.end_mz {
                continue;
            }

            // Extract XIC using walking algorithm
            let mz_tol_da = apex_mz * mz_tol_ppm * 1e-6;
            let mut xic_data: Vec<(f32, f64)> = Vec::new();
            let mut xic_peak_indices: Vec<(usize, usize)> = Vec::new();

            // Walk in both directions from apex
            for direction in [1i32, -1] {
                let mut gap_count = 0;
                let mut offset = if direction > 0 { 1 } else { 0 };

                loop {
                    let target_idx = (apex_spectrum_idx as i32 + direction * offset) as usize;
                    if target_idx >= work.all_spectra.len() {
                        break;
                    }

                    let spectrum = &work.all_spectra[target_idx];

                    // Check time window
                    if (spectrum.time - apex_time).abs() > max_half_duration {
                        break;
                    }

                    // Find nearest peak
                    if let Some((mz, intensity, peak_idx)) = spectrum.find_nearest_peak(apex_mz, mz_tol_da) {
                        if !used_peaks.contains(&(target_idx, peak_idx)) {
                            if direction > 0 {
                                xic_data.push((spectrum.time, intensity as f64));
                                xic_peak_indices.push((target_idx, peak_idx));
                            } else {
                                xic_data.insert(0, (spectrum.time, intensity as f64));
                                xic_peak_indices.insert(0, (target_idx, peak_idx));
                            }
                            gap_count = 0;
                        } else {
                            gap_count += 1;
                        }
                    } else {
                        gap_count += 1;
                    }

                    if gap_count > max_consecutive_gaps {
                        break;
                    }

                    offset += 1;
                }
            }

            // Add apex point
            let apex_insert_pos = xic_data.partition_point(|&(t, _)| t < apex_time);
            xic_data.insert(apex_insert_pos, (apex_time, apex_intensity as f64));
            xic_peak_indices.insert(apex_insert_pos, (apex_spectrum_idx, apex_peak_idx));

            // Detect peakels in XIC
            if xic_data.len() >= min_peaks {
                let peakel_ranges = finder.find_peakels_indices(&xic_data);

                for (start_idx, end_idx) in peakel_ranges {
                    let peakel_xic = &xic_data[start_idx..=end_idx];

                    // Check if apex is in this peakel
                    let has_apex = peakel_xic.iter().any(|&(t, _)| (t - apex_time).abs() < 0.01);

                    if has_apex && peakel_xic.len() >= min_peaks {
                        // Mark peaks as used
                        for i in start_idx..=end_idx {
                            used_peaks.insert(xic_peak_indices[i]);
                        }

                        // Build peakel from XIC data
                        let spectrum_ids: SmallVec<[i64; 16]> = xic_peak_indices[start_idx..=end_idx]
                            .iter()
                            .map(|(spec_idx, _)| work.all_spectra[*spec_idx].spectrum_id)
                            .collect();

                        let elution_times: SmallVec<[f32; 16]> = peakel_xic
                            .iter()
                            .map(|(t, _)| *t)
                            .collect();

                        let mz_values: SmallVec<[f64; 16]> = std::iter::repeat(apex_mz)
                            .take(peakel_xic.len())
                            .collect();

                        let intensity_values: SmallVec<[f32; 16]> = peakel_xic
                            .iter()
                            .map(|(_, i)| *i as f32)
                            .collect();

                        let peakel = Peakel::new(
                            spectrum_ids,
                            elution_times,
                            mz_values,
                            intensity_values,
                            None,
                            None,
                        );
                        peakels.push(peakel);
                    }
                }
            }
        }

        peakels
    }
}

/// Extract peak lists from a RunSlice
/// Returns HashMap<spectrum_id, Vec<(mz, intensity)>>
fn extract_peak_lists_from_run_slice(
    run_slice: &crate::model::RunSlice
) -> HashMap<i64, Vec<(f64, f32)>> {
    let mut peak_lists = HashMap::new();

    for spectrum_slice in &run_slice.data.spectrum_slices {
        let spectrum_id = spectrum_slice.spectrum.header.id;
        let spectrum_data = &spectrum_slice.spectrum.data;

        let peaks: Vec<(f64, f32)> = spectrum_data.mz_array.iter()
            .zip(spectrum_data.intensity_array.iter())
            .map(|(&mz, &intensity)| (mz, intensity))
            .collect();

        if !peaks.is_empty() {
            peak_lists.insert(spectrum_id, peaks);
        }
    }

    peak_lists
}

/// Build work item from three-slice window
fn build_work_item_from_window(
    rs_header: &RunSliceHeader,
    peak_lists_by_rs_number: &HashMap<i64, HashMap<i64, Vec<(f64, f32)>>>,
    current_peak_lists: &HashMap<i64, Vec<(f64, f32)>>,
    ms1_headers: &HashMap<i64, SpectrumHeader>,
) -> Result<RunSliceWork> {
    // Group peaks by spectrum_id across all run slices in window
    let mut peaks_by_spectrum_id: HashMap<i64, Vec<Vec<(f64, f32)>>> = HashMap::new();

    for peak_lists in peak_lists_by_rs_number.values() {
        for (&spectrum_id, peaks) in peak_lists.iter() {
            peaks_by_spectrum_id
                .entry(spectrum_id)
                .or_default()
                .push(peaks.clone());
        }
    }

    // Build indexed spectra
    let mut all_spectra = Vec::new();
    let mut spectrum_idx_map = HashMap::new();

    for (spectrum_id, peak_lists_for_spectrum) in peaks_by_spectrum_id {
        if let Some(header) = ms1_headers.get(&spectrum_id) {
            // Merge peaks from all slices for this spectrum
            let mut merged_peaks: Vec<(f64, f32, usize)> = Vec::new();
            for peaks in peak_lists_for_spectrum {
                for (peak_idx, (mz, intensity)) in peaks.into_iter().enumerate() {
                    merged_peaks.push((mz, intensity, peak_idx));
                }
            }

            // Sort by m/z for binary search
            merged_peaks.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));

            let idx = all_spectra.len();
            spectrum_idx_map.insert(spectrum_id, idx);

            all_spectra.push(IndexedSpectrum {
                spectrum_id,
                time: header.time,
                peaks: merged_peaks,
            });
        }
    }

    // Sort spectra by time
    all_spectra.sort_by(|a, b| a.time.partial_cmp(&b.time).unwrap_or(std::cmp::Ordering::Equal));

    // Rebuild index after sorting
    spectrum_idx_map.clear();
    for (idx, spec) in all_spectra.iter().enumerate() {
        spectrum_idx_map.insert(spec.spectrum_id, idx);
    }

    // Sort peaks from CURRENT slice by descending intensity
    let mut current_peaks_sorted = Vec::new();

    for (&spectrum_id, peaks) in current_peak_lists.iter() {
        if let Some(&spectrum_idx) = spectrum_idx_map.get(&spectrum_id) {
            if let Some(header) = ms1_headers.get(&spectrum_id) {
                for (peak_idx, &(mz, intensity)) in peaks.iter().enumerate() {
                    current_peaks_sorted.push((mz, intensity, header.time, spectrum_idx, peak_idx));
                }
            }
        }
    }

    // Sort by descending intensity
    current_peaks_sorted.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));

    Ok(RunSliceWork {
        current_slice: rs_header.clone(),
        all_spectra,
        current_peaks_sorted,
    })
}

/// Producer function for parallel processing
#[cfg(feature = "processing-parallel")]
fn produce_work_items_from_iterator<'a>(
    mut rs_iter: crate::iterator::RunSliceIterator<'a>,
    work_tx: crossbeam_channel::Sender<Option<RunSliceWork>>,
    ms1_headers: &HashMap<i64, SpectrumHeader>,
    num_consumers: usize,
) -> Result<()> {
    use fallible_iterator::FallibleIterator;

    let mut peak_lists_by_rs_number: HashMap<i64, HashMap<i64, Vec<(f64, f32)>>> = HashMap::new();
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

        // Extract peak lists from current slice
        let current_peak_lists = extract_peak_lists_from_run_slice(&current_rs);

        if current_peak_lists.is_empty() {
            log::warn!("Run slice {} is empty, skipping", rs_number);
            prev_rs_number = rs_number;
            current_rs_opt = next_rs_opt;
            continue;
        }

        // Remove obsolete run slices
        peak_lists_by_rs_number.retain(|&rsn, _| {
            rsn == prev_rs_number || rsn == rs_number || rsn == next_rs_number
        });

        // Add current
        peak_lists_by_rs_number.insert(rs_number, current_peak_lists.clone());

        // Add next if present
        if let Some(ref next_rs) = next_rs_opt {
            let next_peak_lists = extract_peak_lists_from_run_slice(next_rs);
            peak_lists_by_rs_number.insert(next_rs_number, next_peak_lists);
        }

        // Build work item
        let work = build_work_item_from_window(
            &rs_header,
            &peak_lists_by_rs_number,
            &current_peak_lists,
            ms1_headers,
        )?;

        // Send to queue
        if work_tx.send(Some(work)).is_err() {
            log::error!("Failed to send work item, channel closed");
            break;
        }

        prev_rs_number = rs_number;
        current_rs_opt = next_rs_opt;
    }

    // Send termination signals
    for _ in 0..num_consumers {
        work_tx.send(None).ok();
    }

    log::debug!("Producer finished: processed {} run slices", rs_count);
    Ok(())
}

/// Process a work item (used by consumer threads)
#[cfg(feature = "processing-parallel")]
fn process_work_item(
    work: RunSliceWork,
    finder: &dyn PeakelFinder,
    config: &Ms1PeakelConfig,
) -> Vec<Peakel> {
    let rs_header = &work.current_slice;
    let mz_tol_ppm = config.mz_tol_ppm;
    let min_peaks = config.min_peaks;
    let max_consecutive_gaps = config.max_consecutive_gaps;
    let max_half_duration = config.max_time_window / 2.0;

    let mut peakels = Vec::new();
    let mut used_peaks: HashSet<(usize, usize)> = HashSet::new();

    // Apply intensity percentile threshold
    let intensity_threshold = if work.current_peaks_sorted.len() > 10 {
        let threshold_idx = (work.current_peaks_sorted.len() as f32 * config.intensity_percentile) as usize;
        let threshold_idx = threshold_idx.min(work.current_peaks_sorted.len() - 1);
        work.current_peaks_sorted[threshold_idx].1
    } else {
        0.0
    };

    // Process peaks in descending intensity order
    for &(apex_mz, apex_intensity, apex_time, apex_spectrum_idx, apex_peak_idx) in &work.current_peaks_sorted {
        // Check if already used
        if used_peaks.contains(&(apex_spectrum_idx, apex_peak_idx)) {
            continue;
        }

        // Check intensity threshold
        if apex_intensity < intensity_threshold {
            break;
        }

        // Check if apex is in current slice m/z range
        if apex_mz < rs_header.begin_mz || apex_mz > rs_header.end_mz {
            continue;
        }

        // Extract XIC using walking algorithm
        let mz_tol_da = apex_mz * mz_tol_ppm * 1e-6;
        let mut xic_data: Vec<(f32, f64)> = Vec::new();
        let mut xic_mz_values: Vec<f64> = Vec::new();
        let mut xic_spectrum_ids: Vec<i64> = Vec::new();
        let mut xic_peak_indices: Vec<(usize, usize)> = Vec::new();

        // Walk in both directions from apex
        for direction in [1i32, -1] {
            let mut gap_count = 0;
            let mut offset = if direction > 0 { 1 } else { 0 };

            loop {
                let target_idx = (apex_spectrum_idx as i32 + direction * offset) as usize;
                if target_idx >= work.all_spectra.len() {
                    break;
                }

                let spectrum = &work.all_spectra[target_idx];

                // Check time window
                if (spectrum.time - apex_time).abs() > max_half_duration {
                    break;
                }

                // Find nearest peak
                if let Some((mz, intensity, peak_idx)) = spectrum.find_nearest_peak(apex_mz, mz_tol_da) {
                    if !used_peaks.contains(&(target_idx, peak_idx)) {
                        if direction > 0 {
                            xic_data.push((spectrum.time, intensity as f64));
                            xic_mz_values.push(mz);
                            xic_spectrum_ids.push(spectrum.spectrum_id);
                            xic_peak_indices.push((target_idx, peak_idx));
                        } else {
                            xic_data.insert(0, (spectrum.time, intensity as f64));
                            xic_mz_values.insert(0, mz);
                            xic_spectrum_ids.insert(0, spectrum.spectrum_id);
                            xic_peak_indices.insert(0, (target_idx, peak_idx));
                        }
                        gap_count = 0;
                    } else {
                        gap_count += 1;
                    }
                } else {
                    gap_count += 1;
                }

                if gap_count > max_consecutive_gaps {
                    break;
                }

                offset += 1;
            }
        }

        // Add apex point
        let apex_insert_pos = xic_data.partition_point(|&(t, _)| t < apex_time);
        xic_data.insert(apex_insert_pos, (apex_time, apex_intensity as f64));
        xic_mz_values.insert(apex_insert_pos, apex_mz);
        xic_spectrum_ids.insert(apex_insert_pos, work.all_spectra[apex_spectrum_idx].spectrum_id);
        xic_peak_indices.insert(apex_insert_pos, (apex_spectrum_idx, apex_peak_idx));

        // Detect peakels in XIC
        if xic_data.len() >= min_peaks {
            let peakel_ranges = finder.find_peakels_indices(&xic_data);

            for (start_idx, end_idx) in peakel_ranges {
                let peakel_xic = &xic_data[start_idx..=end_idx];

                // Check if apex is in this peakel
                let has_apex = peakel_xic.iter().any(|&(t, _)| (t - apex_time).abs() < 0.01);

                if has_apex && peakel_xic.len() >= min_peaks {
                    // Mark peaks as used
                    for i in start_idx..=end_idx {
                        used_peaks.insert(xic_peak_indices[i]);
                    }

                    // Build peakel from XIC data
                    let spectrum_ids: SmallVec<[i64; 16]> = xic_peak_indices[start_idx..=end_idx]
                        .iter()
                        .map(|(spec_idx, _)| work.all_spectra[*spec_idx].spectrum_id)
                        .collect();

                    let elution_times: SmallVec<[f32; 16]> = peakel_xic
                        .iter()
                        .map(|(t, _)| *t)
                        .collect();

                    let mz_values: SmallVec<[f64; 16]> = xic_mz_values[start_idx..=end_idx]
                        .iter()
                        .copied()
                        .collect();

                    let intensity_values: SmallVec<[f32; 16]> = peakel_xic
                        .iter()
                        .map(|(_, i)| *i as f32)
                        .collect();

                    let peakel = Peakel::new(
                        spectrum_ids,
                        elution_times,
                        mz_values,
                        intensity_values,
                        None,
                        None,
                    );
                    peakels.push(peakel);
                }
            }
        }
    }

    peakels
}