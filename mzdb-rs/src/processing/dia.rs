//! DIA (Data Independent Acquisition) MS2 Peakel Detection
//!
//! This module provides MS2-level peakel detection for DIA data.
//! It processes each isolation window individually to save memory,
//! using the same walking algorithm as MS1 peakel detection.
//!
//! # Architecture
//!
//! The algorithm processes DIA data by:
//! 1. Discovering all unique isolation windows (precursor m/z ranges)
//! 2. For each isolation window:
//!    - Load MS2 spectra for that window
//!    - Build indexed spectra for fast m/z queries
//!    - Use the walking algorithm to detect peakels
//!    - Write peakels with isolation window metadata
//! 3. Create a modified peakelDB with isolation window mapping
//!
//! # Example
//!
//! ```no_run
//! use mzdb::MzDbReader;
//! use mzdb::processing::dia::{DiaMs2PeakelDetector, DiaMs2PeakelConfig};
//!
//! let reader = MzDbReader::open("dia_file.mzDB").unwrap();
//! let detector = DiaMs2PeakelDetector::new();
//! let (windows, peakels) = detector.detect_all_peakels(&reader).unwrap();
//! println!("Detected {} peakels across {} windows", peakels.len(), windows.len());
//! ```

use std::collections::BTreeMap;
use std::path::PathBuf;

use anyhow_ext::anyhow;

use crate::MzDbReader;
use crate::processing::signal::detection::{BasicPeakelFinder, PeakelFinder, SmartPeakelFinder, SmartPeakelFinderConfig};

/// Isolation window definition for DIA
#[derive(Clone, Debug)]
pub struct IsolationWindow {
    /// Unique identifier for this isolation window
    pub id: i64,
    /// Center m/z of the isolation window
    pub target_mz: f64,
    /// Lower m/z bound (target_mz - lower_offset)
    pub lower_mz: f64,
    /// Upper m/z bound (target_mz + upper_offset)
    pub upper_mz: f64,
    /// Number of MS2 spectra in this window
    pub spectrum_count: usize,
}

/// Raw peaks data for serialization in messagepack format
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct PeaksData {
    /// Spectrum IDs for each data point
    pub spectrum_ids: Vec<i64>,
    /// Elution times for each data point
    pub elution_times: Vec<f32>,
    /// m/z values for each data point  
    pub mz_values: Vec<f64>,
    /// Intensity values for each data point
    pub intensity_values: Vec<f32>,
}

impl PeaksData {
    /// Create new empty PeaksData
    pub fn new() -> Self {
        Self {
            spectrum_ids: Vec::new(),
            elution_times: Vec::new(),
            mz_values: Vec::new(),
            intensity_values: Vec::new(),
        }
    }

    /// Create PeaksData with pre-allocated capacity
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            spectrum_ids: Vec::with_capacity(capacity),
            elution_times: Vec::with_capacity(capacity),
            mz_values: Vec::with_capacity(capacity),
            intensity_values: Vec::with_capacity(capacity),
        }
    }

    /// Get the number of peaks
    pub fn len(&self) -> usize {
        self.spectrum_ids.len()
    }

    /// Check if empty
    pub fn is_empty(&self) -> bool {
        self.spectrum_ids.is_empty()
    }

    /// Serialize to messagepack bytes
    pub fn to_msgpack(&self) -> anyhow_ext::Result<Vec<u8>> {
        // Serialize as tuple of arrays for compact representation
        let data = (
            &self.spectrum_ids,
            &self.elution_times,
            &self.mz_values,
            &self.intensity_values,
        );
        rmp_serde::to_vec(&data)
            .map_err(|e| anyhow!("msgpack serialization error: {}", e))
    }
    
    /// Deserialize from messagepack bytes
    pub fn from_msgpack(bytes: &[u8]) -> anyhow_ext::Result<Self> {
        let (spectrum_ids, elution_times, mz_values, intensity_values): 
            (Vec<i64>, Vec<f32>, Vec<f64>, Vec<f32>) = 
            rmp_serde::from_slice(bytes)
                .map_err(|e| anyhow!("msgpack deserialization error: {}", e))?;
        
        Ok(Self {
            spectrum_ids,
            elution_times,
            mz_values,
            intensity_values,
        })
    }
}

impl Default for PeaksData {
    fn default() -> Self {
        Self::new()
    }
}

/// Peakel record with isolation window mapping for DIA
#[derive(Clone, Debug)]
pub struct DiaMs2PeakelRecord {
    pub id: i64,
    /// Fragment m/z (from MS2 spectrum)
    pub mz: f64,
    /// Elution time at apex
    pub elution_time: f32,
    /// Total duration
    pub duration: f32,
    /// Intensity at apex
    pub apex_intensity: f32,
    /// Integrated area
    pub area: f32,
    /// Peak amplitude (apex / baseline)
    pub amplitude: f32,
    /// Number of gaps in the peakel
    pub gap_count: usize,
    /// Number of peaks in the peakel
    pub peaks_count: usize,
    /// First spectrum ID
    pub first_spectrum_id: i64,
    /// Apex spectrum ID
    pub apex_spectrum_id: i64,
    /// Last spectrum ID
    pub last_spectrum_id: i64,
    /// Isolation window ID (foreign key to isolation_window table)
    pub isolation_window_id: i64,
    /// Precursor m/z (isolation window target)
    pub precursor_mz: f64,
    /// Raw peaks data (mz, intensity, rt arrays)
    pub peaks: PeaksData,
}

/// Optimized spectrum data for fast m/z range queries using binary search
#[allow(dead_code)]
struct IndexedMs2Spectrum {
    spectrum_idx: usize,
    spectrum_id: i64,
    time: f32,
    /// Peak data sorted by m/z: (mz, intensity, original_peak_idx)
    peaks: Vec<(f64, f32, usize)>,
}

impl IndexedMs2Spectrum {
    /// Find the nearest peak within m/z tolerance using binary search
    fn find_nearest_peak(&self, target_mz: f64, mz_tol_da: f64) -> Option<(f64, f32, usize)> {
        if self.peaks.is_empty() {
            return None;
        }
        
        let min_mz = target_mz - mz_tol_da;
        let max_mz = target_mz + mz_tol_da;
        
        // Binary search for start position
        let start = self.peaks.partition_point(|p| p.0 < min_mz);
        
        // Find the nearest peak within the m/z range
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

/// Configuration for DIA MS2 peakel detection
#[derive(Clone, Debug)]
pub struct DiaMs2PeakelConfig {
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
    /// Algorithm to use: "basic" or "smart"
    pub algorithm: String,
}

impl Default for DiaMs2PeakelConfig {
    fn default() -> Self {
        Self {
            mz_tol_ppm: 10.0,
            min_intensity: 100.0,
            min_peaks: 5,
            max_consecutive_gaps: 3,
            max_time_window: 1200.0,
            algorithm: "smart".to_string(),
        }
    }
}

/// DIA MS2 Peakel Detector
///
/// Processes DIA data by iterating over each isolation window,
/// detecting peakels in the MS2 spectra for that window.
pub struct DiaMs2PeakelDetector {
    config: DiaMs2PeakelConfig,
}

impl DiaMs2PeakelDetector {
    /// Create a new detector with default configuration
    pub fn new() -> Self {
        Self {
            config: DiaMs2PeakelConfig::default(),
        }
    }

    /// Create with custom configuration
    pub fn with_config(config: DiaMs2PeakelConfig) -> Self {
        log::info!("DiaMs2PeakelDetector config: min_peaks={}, mz_tol={} ppm, max_gaps={}", 
                   config.min_peaks, config.mz_tol_ppm, config.max_consecutive_gaps);
        Self { config }
    }

    /// Discover all isolation windows in the mzDB file
    pub fn discover_isolation_windows(&self, reader: &MzDbReader) -> Vec<IsolationWindow> {
        let headers = reader.get_spectrum_headers();
        
        // Group MS2 spectra by precursor m/z
        let mut window_counts: BTreeMap<i64, (f64, usize)> = BTreeMap::new();
        
        for header in headers {
            if header.ms_level == 2 {
                if let Some(precursor_mz) = header.precursor_mz {
                    // Round to 0.1 m/z for grouping
                    let window_key = (precursor_mz * 10.0).round() as i64;
                    let entry = window_counts.entry(window_key).or_insert((precursor_mz, 0));
                    entry.1 += 1;
                }
            }
        }
        
        // Convert to IsolationWindow structs
        window_counts.into_iter()
            .enumerate()
            .map(|(idx, (_key, (target_mz, count)))| {
                // Assume standard 25 Da half-width for DIA windows
                // This could be parsed from precursor_list XML for more accuracy
                let half_width = 25.0;
                IsolationWindow {
                    id: (idx + 1) as i64,
                    target_mz,
                    lower_mz: target_mz - half_width,
                    upper_mz: target_mz + half_width,
                    spectrum_count: count,
                }
            })
            .collect()
    }

    /// Detect peakels for a single isolation window
    ///
    /// This method loads MS2 spectra only for the specified isolation window,
    /// processes them using the walking algorithm, and returns detected peakels.
    pub fn detect_peakels_for_window(
        &self,
        reader: &MzDbReader,
        window: &IsolationWindow,
    ) -> anyhow_ext::Result<Vec<DiaMs2PeakelRecord>> {
        log::info!("Processing isolation window: {:.1} m/z ({} spectra)", 
                   window.target_mz, window.spectrum_count);
        
        // Get MS2 spectra for this isolation window using efficient SQL filtering
        let spectra = reader.get_dia_spectra_for_window(window.target_mz)?;
        
        if spectra.is_empty() {
            return Ok(Vec::new());
        }
        
        // Build indexed spectra for fast m/z lookup
        let indexed_spectra = self.build_indexed_spectra(&spectra);
        
        // Run the walking algorithm
        let detected_peakels = self.run_walking_algorithm(indexed_spectra, window);
        
        log::info!("  Detected {} MS2 peakels in window {:.1}", 
                   detected_peakels.len(), window.target_mz);
        
        Ok(detected_peakels)
    }
    
    /// Build indexed spectra from raw spectra for fast m/z lookup
    fn build_indexed_spectra(&self, spectra: &[crate::model::Spectrum]) -> Vec<IndexedMs2Spectrum> {
        let mut indexed_spectra: Vec<IndexedMs2Spectrum> = Vec::with_capacity(spectra.len());
        
        for (idx, spectrum) in spectra.iter().enumerate() {
            // Collect peaks that pass intensity threshold
            let peaks: Vec<(f64, f32, usize)> = spectrum.data.mz_array.iter()
                .zip(spectrum.data.intensity_array.iter())
                .enumerate()
                .filter(|(_, (_, intensity))| **intensity >= self.config.min_intensity)
                .map(|(peak_idx, (&mz, &intensity))| (mz, intensity, peak_idx))
                .collect();
            
            indexed_spectra.push(IndexedMs2Spectrum {
                spectrum_idx: idx,
                spectrum_id: spectrum.header.id,
                time: spectrum.header.time,
                peaks,
            });
        }
        
        indexed_spectra
    }
    
    /// Core walking algorithm for peakel detection
    ///
    /// This is the main detection algorithm shared by all detection methods.
    /// It takes pre-built indexed spectra and returns detected peakels.
    fn run_walking_algorithm(
        &self,
        mut indexed_spectra: Vec<IndexedMs2Spectrum>,
        window: &IsolationWindow,
    ) -> Vec<DiaMs2PeakelRecord> {
        use std::collections::HashSet;
        
        if indexed_spectra.is_empty() {
            return Vec::new();
        }
        
        // Sort spectra by time for proper walking
        indexed_spectra.sort_by(|a, b| a.time.partial_cmp(&b.time).unwrap_or(std::cmp::Ordering::Equal));
        
        // Collect all peaks: (mz, intensity, rt, spectrum_idx, peak_index)
        let mut all_peaks: Vec<(f64, f32, f32, usize, usize)> = Vec::new();
        
        for (new_idx, indexed_spec) in indexed_spectra.iter().enumerate() {
            for &(mz, intensity, peak_idx) in &indexed_spec.peaks {
                all_peaks.push((mz, intensity, indexed_spec.time, new_idx, peak_idx));
            }
        }
        
        // Sort peaks by intensity (descending)
        all_peaks.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
        
        // Track used peaks
        let mut used_peaks: Vec<HashSet<usize>> = vec![HashSet::new(); indexed_spectra.len()];
        
        // Create the peakel finder with the configured min_peaks
        let finder: Box<dyn PeakelFinder> = match self.config.algorithm.as_str() {
            "smart" => {
                let mut smart_config = SmartPeakelFinderConfig::default();
                smart_config.min_peaks_count = self.config.min_peaks;
                Box::new(SmartPeakelFinder::with_config(smart_config))
            },
            _ => Box::new(BasicPeakelFinder::default_params()),
        };
        
        let mut detected_peakels: Vec<DiaMs2PeakelRecord> = Vec::new();
        let mut peakel_id = 1i64;
        
        // Walking algorithm - process peaks from highest to lowest intensity
        for &(apex_mz, _apex_intensity, apex_rt, apex_spectrum_idx, apex_peak_idx) in &all_peaks {
            // Skip if already used
            if used_peaks[apex_spectrum_idx].contains(&apex_peak_idx) {
                continue;
            }
            
            // Calculate m/z tolerance in Daltons
            let mz_tol_da = apex_mz * self.config.mz_tol_ppm / 1_000_000.0;
            
            // XIC extraction using walking approach
            let mut xic_peaks: Vec<(f64, f32, f32, usize, usize)> = Vec::new();
            
            // Add the apex peak
            xic_peaks.push((apex_mz, _apex_intensity, apex_rt, apex_spectrum_idx, apex_peak_idx));
            
            // Walk in both directions: right (+1) first, then left (-1)
            for direction in [1i32, -1i32] {
                let mut consecutive_gap_count = 0usize;
                let mut offset = 1i32;
                
                loop {
                    let cur_idx = apex_spectrum_idx as i32 + (offset * direction);
                    
                    // Check bounds
                    if cur_idx < 0 || cur_idx >= indexed_spectra.len() as i32 {
                        break;
                    }
                    
                    let cur_spectrum = &indexed_spectra[cur_idx as usize];
                    
                    // Check time window
                    if (cur_spectrum.time - apex_rt).abs() > self.config.max_time_window / 2.0 {
                        break;
                    }
                    
                    // Try to find the nearest peak within m/z tolerance
                    if let Some((mz, intensity, peak_idx)) = cur_spectrum.find_nearest_peak(apex_mz, mz_tol_da) {
                        // Stop at used peak boundary
                        if used_peaks[cur_idx as usize].contains(&peak_idx) {
                            break;
                        }
                        
                        if direction > 0 {
                            xic_peaks.push((mz, intensity, cur_spectrum.time, cur_idx as usize, peak_idx));
                        } else {
                            xic_peaks.insert(0, (mz, intensity, cur_spectrum.time, cur_idx as usize, peak_idx));
                        }
                        consecutive_gap_count = 0;
                    } else {
                        consecutive_gap_count += 1;
                    }
                    
                    // Stop if too many consecutive gaps
                    if consecutive_gap_count > self.config.max_consecutive_gaps {
                        break;
                    }
                    
                    offset += 1;
                }
            }
            
            // Need at least min_peaks for peakel detection
            if xic_peaks.len() < self.config.min_peaks {
                continue;
            }
            
            // Convert to time-intensity pairs for peakel detection
            let xic_pairs: Vec<(f32, f64)> = xic_peaks.iter()
                .map(|(_, int, rt, _, _)| (*rt, *int as f64))
                .collect();
            
            // Detect peakels
            let peakel_indices = finder.find_peakels_indices(&xic_pairs);
            
            // Process all detected peakels
            for (start, end) in &peakel_indices {
                if end - start + 1 < self.config.min_peaks {
                    continue;
                }
                
                let peakel_peaks = &xic_peaks[*start..=*end];
                
                // Check if any peak is already used
                let any_used = peakel_peaks.iter()
                    .any(|(_, _, _, spec_idx, peak_idx)| used_peaks[*spec_idx].contains(peak_idx));
                
                if any_used {
                    continue;
                }
                
                // Find the actual apex
                let (_, apex_peak) = peakel_peaks.iter()
                    .enumerate()
                    .max_by(|(_, a), (_, b)| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal))
                    .unwrap();
                
                // Calculate weighted m/z
                let total_intensity: f64 = peakel_peaks.iter().map(|(_, i, _, _, _)| *i as f64).sum();
                let weighted_mz = if total_intensity > 0.0 {
                    peakel_peaks.iter()
                        .map(|(mz, i, _, _, _)| *mz * *i as f64)
                        .sum::<f64>() / total_intensity
                } else {
                    apex_peak.0
                };
                
                // Calculate area using trapezoidal integration
                let mut area: f32 = 0.0;
                for i in 1..peakel_peaks.len() {
                    let (_, prev_int, prev_rt, _, _) = peakel_peaks[i - 1];
                    let (_, cur_int, cur_rt, _, _) = peakel_peaks[i];
                    let delta_time = cur_rt - prev_rt;
                    area += (prev_int + cur_int) * delta_time / 2.0;
                }
                if area == 0.0 {
                    area = peakel_peaks.iter().map(|(_, i, _, _, _)| *i).sum();
                }
                
                // Calculate duration
                let first_rt = peakel_peaks.first().unwrap().2;
                let last_rt = peakel_peaks.last().unwrap().2;
                let duration = last_rt - first_rt;
                
                // Calculate amplitude
                let min_int = peakel_peaks.iter()
                    .map(|(_, i, _, _, _)| *i)
                    .filter(|i| *i > 0.0)
                    .fold(f32::INFINITY, f32::min);
                let amplitude = if min_int > 0.0 && min_int < f32::INFINITY {
                    apex_peak.1 / min_int
                } else {
                    1.0
                };
                
                // Get spectrum IDs
                let first_spectrum_id = indexed_spectra[peakel_peaks.first().unwrap().3].spectrum_id;
                let apex_spectrum_id = indexed_spectra[apex_peak.3].spectrum_id;
                let last_spectrum_id = indexed_spectra[peakel_peaks.last().unwrap().3].spectrum_id;
                
                // Calculate gap count
                let first_spec_idx = peakel_peaks.first().unwrap().3;
                let last_spec_idx = peakel_peaks.last().unwrap().3;
                let total_in_range = last_spec_idx - first_spec_idx + 1;
                let gap_count = total_in_range.saturating_sub(peakel_peaks.len());
                
                // Collect raw peaks data for messagepack serialization
                let peaks_data = PeaksData {
                    spectrum_ids: peakel_peaks.iter()
                        .map(|(_, _, _, spec_idx, _)| indexed_spectra[*spec_idx].spectrum_id)
                        .collect(),
                    elution_times: peakel_peaks.iter()
                        .map(|(_, _, rt, _, _)| *rt)
                        .collect(),
                    mz_values: peakel_peaks.iter()
                        .map(|(mz, _, _, _, _)| *mz)
                        .collect(),
                    intensity_values: peakel_peaks.iter()
                        .map(|(_, int, _, _, _)| *int)
                        .collect(),
                };
                
                // Mark all peaks as used
                for (_, _, _, spec_idx, peak_idx) in peakel_peaks {
                    used_peaks[*spec_idx].insert(*peak_idx);
                }
                
                detected_peakels.push(DiaMs2PeakelRecord {
                    id: peakel_id,
                    mz: weighted_mz,
                    elution_time: apex_peak.2,
                    duration,
                    apex_intensity: apex_peak.1,
                    area,
                    amplitude,
                    gap_count,
                    peaks_count: peaks_data.spectrum_ids.len(),
                    first_spectrum_id,
                    apex_spectrum_id,
                    last_spectrum_id,
                    isolation_window_id: window.id,
                    precursor_mz: window.target_mz,
                    peaks: peaks_data,
                });
                
                peakel_id += 1;
            }
        }
        
        detected_peakels
    }

    /// Detect all MS2 peakels across all isolation windows
    ///
    /// This processes each isolation window sequentially to save memory.
    pub fn detect_all_peakels(
        &self,
        reader: &MzDbReader,
    ) -> anyhow_ext::Result<(Vec<IsolationWindow>, Vec<DiaMs2PeakelRecord>)> {
        self.detect_all_peakels_with_threads(reader, 1)
    }

    /// Detect all MS2 peakels across all isolation windows with configurable parallelism
    ///
    /// When num_threads > 1 and processing-parallel feature is enabled, processes
    /// isolation windows in parallel using a producer-consumer pattern.
    pub fn detect_all_peakels_with_threads(
        &self,
        reader: &MzDbReader,
        num_threads: usize,
    ) -> anyhow_ext::Result<(Vec<IsolationWindow>, Vec<DiaMs2PeakelRecord>)> {
        // Discover isolation windows
        let windows = self.discover_isolation_windows(reader);
        
        log::info!("Found {} isolation windows", windows.len());
        
        let all_peakels = if num_threads > 1 {
            #[cfg(feature = "processing-parallel")]
            {
                self.detect_peakels_parallel(reader, &windows, num_threads)?
            }
            #[cfg(not(feature = "processing-parallel"))]
            {
                self.detect_peakels_sequential(reader, &windows)?
            }
        } else {
            self.detect_peakels_sequential(reader, &windows)?
        };
        
        log::info!("Total MS2 peakels detected: {}", all_peakels.len());
        
        Ok((windows, all_peakels))
    }

    /// Sequential processing of isolation windows
    fn detect_peakels_sequential(
        &self,
        reader: &MzDbReader,
        windows: &[IsolationWindow],
    ) -> anyhow_ext::Result<Vec<DiaMs2PeakelRecord>> {
        let mut all_peakels: Vec<DiaMs2PeakelRecord> = Vec::new();
        let mut next_id = 1i64;
        
        // Process each window
        for window in windows {
            let mut window_peakels = self.detect_peakels_for_window(reader, window)?;
            
            // Renumber peakel IDs to be globally unique
            for peakel in &mut window_peakels {
                peakel.id = next_id;
                next_id += 1;
            }
            
            all_peakels.extend(window_peakels);
        }
        
        Ok(all_peakels)
    }

    /// Parallel processing of isolation windows using producer-consumer pattern
    /// 
    /// Strategy: Use a bounded producer-consumer queue pattern.
    /// Producer loads spectra using efficient SQL filtering (by main_precursor_mz),
    /// consumers process peakel detection in parallel.
    /// Memory is bounded by queue size (num_threads * 2).
    #[cfg(feature = "processing-parallel")]
    fn detect_peakels_parallel(
        &self,
        reader: &MzDbReader,
        windows: &[IsolationWindow],
        num_threads: usize,
    ) -> anyhow_ext::Result<Vec<DiaMs2PeakelRecord>> {
        use crossbeam_channel::bounded;
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::sync::Mutex;
        use std::time::Instant;
        
        let total_windows = windows.len();
        let queue_size = num_threads * 2;
        
        log::info!("Processing {} isolation windows with {} consumer threads (queue size: {})", 
                   total_windows, num_threads, queue_size);
        
        // Create bounded channel - limits memory usage
        type WorkItem = (IsolationWindow, Vec<crate::model::Spectrum>);
        let (tx, rx) = bounded::<WorkItem>(queue_size);
        
        // Shared results collector
        let results: Mutex<Vec<Vec<DiaMs2PeakelRecord>>> = Mutex::new(Vec::new());
        let windows_processed = AtomicUsize::new(0);
        let windows_loaded = AtomicUsize::new(0);
        
        let start_time = Instant::now();
        
        // Use std::thread::scope for scoped threads
        std::thread::scope(|scope| {
            // Spawn consumer threads
            for thread_id in 0..num_threads {
                let rx = rx.clone();
                let results = &results;
                let windows_processed = &windows_processed;
                
                scope.spawn(move || {
                    let mut thread_peakels: Vec<Vec<DiaMs2PeakelRecord>> = Vec::new();
                    let mut items_processed = 0usize;
                    
                    log::debug!("Consumer thread {} started", thread_id);
                    
                    // Receive work items until channel is closed
                    while let Ok((window, spectra)) = rx.recv() {
                        let process_start = Instant::now();
                        
                        let peakels = self.detect_peakels_from_spectra(&window, &spectra);
                        thread_peakels.push(peakels);
                        items_processed += 1;
                        
                        let count = windows_processed.fetch_add(1, Ordering::Relaxed) + 1;
                        let process_time = process_start.elapsed();
                        
                        log::debug!("Thread {} processed window {:.1} m/z ({} spectra) in {:?} [{}/{}]", 
                                   thread_id, window.target_mz, spectra.len(), process_time, count, total_windows);
                    }
                    
                    log::debug!("Consumer thread {} finished, processed {} items", thread_id, items_processed);
                    
                    // Collect results
                    if let Ok(mut guard) = results.lock() {
                        guard.extend(thread_peakels);
                    }
                });
            }
            
            // Drop extra receiver clone so consumers can exit when producer is done
            drop(rx);
            
            // Producer: load spectra using efficient SQL filtering and send to queue
            log::info!("Producer starting to load spectra (using efficient SQL filtering)...");
            for window in windows {
                let load_start = Instant::now();
                
                // Use efficient method that filters by main_precursor_mz in SQL
                let spectra = reader.get_dia_spectra_for_window(window.target_mz)
                    .unwrap_or_default();
                
                let load_time = load_start.elapsed();
                let loaded = windows_loaded.fetch_add(1, Ordering::Relaxed) + 1;
                
                log::debug!("Producer loaded window {}/{}: {:.1} m/z ({} spectra) in {:?}", 
                           loaded, total_windows, window.target_mz, spectra.len(), load_time);
                
                // This will block if queue is full (bounded backpressure)
                if tx.send((window.clone(), spectra)).is_err() {
                    log::error!("Failed to send work item to queue");
                    break;
                }
            }
            
            log::info!("Producer finished loading all {} windows", total_windows);
            
            // Drop sender to signal consumers that no more work is coming
            drop(tx);
            
            // Threads are automatically joined when scope exits
        });
        
        let total_time = start_time.elapsed();
        log::info!("All processing completed in {:?}", total_time);
        
        // Extract results and renumber IDs
        let collected_results = results.into_inner()
            .map_err(|e| anyhow!("Failed to collect results: {:?}", e))?;
        
        let mut all_peakels: Vec<DiaMs2PeakelRecord> = Vec::new();
        let mut next_id = 1i64;
        
        for window_peakels in collected_results {
            for mut peakel in window_peakels {
                peakel.id = next_id;
                next_id += 1;
                all_peakels.push(peakel);
            }
        }
        
        Ok(all_peakels)
    }

    /// Detect peakels from preloaded spectra (for parallel processing)
    #[cfg(feature = "processing-parallel")]
    fn detect_peakels_from_spectra(
        &self,
        window: &IsolationWindow,
        spectra: &[crate::model::Spectrum],
    ) -> Vec<DiaMs2PeakelRecord> {
        if spectra.is_empty() {
            return Vec::new();
        }
        
        // Build indexed spectra for fast m/z lookup
        let indexed_spectra = self.build_indexed_spectra(spectra);
        
        // Run the shared walking algorithm
        self.run_walking_algorithm(indexed_spectra, window)
    }
}

impl Default for DiaMs2PeakelDetector {
    fn default() -> Self {
        Self::new()
    }
}

/// Write DIA MS2 peakels to a SQLite database (modified peakelDB format)
///
/// The schema includes an additional isolation_window table and
/// peakel table with isolation_window_id foreign key.
///
/// Note: This is a convenience wrapper around `Ms2PeakelDbWriter`.
/// For more control, use `Ms2PeakelDbWriter` directly.
pub fn write_dia_peakeldb(
    path: &PathBuf,
    mzdb_filename: &str,
    windows: &[IsolationWindow],
    peakels: &[DiaMs2PeakelRecord],
) -> anyhow_ext::Result<()> {
    use crate::processing::peakeldb::Ms2PeakelDbWriter;
    
    let writer = Ms2PeakelDbWriter::create(path)?;
    writer.write_peakels(mzdb_filename, windows, peakels)?;
    
    log::info!("DIA MS2 peakelDB created with {} isolation windows and {} peakels",
               windows.len(), peakels.len());
    
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_isolation_window() {
        let window = IsolationWindow {
            id: 1,
            target_mz: 500.0,
            lower_mz: 475.0,
            upper_mz: 525.0,
            spectrum_count: 100,
        };
        
        assert_eq!(window.id, 1);
        assert_eq!(window.target_mz, 500.0);
        assert_eq!(window.upper_mz - window.lower_mz, 50.0);
    }

    #[test]
    fn test_config_defaults() {
        let config = DiaMs2PeakelConfig::default();
        
        assert_eq!(config.mz_tol_ppm, 10.0);
        assert_eq!(config.min_intensity, 100.0);
        assert_eq!(config.min_peaks, 5);
        assert_eq!(config.algorithm, "smart");
    }

    #[test]
    fn test_peaks_data() {
        let mut peaks = PeaksData::new();
        assert!(peaks.is_empty());
        
        peaks.spectrum_ids.push(1);
        peaks.elution_times.push(100.0);
        peaks.mz_values.push(500.0);
        peaks.intensity_values.push(1000.0);
        
        assert_eq!(peaks.len(), 1);
        assert!(!peaks.is_empty());
    }
}
