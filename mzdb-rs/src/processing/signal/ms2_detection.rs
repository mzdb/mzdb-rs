//! MS2 DIA Peakel Detection using Walking Algorithm
//!
//! This module provides MS2-level peakel detection for DIA (Data Independent Acquisition) data.
//! It processes each isolation window individually to save memory, using the walking algorithm.
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
//!
//! # Example
//!
//! ```no_run
//! use mzdb::MzDbReader;
//! use mzdb::processing::signal::ms2_detection::{DiaMs2PeakelDetector, DiaMs2PeakelConfig};
//!
//! let reader = MzDbReader::open("dia_file.mzDB").unwrap();
//! let detector = DiaMs2PeakelDetector::new(&reader);
//! let windows = detector.isolation_windows();
//! let peakels = detector.detect_all_peakels(&reader).unwrap();
//! println!("Detected {} peakels across {} windows", peakels.len(), windows.len());
//! ```

use std::collections::BTreeMap;
use anyhow_ext::*;

#[cfg(feature = "processing-parallel")]
use std::sync::Mutex;

use crate::MzDbReader;
use crate::metadata::parse_isolation_window_offsets_from_xml;
use super::detection::{
    PeakelBatch, PeakelDetectionConfig, SpectrumPeakLookup, SortedPeaksProvider, PeakelDetector,
};

// ============================================================================
// Isolation Window
// ============================================================================

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

// ============================================================================
// DIA MS2 Peakel Record
// ============================================================================

use crate::processing::{Peakel, HasPeakelData};

/// Peakel record with isolation window mapping for DIA.
/// 
/// This struct wraps a `Peakel` and adds MS2-specific metadata
/// (isolation window ID and precursor m/z). Implements `HasPeakelData`
/// to provide direct access to the underlying peak arrays.
#[derive(Clone, Debug)]
pub struct DiaMs2PeakelRecord {
    /// The underlying peakel data
    pub data: Peakel,
    /// Isolation window ID (foreign key to isolation_window table)
    pub isolation_window_id: i64,
    /// Precursor m/z (isolation window target)
    pub precursor_mz: f64,
}

impl DiaMs2PeakelRecord {
    /// Create a new DIA MS2 peakel record from a Peakel and isolation window info
    pub fn new(peakel: Peakel, isolation_window_id: i64, precursor_mz: f64) -> Self {
        Self {
            data: peakel,
            isolation_window_id,
            precursor_mz,
        }
    }
    
    /// Get the peakel ID
    #[inline]
    pub fn id(&self) -> i64 {
        self.data.id
    }
    
    /// Get the fragment m/z at apex
    #[inline]
    pub fn mz(&self) -> f32 {
        self.data.apex_mz().unwrap_or(f32::NAN)
    }
    
    /// Get the elution time at apex
    #[inline]
    pub fn elution_time(&self) -> f32 {
        self.data.apex_elution_time().unwrap_or(0.0)
    }
    
    /// Get the duration
    #[inline]
    pub fn duration(&self) -> f32 {
        self.data.calc_duration()
    }
    
    /// Get the apex intensity
    #[inline]
    pub fn apex_intensity(&self) -> f32 {
        self.data.apex_intensity().unwrap_or(0.0)
    }
    
    /// Get the area
    #[inline]
    pub fn area(&self) -> f32 {
        self.data.calc_area()
    }
    
    /// Get the amplitude (apex/min ratio)
    #[inline]
    pub fn amplitude(&self) -> f32 {
        self.data.calc_amplitude()
    }
    
    /// Get the gap count
    #[inline]
    pub fn gap_count(&self) -> usize {
        self.data.gap_count
    }
    
    /// Get the peaks count
    #[inline]
    pub fn peaks_count(&self) -> usize {
        self.data.peaks_count()
    }
    
    /// Get the first spectrum ID
    #[inline]
    pub fn first_spectrum_id(&self) -> i64 {
        self.data.first_spectrum_id().unwrap_or(0)
    }
    
    /// Get the apex spectrum ID
    #[inline]
    pub fn apex_spectrum_id(&self) -> i64 {
        self.data.apex_spectrum_id().unwrap_or(0)
    }
    
    /// Get the last spectrum ID
    #[inline]
    pub fn last_spectrum_id(&self) -> i64 {
        self.data.last_spectrum_id().unwrap_or(0)
    }
}

impl HasPeakelData for DiaMs2PeakelRecord {
    fn spectrum_ids(&self) -> &[i64] {
        self.data.spectrum_ids()
    }
    
    fn elution_times(&self) -> &[f32] {
        self.data.elution_times()
    }
    
    fn mz_values(&self) -> &[f32] {
        self.data.mz_values()
    }
    
    fn intensity_values(&self) -> &[f32] {
        self.data.intensity_values()
    }

    fn apex_index(&self) -> Option<usize> {
        self.data.apex_index()
    }
}

impl std::ops::Deref for DiaMs2PeakelRecord {
    type Target = Peakel;
    
    fn deref(&self) -> &Self::Target {
        &self.data
    }
}

// ============================================================================
// Indexed Spectrum for Fast m/z Lookup
// ============================================================================

/// Optimized spectrum data for fast m/z range queries using binary search.
/// 
/// Uses separate vectors for m/z and intensity values (consistent with MS1).
/// The `peak_indices` vector maps back to original peak positions in the source spectrum.
pub struct IndexedMs2Spectrum {
    #[allow(dead_code)]
    spectrum_idx: usize,
    spectrum_id: i64,
    time: f32,
    /// m/z values sorted by m/z (for binary search) - 32-bit for centroid data
    mz_values: Vec<f32>,
    /// Intensity values (parallel to mz_values)
    intensity_values: Vec<f32>,
    /// Original peak indices in source spectrum (parallel to mz_values)
    peak_indices: Vec<usize>,
}

impl IndexedMs2Spectrum {
    /// Find the nearest peak within m/z tolerance using binary search.
    /// Returns (mz, intensity, original_peak_idx) if found.
    fn find_nearest_peak_internal(&self, target_mz: f32, mz_tol_da: f32) -> Option<(f32, f32, usize)> {
        if self.mz_values.is_empty() {
            return None;
        }
        
        let lower = target_mz - mz_tol_da;
        let upper = target_mz + mz_tol_da;
        
        // Binary search for starting position
        let start_idx = self.mz_values.partition_point(|&mz| mz < lower);
        
        if start_idx >= self.mz_values.len() {
            return None;
        }
        
        let mut best: Option<(f32, f32, usize)> = None;
        let mut min_diff = mz_tol_da;
        
        for idx in start_idx..self.mz_values.len() {
            let mz = self.mz_values[idx];
            if mz > upper {
                break;
            }
            
            let diff = (mz - target_mz).abs();
            if diff < min_diff {
                min_diff = diff;
                best = Some((mz, self.intensity_values[idx], self.peak_indices[idx]));
            }
        }
        
        best
    }
}

// ============================================================================
// Configuration
// ============================================================================

/// Configuration for DIA MS2 peakel detection
#[derive(Clone, Debug)]
pub struct DiaMs2PeakelConfig {
    /// m/z tolerance in PPM for XIC extraction
    pub mz_tol_ppm: f32,
    /// Minimum intensity threshold for peak detection (used during spectrum loading)
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
    /// Peaks below this percentile threshold will be skipped during walking
    pub intensity_percentile: f32,
    /// Minimum peakel amplitude (apex/min intensity ratio)
    pub min_peakel_amplitude: f32,
    /// Minimum peakel duration in seconds
    pub min_peakel_duration: f32,
    /// Algorithm to use: "basic" or "smart"
    pub algorithm: String,
    /// Whether to skip the apex boundary check (apex must not be first or last peak).
    /// Default is true to match Scala reference implementation behavior.
    pub skip_apex_boundary_check: bool,
}

impl Default for DiaMs2PeakelConfig {
    fn default() -> Self {
        Self {
            mz_tol_ppm: 10.0,
            min_intensity: 100.0,
            min_peaks: 5,
            max_consecutive_gaps: 3,
            max_total_gaps: usize::MAX,
            max_time_window: 1200.0,
            intensity_percentile: 0.9,
            min_peakel_amplitude: 1.5,
            min_peakel_duration: 0.0,
            algorithm: "smart".to_string(),
            skip_apex_boundary_check: true,
        }
    }
}

// ============================================================================
// Trait Implementations for Generic Detection Algorithm
// ============================================================================

impl PeakelDetectionConfig for DiaMs2PeakelConfig {
    #[inline] fn mz_tol_ppm(&self) -> f32 { self.mz_tol_ppm }
    #[inline] fn min_intensity(&self) -> f32 { self.min_intensity }
    #[inline] fn min_peaks(&self) -> usize { self.min_peaks }
    #[inline] fn max_consecutive_gaps(&self) -> usize { self.max_consecutive_gaps }
    #[inline] fn max_total_gaps(&self) -> usize { self.max_total_gaps }
    #[inline] fn max_time_window(&self) -> f32 { self.max_time_window }
    #[inline] fn intensity_percentile(&self) -> f32 { self.intensity_percentile }
    #[inline] fn min_peakel_amplitude(&self) -> f32 { self.min_peakel_amplitude }
    #[inline] fn min_peakel_duration(&self) -> f32 { self.min_peakel_duration }
    #[inline] fn algorithm(&self) -> &str { &self.algorithm }
    #[inline] fn skip_apex_boundary_check(&self) -> bool { self.skip_apex_boundary_check }
}

/// Peak key for MS2 detection - simpler than MS1, no triplet
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct Ms2PeakKey {
    pub spectrum_idx: usize,
    pub peak_idx: usize,
}

impl Ms2PeakKey {
    #[inline]
    pub fn new(spectrum_idx: usize, peak_idx: usize) -> Self {
        Self { spectrum_idx, peak_idx }
    }
}

impl SpectrumPeakLookup for IndexedMs2Spectrum {
    type PeakKey = Ms2PeakKey;
    
    fn find_nearest_peak(&self, target_mz: f32, mz_tol_da: f32, spectrum_idx: usize) -> Option<(f32, f32, Self::PeakKey)> {
        self.find_nearest_peak_internal(target_mz, mz_tol_da)
            .map(|(mz, intensity, peak_idx)| {
                (mz, intensity, Ms2PeakKey::new(spectrum_idx, peak_idx))
            })
    }
    
    fn spectrum_id(&self) -> i64 {
        self.spectrum_id
    }
    
    fn time(&self) -> f32 {
        self.time
    }
}

/// Peak data for an isolation window, ready for peakel detection.
/// 
/// This struct wraps the indexed spectra and sorted peak indices
/// to implement `SortedPeaksProvider` for the generic detection algorithm.
pub struct IsolationWindowPeakData {
    /// Indexed spectra sorted by time
    spectra: Vec<IndexedMs2Spectrum>,
    /// All peaks: (mz, intensity, spectrum_idx, peak_idx)
    all_peaks: Vec<(f32, f32, usize, usize)>,
    /// Indices sorted by descending intensity
    sorted_indices: Vec<usize>,
}

impl IsolationWindowPeakData {
    /// Create peak data from indexed spectra
    pub fn new(spectra: Vec<IndexedMs2Spectrum>) -> Self {
        // Collect all peaks from separate vectors
        let mut all_peaks: Vec<(f32, f32, usize, usize)> = Vec::new();
        for (spectrum_idx, spectrum) in spectra.iter().enumerate() {
            for (i, (&mz, &intensity)) in spectrum.mz_values.iter()
                .zip(spectrum.intensity_values.iter())
                .enumerate() 
            {
                let peak_idx = spectrum.peak_indices[i];
                all_peaks.push((mz, intensity, spectrum_idx, peak_idx));
            }
        }
        
        // Sort indices by descending intensity
        let mut sorted_indices: Vec<usize> = (0..all_peaks.len()).collect();
        sorted_indices.sort_by(|&a, &b| {
            all_peaks[b].1.total_cmp(&all_peaks[a].1)
        });
        
        Self {
            spectra,
            all_peaks,
            sorted_indices,
        }
    }
}

impl SortedPeaksProvider for IsolationWindowPeakData {
    type PeakKey = Ms2PeakKey;
    type SpectrumLookup = IndexedMs2Spectrum;
    
    fn sorted_peaks_iter(&self) -> impl Iterator<Item = (f32, f32, usize, Self::PeakKey)> {
        self.sorted_indices.iter().map(move |&idx| {
            let (mz, intensity, spectrum_idx, peak_idx) = self.all_peaks[idx];
            let peak_key = Ms2PeakKey::new(spectrum_idx, peak_idx);
            (mz, intensity, spectrum_idx, peak_key)
        })
    }
    
    fn get_spectrum_lookup(&self, idx: usize) -> &Self::SpectrumLookup {
        &self.spectra[idx]
    }
    
    fn spectra_count(&self) -> usize {
        self.spectra.len()
    }
    
    fn is_apex_in_valid_mz_range(&self, _apex_mz: f32) -> bool {
        // MS2: no m/z range filtering (all peaks in isolation window are valid)
        true
    }
    
    fn calc_intensity_threshold(&self, detector_config: &impl PeakelDetectionConfig) -> f32 {
        if self.sorted_indices.len() > 10 {
            let pos = (self.sorted_indices.len() as f32 * detector_config.intensity_percentile()) as usize;
            let pos = pos.min(self.sorted_indices.len() - 1);
            self.all_peaks[self.sorted_indices[pos]].1
        } else {
            0.0
        }
    }
}

// ============================================================================
// DIA MS2 Peakel Detector
// ============================================================================

/// DIA MS2 Peakel Detector
///
/// Processes DIA data by iterating over each isolation window,
/// detecting peakels in the MS2 spectra for that window.
pub struct DiaMs2PeakelDetector {
    config: DiaMs2PeakelConfig,
    isolation_windows: Vec<IsolationWindow>,
}

impl PeakelDetector for DiaMs2PeakelDetector {
    type Config = DiaMs2PeakelConfig;
    type PeakData = IsolationWindowPeakData;
    
    fn config(&self) -> &Self::Config {
        &self.config
    }
}

impl DiaMs2PeakelDetector {
    /// Create a new detector with default configuration
    ///
    /// Discovers isolation windows from the mzDB file at construction time.
    pub fn new(reader: &MzDbReader) -> Self {
        let config = DiaMs2PeakelConfig::default();
        let isolation_windows = Self::discover_isolation_windows(reader);
        log::info!("Found {} isolation windows", isolation_windows.len());
        Self { config, isolation_windows }
    }

    /// Create with custom configuration
    ///
    /// Discovers isolation windows from the mzDB file at construction time.
    pub fn with_config(config: DiaMs2PeakelConfig, reader: &MzDbReader) -> Self {
        log::info!("DiaMs2PeakelDetector config: min_peaks={}, mz_tol={} ppm, max_gaps={}",
                   config.min_peaks, config.mz_tol_ppm, config.max_consecutive_gaps);
        let isolation_windows = Self::discover_isolation_windows(reader);
        log::info!("Found {} isolation windows", isolation_windows.len());
        Self { config, isolation_windows }
    }

    /// Get the isolation windows discovered from the mzDB file
    pub fn isolation_windows(&self) -> &[IsolationWindow] {
        &self.isolation_windows
    }

    /// Discover all isolation windows in the mzDB file
    ///
    /// This method parses the actual isolation window bounds from the precursor_list XML
    /// rather than assuming a fixed window width.
    fn discover_isolation_windows(reader: &MzDbReader) -> Vec<IsolationWindow> {
        let headers = reader.get_spectrum_headers();

        // Group MS2 spectra by precursor m/z and track actual window bounds
        // Key: window_key (target_mz rounded to 0.1)
        // Value: (target_mz, lower_offset, upper_offset, count)
        let mut window_data: BTreeMap<i64, (f64, Option<f64>, Option<f64>, usize)> = BTreeMap::new();

        for header in headers {
            if header.ms_level == 2 {
                if let Some(precursor_mz) = header.precursor_mz {
                    // Round to 0.1 m/z for grouping
                    let window_key = (precursor_mz * 10.0).round() as i64;

                    // Try to parse window offsets from precursor_list XML
                    let (lower_offset, upper_offset) = header.precursor_list_str
                        .as_ref()
                        .map(|xml| parse_isolation_window_offsets_from_xml(xml))
                        .unwrap_or((None, None));

                    let entry = window_data.entry(window_key).or_insert((precursor_mz, None, None, 0));
                    entry.3 += 1;

                    // Update offsets if we found them and don't have them yet
                    if entry.1.is_none() && lower_offset.is_some() {
                        entry.1 = lower_offset;
                    }
                    if entry.2.is_none() && upper_offset.is_some() {
                        entry.2 = upper_offset;
                    }
                }
            }
        }

        // Convert to IsolationWindow structs
        window_data.into_iter()
            .enumerate()
            .map(|(idx, (_key, (target_mz, lower_offset, upper_offset, count)))| {
                // Use parsed offsets, or fall back to a conservative default
                let half_width_lower = lower_offset.unwrap_or_else(|| {
                    log::warn!("No isolation window offset found for m/z {:.1}, using default 4.0 Da", target_mz);
                    4.0
                });
                let half_width_upper = upper_offset.unwrap_or_else(|| {
                    log::warn!("No isolation window offset found for m/z {:.1}, using default 4.0 Da", target_mz);
                    4.0
                });

                IsolationWindow {
                    id: (idx + 1) as i64,
                    target_mz,
                    lower_mz: target_mz - half_width_lower,
                    upper_mz: target_mz + half_width_upper,
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
    ) -> Result<Vec<DiaMs2PeakelRecord>> {
        log::info!("Processing isolation window: {:.1} m/z ({} spectra)",
                   window.target_mz, window.spectrum_count);

        // Get MS2 spectra for this isolation window using efficient SQL filtering
        // Use tolerance based on window width to capture all spectra
        let tolerance = (window.upper_mz - window.lower_mz) / 2.0;
        let spectra = reader.get_dia_spectra_for_window(window.target_mz, Some(tolerance))?;

        if spectra.is_empty() {
            return Ok(Vec::new());
        }

        // Build indexed spectra for fast m/z lookup
        let indexed_spectra = self.build_indexed_spectra(&spectra);

        // Run the walking algorithm
        let detected_peakels = self.run_walking_algorithm_for_window(indexed_spectra, window);

        log::info!("  Detected {} MS2 peakels in window {:.1}",
                   detected_peakels.len(), window.target_mz);

        Ok(detected_peakels)
    }

    /// Build indexed spectra from raw spectra for fast m/z lookup
    fn build_indexed_spectra(&self, spectra: &[crate::model::Spectrum]) -> Vec<IndexedMs2Spectrum> {
        let mut indexed_spectra: Vec<IndexedMs2Spectrum> = Vec::with_capacity(spectra.len());

        for (idx, spectrum) in spectra.iter().enumerate() {
            // Collect peaks that pass intensity threshold into separate vectors
            let mut mz_values: Vec<f32> = Vec::new();
            let mut intensity_values: Vec<f32> = Vec::new();
            let mut peak_indices: Vec<usize> = Vec::new();
            
            for (peak_idx, (&mz, &intensity)) in spectrum.data.mz_array.iter()
                .zip(spectrum.data.intensity_array.iter())
                .enumerate()
            {
                if intensity >= self.config.min_intensity {
                    mz_values.push(mz);
                    intensity_values.push(intensity);
                    peak_indices.push(peak_idx);
                }
            }

            indexed_spectra.push(IndexedMs2Spectrum {
                spectrum_idx: idx,
                spectrum_id: spectrum.header.id,
                time: spectrum.header.time,
                mz_values,
                intensity_values,
                peak_indices,
            });
        }

        indexed_spectra
    }

    /// Core walking algorithm for peakel detection
    ///
    /// This method uses the generic `PeakelDetector::run_walking_algorithm` 
    /// implementation and wraps the results with isolation window metadata.
    fn run_walking_algorithm_for_window(
        &self,
        mut indexed_spectra: Vec<IndexedMs2Spectrum>,
        window: &IsolationWindow,
    ) -> Vec<DiaMs2PeakelRecord> {
        if indexed_spectra.is_empty() {
            return Vec::new();
        }

        // Sort spectra by time for proper walking
        indexed_spectra.sort_by(|a, b| a.time.total_cmp(&b.time));

        // Create peak data for the generic algorithm
        let peak_data = IsolationWindowPeakData::new(indexed_spectra);
        
        // Use the trait's default implementation
        let peakels = self.run_walking_algorithm(&peak_data);
        
        // Wrap peakels with isolation window metadata
        peakels.into_iter()
            .map(|peakel| DiaMs2PeakelRecord::new(peakel, window.id, window.target_mz))
            .collect()
    }


    /// Detect all MS2 peakels across all isolation windows
    ///
    /// This processes each isolation window sequentially to save memory.
    pub fn detect_all_peakels(
        &self,
        reader: &MzDbReader,
    ) -> Result<Vec<DiaMs2PeakelRecord>> {
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
    ) -> Result<Vec<DiaMs2PeakelRecord>> {
        let mut all_peakels = Vec::new();

        self.detect_peakels_in_batches_with_threads(reader, num_threads, |batch| {
            all_peakels.extend(batch.peakels);
            Ok(())
        })?;

        log::info!("Total MS2 peakels detected: {}", all_peakels.len());

        Ok(all_peakels)
    }

    /// Detect peakels in batches (one batch per isolation window), single-threaded.
    ///
    /// The callback receives a `PeakelBatch<DiaMs2PeakelRecord>` for each window,
    /// allowing streaming writes without accumulating all peakels in memory.
    pub fn detect_peakels_in_batches(
        &self,
        reader: &MzDbReader,
        on_batch: impl FnMut(PeakelBatch<DiaMs2PeakelRecord>) -> Result<()>,
    ) -> Result<()> {
        self.detect_peakels_in_batches_with_threads(reader, 1, on_batch)
    }

    /// Detect peakels in batches with configurable parallelism.
    ///
    /// The callback receives a `PeakelBatch<DiaMs2PeakelRecord>` for each isolation window.
    pub fn detect_peakels_in_batches_with_threads(
        &self,
        reader: &MzDbReader,
        num_threads: usize,
        on_batch: impl FnMut(PeakelBatch<DiaMs2PeakelRecord>) -> Result<()>,
    ) -> Result<()> {
        let windows = &self.isolation_windows;

        if num_threads > 1 {
            #[cfg(feature = "processing-parallel")]
            {
                let db_path = reader.connection().path()
                    .ok_or_else(|| anyhow!("Cannot get database path"))?
                    .to_string();
                self.detect_parallel_in_batches(&db_path, windows, num_threads, on_batch)?;
            }
            #[cfg(not(feature = "processing-parallel"))]
            {
                log::warn!("Parallel processing not enabled, using sequential");
                self.detect_sequential_in_batches(reader, windows, on_batch)?;
            }
        } else {
            self.detect_sequential_in_batches(reader, windows, on_batch)?;
        }

        Ok(())
    }

    /// Sequential processing of isolation windows, emitting batches via callback
    fn detect_sequential_in_batches(
        &self,
        reader: &MzDbReader,
        windows: &[IsolationWindow],
        mut on_batch: impl FnMut(PeakelBatch<DiaMs2PeakelRecord>) -> Result<()>,
    ) -> Result<()> {
        let total_batches = windows.len();

        for (batch_index, window) in windows.iter().enumerate() {
            let peakels = self.detect_peakels_for_window(reader, window)?;
            on_batch(PeakelBatch {
                peakels,
                batch_index,
                total_batches,
            })?;
        }

        Ok(())
    }

    /// Parallel processing of isolation windows using producer-consumer pattern
    /// with streaming ordered emission via BinaryHeap.
    ///
    /// Architecture:
    /// - Producer thread: loads spectra with its own DB connection, sends through bounded work channel
    /// - N consumer threads: run detection, send (window_index, peakels) through bounded results channel
    /// - Main thread: receives results, reorders via BinaryHeap, calls on_batch in window order
    ///
    /// This ensures on_batch is called progressively (not deferred to the end),
    /// allowing the writer to work in parallel with detection.
    #[cfg(feature = "processing-parallel")]
    fn detect_parallel_in_batches(
        &self,
        db_path: &str,
        windows: &[IsolationWindow],
        num_threads: usize,
        mut on_batch: impl FnMut(PeakelBatch<DiaMs2PeakelRecord>) -> Result<()>,
    ) -> Result<()> {
        use crossbeam_channel::bounded;
        use std::time::Instant;

        let total_windows = windows.len();
        let queue_size = num_threads * 2;

        log::info!("Processing {} isolation windows with {} consumer threads (queue size: {})",
                   total_windows, num_threads, queue_size);

        // Work channel: producer -> consumers (spectra to process)
        type WorkItem = (usize, IsolationWindow, Vec<crate::model::Spectrum>);
        let (work_tx, work_rx) = bounded::<WorkItem>(queue_size);

        // Results channel: consumers -> main thread (detected peakels with window index)
        let (result_tx, result_rx) = bounded::<(usize, Vec<DiaMs2PeakelRecord>)>(queue_size);

        // Producer error propagation
        let producer_error: Mutex<Option<anyhow_ext::Error>> = Mutex::new(None);
        let start_time = Instant::now();

        std::thread::scope(|scope| {
            // Spawn consumer threads
            for thread_id in 0..num_threads {
                let work_rx = work_rx.clone();
                let result_tx = result_tx.clone();

                scope.spawn(move || {
                    let mut items_processed = 0usize;
                    log::debug!("Consumer thread {} started", thread_id);

                    while let std::result::Result::Ok((window_idx, window, spectra)) = work_rx.recv() {
                        let peakels = self.detect_from_spectra(&window, &spectra);

                        log::debug!("Thread {} processed window {}/{}: {:.1} m/z ({} spectra, {} peakels)",
                                   thread_id, window_idx + 1, total_windows,
                                   window.target_mz, spectra.len(), peakels.len());

                        // Send result to main thread for ordered emission
                        // This blocks if main thread is slow (backpressure)
                        if result_tx.send((window_idx, peakels)).is_err() {
                            log::warn!("Result channel closed, stopping consumer {}", thread_id);
                            break;
                        }
                        items_processed += 1;
                    }

                    log::debug!("Consumer thread {} finished, processed {} items", thread_id, items_processed);
                    // result_tx clone is dropped here
                });
            }

            // Drop extra clones so channels close properly when all producers/consumers finish
            drop(work_rx);
            drop(result_tx);

            // Producer: runs on a scoped thread with its own DB connection
            let producer_error = &producer_error;
            scope.spawn(move || {
                let result: Result<()> = (|| {
                    let producer_reader = MzDbReader::builder(db_path)
                        .read_only()
                        .build()?;

                    log::info!("Producer starting to load spectra...");

                    for (idx, window) in windows.iter().enumerate() {
                        let tolerance = (window.upper_mz - window.lower_mz) / 2.0;
                        let spectra = producer_reader.get_dia_spectra_for_window(
                            window.target_mz, Some(tolerance)
                        )?;

                        log::debug!("Producer loaded window {}/{}: {:.1} m/z ({} spectra)",
                                   idx + 1, total_windows, window.target_mz, spectra.len());

                        if work_tx.send((idx, window.clone(), spectra)).is_err() {
                            log::warn!("Work channel closed early, stopping producer");
                            break;
                        }
                    }

                    log::debug!("Producer finished: {} windows", total_windows);
                    Ok(())
                })();

                if let Err(e) = result {
                    log::error!("Producer error: {:?}", e);
                    *producer_error.lock().expect("producer_error mutex poisoned") = Some(e);
                }
                // work_tx is dropped here, closing the work channel
            });

            // Main thread: receive results and emit batches as they arrive.
            // No ordering needed — the peakeldb writer is order-independent.
            let mut total_peakels = 0usize;
            let mut batches_emitted = 0usize;

            for (window_idx, peakels) in result_rx {
                total_peakels += peakels.len();

                if let Err(e) = on_batch(PeakelBatch {
                    peakels,
                    batch_index: window_idx,
                    total_batches: total_windows,
                }) {
                    log::error!("on_batch error at window {}: {:?}", window_idx, e);
                    break;
                }
                batches_emitted += 1;
            }

            log::info!("Parallel detection complete in {:?}: {} peakels in {} batches",
                       start_time.elapsed(), total_peakels, batches_emitted);

            // All threads are automatically joined when scope exits
        });

        // Check for producer errors
        if let Some(e) = producer_error.into_inner()
            .map_err(|e| anyhow!("Failed to check producer error: {:?}", e))?
        {
            return Err(e);
        }

        Ok(())
    }

    /// Detect peakels from preloaded spectra (for parallel processing)
    #[cfg(feature = "processing-parallel")]
    fn detect_from_spectra(
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
        self.run_walking_algorithm_for_window(indexed_spectra, window)
    }
}


// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::processing::Peakel;

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
        assert_eq!(config.intensity_percentile, 0.9);
        assert_eq!(config.algorithm, "smart");
    }

    #[test]
    fn test_peakel_creation() -> Result<()> {
        let peakel = Peakel::from_vectors(
            vec![1],
            vec![100.0],
            vec![500.0],
            vec![1000.0],
            None,
            None,
            0,
        )?;

        assert_eq!(peakel.peaks_count(), 1);
        assert_eq!(peakel.apex_intensity(), Some(1000.0));

        Ok(())
    }
}