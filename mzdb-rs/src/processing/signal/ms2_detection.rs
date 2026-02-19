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

use std::collections::HashSet;
use std::sync::Arc;
use anyhow_ext::*;

#[cfg(feature = "processing-parallel")]
use std::sync::Mutex;

use crate::MzDbReader;
use super::detection::{
    PeakelBatch, PeakelDetectionConfig, SpectrumPeakLookup, SortedPeaksProvider, PeakelDetector,
};

// ============================================================================
// Isolation Window
// ============================================================================

// IsolationWindow is defined in crate::model
pub use crate::model::IsolationWindow;

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
}

/// Globally unique identifier for an apex peak: (spectrum_id, peak_index_in_original_spectrum).
/// Result of running the walking algorithm for a single isolation window.
struct Ms2PeakelDetectionResult {
    /// Peakels whose apex is in the current window
    peakels: Vec<DiaMs2PeakelRecord>,
    /// Count of peakels discarded because their apex falls in a neighbor window's
    /// spectrum (these will be detected when that neighbor is processed as current)
    neighbor_discarded: usize,
}

impl DiaMs2PeakelRecord {
    /// Create a new DIA MS2 peakel record from a Peakel and isolation window info
    pub fn new(peakel: Peakel, isolation_window_id: i64) -> Self {
        Self {
            data: peakel,
            isolation_window_id,
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
///
/// Data vectors are Arc-wrapped for efficient sharing across staggered window pairs.
/// Cloning this struct copies scalar fields and bumps Arc refcounts (no data copy).
#[derive(Clone)]
pub struct IndexedMs2Spectrum {
    spectrum_id: i64,
    time: f32,
    /// m/z values sorted by m/z (for binary search) - 32-bit for centroid data
    mz_values: Arc<Vec<f32>>,
    /// Intensity values (parallel to mz_values)
    intensity_values: Arc<Vec<f32>>,
    /// Original peak indices in source spectrum (parallel to mz_values)
    peak_indices: Arc<Vec<usize>>,
    /// Source isolation window this spectrum belongs to
    source_window: Arc<IsolationWindow>,
}

impl IndexedMs2Spectrum {
    /// Get the spectrum time (RT)
    pub fn rt(&self) -> f32 { self.time }
    /// Get the spectrum ID
    pub fn id(&self) -> i64 { self.spectrum_id }
    /// Get the source isolation window this spectrum belongs to
    pub fn source_window(&self) -> &Arc<IsolationWindow> { &self.source_window }
    /// Get the m/z values
    pub fn mz_values(&self) -> &[f32] { &self.mz_values }
    /// Get the intensity values
    pub fn intensity_values(&self) -> &[f32] { &self.intensity_values }

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
///
/// # Gap tolerance and staggered DIA
///
/// For staggered DIA acquisitions, spectra from overlapping windows are interleaved
/// in the merged timeline. When walking an XIC for a fragment ion, spectra from
/// the neighbor window appear as gaps (they don't contain the same fragment peaks).
/// With N overlapping windows at the same RT, up to N-1 consecutive neighbor spectra
/// may separate two current-window spectra in the timeline.
///
/// `max_consecutive_gaps` must be set high enough to bridge these interleaved spectra:
/// - Non-staggered DIA: 1 gap is often sufficient
/// - Staggered DIA (2 overlapping windows): 3+ gaps recommended (default)
///
/// Setting this too low causes XICs to be truncated prematurely, losing signal.
/// Setting this too high may merge distinct elution events into one peakel.
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
    /// Whether to zero-pad the XIC before derivative analysis.
    /// Default is true for MS2 DIA data where fragment ions often appear as
    /// sharp spikes with the apex at the first position.
    pub zero_pad_xic: bool,
}

impl Default for DiaMs2PeakelConfig {
    fn default() -> Self {
        Self {
            mz_tol_ppm: 10.0,
            min_intensity: 0.0,
            min_peaks: 5,
            // Default of 3 bridges interleaved neighbor spectra in staggered DIA
            max_consecutive_gaps: 3,
            max_total_gaps: usize::MAX,
            max_time_window: 1200.0,
            intensity_percentile: 0.9,
            // Amplitude filter disabled for DIA MS2: with staggered interleaving,
            // fragments often have very few points per peak and naturally low amplitude ratios
            min_peakel_amplitude: 1.0,
            min_peakel_duration: 0.0,
            algorithm: "smart".to_string(),
            skip_apex_boundary_check: true,
            zero_pad_xic: true,
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
    #[inline] fn zero_pad_xic(&self) -> bool { self.zero_pad_xic }
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
///
/// When used with staggered DIA data, spectra from neighbor windows are included
/// for walk extension, but only peaks from the current (reference) window are
/// used as seeds. This mirrors the MS1 run slice triplet pattern.
pub struct IsolationWindowPeakData {
    /// All spectra sorted by time (current + neighbors merged)
    spectra: Vec<IndexedMs2Spectrum>,
    /// All peaks from CURRENT window only: (mz, intensity, spectrum_idx, peak_idx)
    /// spectrum_idx refers to position in the merged `spectra` vec
    all_peaks: Vec<(f32, f32, usize, usize)>,
    /// Indices into all_peaks sorted by descending intensity
    sorted_indices: Vec<usize>,
    /// Spectrum IDs belonging to the current (reference) window.
    /// Used to check if a peakel's true apex falls in the current window.
    current_window_spectrum_ids: HashSet<i64>,
}

impl IsolationWindowPeakData {
    /// Create peak data from a single window's spectra (no neighbors).
    ///
    /// All spectra are treated as current — all peaks are seeds.
    /// This preserves backward compatibility for non-staggered DIA data.
    pub fn new(spectra: &[IndexedMs2Spectrum]) -> Self {
        let current_window_spectrum_ids: HashSet<i64> = spectra.iter()
            .map(|s| s.spectrum_id)
            .collect();

        // Clone into owned vec (cheap: Arc refcount bumps only), sort by time
        let mut owned: Vec<IndexedMs2Spectrum> = spectra.to_vec();
        owned.sort_by(|a, b| a.time.total_cmp(&b.time));

        Self::build(owned, &current_window_spectrum_ids)
    }

    /// Create peak data from current window spectra + neighbor windows.
    ///
    /// Seeds (all_peaks/sorted_indices) are built only from current window spectra.
    /// Neighbor spectra are included in the merged spectra list so the walking
    /// algorithm can extend XICs into them, but they are never used as starting
    /// points — mirroring the MS1 run slice triplet pattern.
    ///
    /// IMPORTANT: Each window's spectra must be loaded with strict filtering
    /// (exact precursor_mz match) to avoid duplicate spectrum IDs across windows.
    pub fn new_with_neighbors(
        current_spectra: &[IndexedMs2Spectrum],
        left_neighbor: Option<&[IndexedMs2Spectrum]>,
        right_neighbor: Option<&[IndexedMs2Spectrum]>,
    ) -> Self {
        let current_window_spectrum_ids: HashSet<i64> = current_spectra.iter()
            .map(|s| s.spectrum_id)
            .collect();

        // Merge all spectra (struct fields like spectrum_id and time are copied,
        // but data vectors are Arc-wrapped so only refcounts are bumped)
        let total_len = current_spectra.len()
            + left_neighbor.map_or(0, |s| s.len())
            + right_neighbor.map_or(0, |s| s.len());

        let mut merged = Vec::with_capacity(total_len);
        if let Some(left) = left_neighbor {
            merged.extend_from_slice(left);
        }
        merged.extend_from_slice(current_spectra);
        if let Some(right) = right_neighbor {
            merged.extend_from_slice(right);
        }

        // Sort by time to interleave staggered cycles
        merged.sort_by(|a, b| a.time.total_cmp(&b.time));

        Self::build(merged, &current_window_spectrum_ids)
    }

    /// Common builder: index only current-window spectra as seeds.
    fn build(spectra: Vec<IndexedMs2Spectrum>, current_ids: &HashSet<i64>) -> Self {
        let mut all_peaks: Vec<(f32, f32, usize, usize)> = Vec::new();

        for (spectrum_idx, spectrum) in spectra.iter().enumerate() {
            // Only index peaks from the current window as seeds
            if !current_ids.contains(&spectrum.spectrum_id) {
                continue;
            }

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
            current_window_spectrum_ids: current_ids.clone(),
        }
    }

    /// Check if a spectrum ID belongs to the current (reference) window.
    #[inline]
    pub fn is_current_window_spectrum(&self, spectrum_id: i64) -> bool {
        self.current_window_spectrum_ids.contains(&spectrum_id)
    }

    /// Number of seed peaks (from current window only)
    pub fn all_peaks_count(&self) -> usize {
        self.all_peaks.len()
    }

    /// Iterate over all seed peaks: (mz, intensity, spectrum_idx, peak_idx)
    pub fn all_peaks_iter(&self) -> impl Iterator<Item = &(f32, f32, usize, usize)> {
        self.all_peaks.iter()
    }

    /// Compute intensity threshold using the given config
    pub fn intensity_threshold(&self, config: &impl PeakelDetectionConfig) -> f32 {
        self.calc_intensity_threshold(config)
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
///
/// For staggered DIA data, automatically detects overlapping windows
/// and uses a sliding triplet pattern (current + left/right neighbors)
/// to double the effective sampling rate for XIC extraction.
    pub struct DiaMs2PeakelDetector {
    config: DiaMs2PeakelConfig,
    /// Isolation windows as Arc for sharing with IndexedMs2Spectrum
    isolation_windows: Vec<Arc<IsolationWindow>>,
    /// Whether the DIA acquisition uses staggered (overlapping) windows
    is_staggered: bool,
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
        Self::with_config(config, reader)
    }

    /// Create with custom configuration
    ///
    /// Discovers isolation windows from the mzDB file at construction time.
    /// Automatically detects staggered DIA acquisition.
    pub fn with_config(config: DiaMs2PeakelConfig, reader: &MzDbReader) -> Self {
        log::info!("DiaMs2PeakelDetector config: min_peaks={}, mz_tol={} ppm, max_gaps={}",
                   config.min_peaks, config.mz_tol_ppm, config.max_consecutive_gaps);
        let raw_windows = reader.get_isolation_windows();
        let is_staggered = Self::detect_staggering(&raw_windows);
        let isolation_windows: Vec<Arc<IsolationWindow>> = raw_windows.into_iter()
            .map(Arc::new)
            .collect();
        log::info!("Found {} isolation windows (staggered={})",
                   isolation_windows.len(), is_staggered);
        Self { config, isolation_windows, is_staggered }
    }

    /// Get the isolation windows discovered from the mzDB file
    pub fn isolation_windows(&self) -> Vec<IsolationWindow> {
        self.isolation_windows.iter().map(|w| (**w).clone()).collect()
    }

    /// Whether staggered DIA was auto-detected
    pub fn is_staggered(&self) -> bool {
        self.is_staggered
    }

    /// Get a reference to the detection configuration
    pub fn config(&self) -> &DiaMs2PeakelConfig {
        &self.config
    }

    /// Detect whether the DIA acquisition uses staggered (overlapping) windows.
    ///
    /// Checks if consecutive windows (sorted by target m/z) overlap.
    fn detect_staggering(windows: &[IsolationWindow]) -> bool {
        if windows.len() < 2 {
            return false;
        }

        // Count overlapping consecutive pairs
        let mut overlap_count = 0usize;
        for pair in windows.windows(2) {
            if pair[0].upper_mz > pair[1].lower_mz {
                overlap_count += 1;
            }
        }

        // Staggered if majority of consecutive pairs overlap
        let is_staggered = overlap_count > windows.len() / 3;
        if is_staggered {
            log::info!("Detected staggered DIA: {}/{} consecutive window pairs overlap",
                       overlap_count, windows.len() - 1);
        }
        is_staggered
    }

    /// Check if two windows overlap in m/z space.
    #[inline]
    fn windows_overlap(a: &IsolationWindow, b: &IsolationWindow) -> bool {
        a.upper_mz > b.lower_mz && b.upper_mz > a.lower_mz
    }

    /// Discover all isolation windows in the mzDB file
    ///
    /// Detect peakels for a single isolation window
    ///
    /// This method loads MS2 spectra only for the specified isolation window,
    /// processes them using the walking algorithm, and returns detected peakels.
    /// Detect peakels for a single isolation window (backward-compatible, no neighbors)
    ///
    /// This method loads MS2 spectra only for the specified isolation window,
    /// processes them using the walking algorithm, and returns detected peakels.
    pub fn detect_peakels_for_window(
        &self,
        reader: &MzDbReader,
        window: &Arc<IsolationWindow>,
    ) -> Result<Vec<DiaMs2PeakelRecord>> {
        let spectra = Self::load_spectra_for_window(reader, window)?;
        let indexed = self.build_indexed_spectra(&spectra, window);
        let result = self.run_walking_algorithm_for_window(&indexed, window, None, None);
        Ok(result.peakels)
    }

    /// Load raw spectra for a single isolation window.
    ///
    /// Uses tight tolerance around the target m/z to match only spectra belonging
    /// to this specific window. The tolerance is based on the window spacing
    /// (half the distance to the nearest neighbor), not the isolation window width,
    /// to avoid capturing spectra from overlapping staggered windows.
    fn load_spectra_for_window(
        reader: &MzDbReader,
        window: &IsolationWindow,
    ) -> Result<Vec<crate::model::Spectrum>> {
        log::info!("Loading spectra for window {:.1} m/z ({} spectra)",
                   window.target_mz, window.spectrum_count);
        // Use default tolerance (0.1 Da) — main_precursor_mz values are identical
        // for all spectra in a given window, so tight matching is correct.
        // Using the window half-width would capture spectra from adjacent
        // staggered windows that share the overlap region.
        reader.get_dia_spectra_for_window(window.target_mz, None)
    }

    /// Build indexed spectra from raw spectra for fast m/z lookup.
    ///
    /// Data vectors are wrapped in Arc for efficient sharing across staggered window pairs.
    pub fn build_indexed_spectra(
        &self,
        spectra: &[crate::model::Spectrum],
        window: &Arc<IsolationWindow>,
    ) -> Vec<IndexedMs2Spectrum> {
        let mut indexed_spectra: Vec<IndexedMs2Spectrum> = Vec::with_capacity(spectra.len());

        for spectrum in spectra.iter() {
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
                spectrum_id: spectrum.header.id,
                time: spectrum.header.time,
                mz_values: Arc::new(mz_values),
                intensity_values: Arc::new(intensity_values),
                peak_indices: Arc::new(peak_indices),
                source_window: Arc::clone(window),
            });
        }

        indexed_spectra
    }

    /// Core walking algorithm for peakel detection with optional neighbor windows.
    ///
    /// When neighbors are provided, their spectra are included in the merged
    /// timeline for walk extension, but only peaks from `current_spectra` are
    /// used as seeds. Peakels whose true apex falls in a neighbor spectrum
    /// are discarded (they will be detected when that neighbor is current).
    /// Result of running the walking algorithm for a single window.
    /// Contains the kept peakels and any peakels discarded because their apex
    /// falls in a neighbor window's spectrum.
    fn run_walking_algorithm_for_window(
        &self,
        current_spectra: &[IndexedMs2Spectrum],
        window: &Arc<IsolationWindow>,
        left_neighbor: Option<&[IndexedMs2Spectrum]>,
        right_neighbor: Option<&[IndexedMs2Spectrum]>,
    ) -> Ms2PeakelDetectionResult {
        if current_spectra.is_empty() {
            return Ms2PeakelDetectionResult {
                peakels: Vec::new(),
                neighbor_discarded: 0,
            };
        }

        // Build peak data with or without neighbors
        let has_neighbors = left_neighbor.is_some() || right_neighbor.is_some();
        let peak_data = if has_neighbors {
            IsolationWindowPeakData::new_with_neighbors(
                current_spectra, left_neighbor, right_neighbor,
            )
        } else {
            IsolationWindowPeakData::new(current_spectra)
        };

        // Use the trait's default walking algorithm
        let peakels_with_keys = self.run_walking_algorithm(&peak_data);

        // Keep only peakels whose apex is in the current window.
        // Those with apex in a neighbor are discarded — they will be detected
        // when that neighbor window is processed as current.
        let mut kept = Vec::new();
        let mut neighbor_discarded = 0usize;

        for (peakel, _apex_peak_key) in peakels_with_keys {
            let in_current = if let Some(apex_idx) = peakel.apex_index() {
                let spectrum_ids = peakel.spectrum_ids();
                if apex_idx < spectrum_ids.len() {
                    peak_data.is_current_window_spectrum(spectrum_ids[apex_idx])
                } else {
                    true // conservative: keep if can't determine
                }
            } else {
                true
            };

            if in_current {
                kept.push(DiaMs2PeakelRecord::new(peakel, window.id));
            } else {
                neighbor_discarded += 1;
            }
        }

        Ms2PeakelDetectionResult { peakels: kept, neighbor_discarded }
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

    /// Sequential processing of isolation windows, emitting batches via callback.
    ///
    /// For staggered DIA, uses a sliding window of [prev, current, next] indexed spectra.
    /// Only peaks from the current window are used as seeds; neighbor spectra extend
    /// the walking algorithm's reach.
    ///
    /// The right neighbor's indexed spectra are reused as the current window's spectra
    /// in the next iteration, avoiding redundant DB loads.
    fn detect_sequential_in_batches(
        &self,
        reader: &MzDbReader,
        windows: &[Arc<IsolationWindow>],
        mut on_batch: impl FnMut(PeakelBatch<DiaMs2PeakelRecord>) -> Result<()>,
    ) -> Result<()> {
        let total_batches = windows.len();

        if !self.is_staggered {
            // Non-staggered: simple per-window processing (backward compatible)
            for (batch_index, window) in windows.iter().enumerate() {
                let peakels = self.detect_peakels_for_window(reader, window)?;
                on_batch(PeakelBatch { peakels, batch_index, total_batches })?;
            }
            return Ok(());
        }

        // Staggered: sliding window of indexed spectra
        let mut prev_indexed: Option<Vec<IndexedMs2Spectrum>> = None;
        let mut current_indexed: Option<Vec<IndexedMs2Spectrum>> = None;

        for (batch_index, window) in windows.iter().enumerate() {
            // Get current indexed spectra: reuse from previous iteration's right neighbor,
            // or load fresh from DB
            let cur = match current_indexed.take() {
                Some(cached) => cached,
                None => {
                    let raw_spectra = Self::load_spectra_for_window(reader, window)?;
                    self.build_indexed_spectra(&raw_spectra, window)
                }
            };

            // Determine left neighbor
            let left_neighbor = prev_indexed.as_ref()
                .filter(|_| batch_index > 0 && Self::windows_overlap(&windows[batch_index - 1], window))
                .map(|v| v.as_slice());

            // Load right neighbor if next window overlaps
            let next_indexed = if batch_index + 1 < windows.len()
                && Self::windows_overlap(window, &windows[batch_index + 1])
            {
                let next_window = &windows[batch_index + 1];
                let next_raw = Self::load_spectra_for_window(reader, next_window)?;
                Some(self.build_indexed_spectra(&next_raw, next_window))
            } else {
                None
            };
            let right_neighbor = next_indexed.as_deref();

            let result = self.run_walking_algorithm_for_window(
                &cur, window, left_neighbor, right_neighbor,
            );

            log::info!("  Window {}/{}: {:.1} m/z → {} peakels, {} neighbor-discarded (neighbors: left={}, right={})",
                       batch_index + 1, total_batches, window.target_mz, result.peakels.len(),
                       result.neighbor_discarded,
                       left_neighbor.is_some(), right_neighbor.is_some());

            on_batch(PeakelBatch { peakels: result.peakels, batch_index, total_batches })?;

            // Slide: current → prev, right neighbor → current for next iteration
            prev_indexed = Some(cur);
            current_indexed = next_indexed;
        }

        Ok(())
    }

    /// Parallel processing of isolation windows using producer-consumer pattern.
    ///
    /// For staggered DIA, the producer builds indexed spectra and shares them
    /// via Arc between consecutive work items (sliding triplet pattern).
    /// Each window's spectra are loaded once from DB and reused as neighbor.
    ///
    /// Architecture:
    /// - Producer thread: loads spectra with sliding Arc cache, sends triplets
    /// - N consumer threads: run detection with neighbors, send results
    /// - Main thread: receives results and calls on_batch
    #[cfg(feature = "processing-parallel")]
    fn detect_parallel_in_batches(
        &self,
        db_path: &str,
        windows: &[Arc<IsolationWindow>],
        num_threads: usize,
        mut on_batch: impl FnMut(PeakelBatch<DiaMs2PeakelRecord>) -> Result<()>,
    ) -> Result<()> {
        use crossbeam_channel::bounded;
        use std::time::Instant;

        let total_windows = windows.len();
        let queue_size = num_threads * 2;

        log::info!("Processing {} isolation windows with {} consumer threads (queue size: {}, staggered={})",
                   total_windows, num_threads, queue_size, self.is_staggered);

        // Work item: window + current indexed spectra + optional neighbor spectra (Arc-shared)
        struct IsolationWindowTripletData {
            window_idx: usize,
            window: Arc<IsolationWindow>,
            current_spectra: Arc<Vec<IndexedMs2Spectrum>>,
            left_neighbor: Option<Arc<Vec<IndexedMs2Spectrum>>>,
            right_neighbor: Option<Arc<Vec<IndexedMs2Spectrum>>>,
        }
        // SAFETY: IndexedMs2Spectrum contains Arc fields which are Send+Sync
        unsafe impl Send for IsolationWindowTripletData {}

        let (work_tx, work_rx) = bounded::<IsolationWindowTripletData>(queue_size);
        let (result_tx, result_rx) = bounded::<(usize, Ms2PeakelDetectionResult)>(queue_size);

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

                    while let std::result::Result::Ok(work) = work_rx.recv() {
                        let left = work.left_neighbor.as_deref().map(|v| v.as_slice());
                        let right = work.right_neighbor.as_deref().map(|v| v.as_slice());

                        let result = self.run_walking_algorithm_for_window(
                            &work.current_spectra, &work.window, left, right,
                        );

                        log::debug!("Thread {} processed window {}/{}: {:.1} m/z ({} peakels, {} neighbor-discarded)",
                                   thread_id, work.window_idx + 1, total_windows,
                                   work.window.target_mz, result.peakels.len(), result.neighbor_discarded);

                        if result_tx.send((work.window_idx, result)).is_err() {
                            log::warn!("Result channel closed, stopping consumer {}", thread_id);
                            break;
                        }
                        items_processed += 1;
                    }

                    log::debug!("Consumer thread {} finished, processed {} items", thread_id, items_processed);
                });
            }

            drop(work_rx);
            drop(result_tx);

            // Producer: sliding window with Arc sharing
            let producer_error = &producer_error;
            let is_staggered = self.is_staggered;

            scope.spawn(move || {
                let result: Result<()> = (|| {
                    let producer_reader = MzDbReader::builder(db_path)
                        .read_only()
                        .build()?;

                    log::info!("Producer starting to load spectra (staggered={})...", is_staggered);

                    // Sliding cache: prev, current, and prefetched next (as Arcs)
                    let mut prev_arc: Option<Arc<Vec<IndexedMs2Spectrum>>> = None;
                    let mut current_arc: Option<Arc<Vec<IndexedMs2Spectrum>>> = None;

                    for (idx, window) in windows.iter().enumerate() {
                        // Get current: reuse from previous iteration's prefetched next, or load fresh
                        let cur_arc = match current_arc.take() {
                            Some(cached) => cached,
                            None => {
                                let raw = Self::load_spectra_for_window(&producer_reader, window)?;
                                Arc::new(self.build_indexed_spectra(&raw, window))
                            }
                        };

                        // Determine neighbors for staggered DIA
                        let left_neighbor = if is_staggered {
                            prev_arc.as_ref()
                                .filter(|_| idx > 0 && Self::windows_overlap(&windows[idx - 1], window))
                                .cloned()
                        } else {
                            None
                        };

                        // Load and cache right neighbor if next window overlaps
                        let next_arc = if is_staggered && idx + 1 < windows.len()
                            && Self::windows_overlap(window, &windows[idx + 1])
                        {
                            let next_window = &windows[idx + 1];
                            let next_raw = Self::load_spectra_for_window(
                                &producer_reader, next_window
                            )?;
                            Some(Arc::new(self.build_indexed_spectra(&next_raw, next_window)))
                        } else {
                            None
                        };

                        let right_neighbor = next_arc.clone();

                        log::debug!("Producer loaded window {}/{}: {:.1} m/z (left={}, right={})",
                                   idx + 1, total_windows, window.target_mz,
                                   left_neighbor.is_some(), right_neighbor.is_some());

                        if work_tx.send(IsolationWindowTripletData {
                            window_idx: idx,
                            window: Arc::clone(window),
                            current_spectra: Arc::clone(&cur_arc),
                            left_neighbor,
                            right_neighbor,
                        }).is_err() {
                            log::warn!("Work channel closed early, stopping producer");
                            break;
                        }

                        // Slide: current → prev, prefetched next → current
                        prev_arc = Some(cur_arc);
                        current_arc = next_arc;
                    }

                    log::debug!("Producer finished: {} windows", total_windows);
                    Ok(())
                })();

                if let Err(e) = result {
                    log::error!("Producer error: {:?}", e);
                    *producer_error.lock().expect("producer_error mutex poisoned") = Some(e);
                }
            });

            // Main thread: receive results, emit batches
            let mut total_peakels = 0usize;
            let mut batches_emitted = 0usize;

            for (window_idx, result) in result_rx {
                total_peakels += result.peakels.len();

                if let Err(e) = on_batch(PeakelBatch {
                    peakels: result.peakels,
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
        });

        // Check for producer errors
        if let Some(e) = producer_error.into_inner()
            .map_err(|e| anyhow!("Failed to check producer error: {:?}", e))?
        {
            return Err(e);
        }

        Ok(())
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