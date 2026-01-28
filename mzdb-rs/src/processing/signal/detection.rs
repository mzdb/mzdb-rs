//! Common Detection Utilities for MS1 and MS2 Peakel Detection
//!
//! This module provides shared components used by both MS1 and MS2 detection algorithms:
//! - Core traits for abstracting the walking algorithm
//! - Binary search for nearest peak within m/z tolerance
//! - Index-based sorting (2-3x faster than tuple sorting)
//! - Peakel finder creation based on algorithm selection
//! - Generic walking algorithm implementation

use std::collections::HashSet;
use std::hash::Hash;

use smallvec::SmallVec;

use super::finder::{
    BasicPeakelFinder, PeakelFinder, SmartPeakelFinder, SmartPeakelFinderConfig,
};
use crate::processing::Peakel;

// ============================================================================
// Configuration Trait
// ============================================================================

/// Common configuration for peakel detection algorithms.
pub trait PeakelDetectionConfig: Clone + Send + Sync {
    fn mz_tol_ppm(&self) -> f32;
    fn min_intensity(&self) -> f32;
    fn min_peaks(&self) -> usize;
    fn max_consecutive_gaps(&self) -> usize;
    fn max_total_gaps(&self) -> usize;
    fn max_time_window(&self) -> f32;
    fn intensity_percentile(&self) -> f32;
    fn min_peakel_amplitude(&self) -> f32;
    fn min_peakel_duration(&self) -> f32;
    fn algorithm(&self) -> &str;
    
    #[inline]
    fn half_of_max_total_gaps(&self) -> usize {
        1 + (self.max_total_gaps() / 2)
    }
    
    #[inline]
    fn max_half_duration(&self) -> f32 {
        self.max_time_window() / 2.0
    }
    
    fn create_finder(&self) -> Box<dyn PeakelFinder + Send + Sync> {
        create_peakel_finder(self.algorithm(), self.min_peaks())
    }
}

// ============================================================================
// Spectrum Peak Lookup Trait
// ============================================================================

/// Trait for spectrum-level peak lookup.
pub trait SpectrumPeakLookup {
    type PeakKey: Eq + Hash + Clone + Copy;
    
    /// Find the nearest peak within m/z tolerance.
    /// The `spectrum_idx` is provided by the caller so the returned PeakKey can include it.
    fn find_nearest_peak(&self, target_mz: f32, mz_tol_da: f32, spectrum_idx: usize) -> Option<(f32, f32, Self::PeakKey)>;
    fn spectrum_id(&self) -> i64;
    fn time(&self) -> f32;
}

// ============================================================================
// Sorted Peaks Provider Trait
// ============================================================================

/// Provider for peaks sorted by descending intensity.
pub trait SortedPeaksProvider {
    type PeakKey: Eq + Hash + Clone + Copy;
    type SpectrumLookup: SpectrumPeakLookup<PeakKey = Self::PeakKey>;
    
    fn sorted_peaks_iter(&self) -> impl Iterator<Item = (f32, f32, usize, Self::PeakKey)>;
    fn get_spectrum_lookup(&self, idx: usize) -> &Self::SpectrumLookup;
    fn spectra_count(&self) -> usize;
    fn is_apex_in_valid_mz_range(&self, apex_mz: f32) -> bool;
    fn calc_intensity_threshold(&self, detector_config: &impl PeakelDetectionConfig) -> f32;
}

// ============================================================================
// Peakel Detector Trait
// ============================================================================

/// Common interface for peakel detectors (MS1 and MS2).
/// 
/// This trait provides a unified contract for peakel detection algorithms,
/// with a default implementation of the core walking algorithm. Implementors
/// only need to provide the configuration and can use the default detection method.
/// 
/// # Type Parameters
/// 
/// The associated types allow each implementation to use appropriate structures:
/// - `Config`: Detection configuration (Ms1PeakelConfig, DiaMs2PeakelConfig)
/// - `PeakData`: Prepared peak data for detection (RunSlicePeakData, IsolationWindowPeakData)
/// 
/// # Example
/// 
/// ```ignore
/// impl PeakelDetector for Ms1PeakelDetector {
///     type Config = Ms1PeakelConfig;
///     type PeakData = RunSlicePeakData;
///     
///     fn config(&self) -> &Self::Config { &self.config }
/// }
/// 
/// // Use the default walking algorithm
/// let peakels = detector.detect_from_peak_data(&peak_data);
/// ```
pub trait PeakelDetector {
    /// Configuration type for detection parameters
    type Config: PeakelDetectionConfig;
    
    /// Peak data type prepared for detection
    type PeakData: SortedPeaksProvider;
    
    /// Get the detector's configuration
    fn config(&self) -> &Self::Config;
    
    /// Detect peakels from prepared peak data using the walking algorithm.
    /// 
    /// This is the core detection method that implements the walking algorithm
    /// shared by both MS1 and MS2 detection. The algorithm:
    /// 
    /// 1. Iterates peaks by descending intensity
    /// 2. For each apex, walks bidirectionally to extract XIC
    /// 3. Applies PeakelFinder to detect boundaries
    /// 4. Validates and builds peakels
    /// 
    /// Override this method only if you need completely custom detection logic.
    fn detect_from_peak_data(&self, peak_data: &Self::PeakData) -> Vec<Peakel>
    where
        <Self::PeakData as SortedPeaksProvider>::SpectrumLookup: 
            SpectrumPeakLookup<PeakKey = <Self::PeakData as SortedPeaksProvider>::PeakKey>,
    {
        let config = self.config();
        let finder = config.create_finder();
        let intensity_threshold = peak_data.calc_intensity_threshold(config);
        let half_of_max_total_gaps = config.half_of_max_total_gaps();
        let max_half_duration = config.max_half_duration();
        let min_peaks = config.min_peaks();
        let max_consecutive_gaps = config.max_consecutive_gaps();
        let mz_tol_ppm = config.mz_tol_ppm();
        
        let mut peakels = Vec::new();
        let mut used_peaks: HashSet<<Self::PeakData as SortedPeaksProvider>::PeakKey> = HashSet::new();
        
        // XIC vectors reused across iterations
        let mut xic_times: Vec<f32> = Vec::new();
        let mut xic_intensities: Vec<f32> = Vec::new();
        let mut xic_mz_values: Vec<f32> = Vec::new();
        let mut xic_peak_keys: Vec<<Self::PeakData as SortedPeaksProvider>::PeakKey> = Vec::new();
        let mut xic_spectrum_indices: Vec<usize> = Vec::new();
        
        for (apex_mz, apex_intensity, apex_spectrum_idx, apex_peak_key) in peak_data.sorted_peaks_iter() {
            if used_peaks.contains(&apex_peak_key) {
                continue;
            }
            
            if apex_intensity < intensity_threshold {
                break;
            }
            
            if !peak_data.is_apex_in_valid_mz_range(apex_mz) {
                continue;
            }
            
            let mz_tol_da = apex_mz * mz_tol_ppm * 1e-6;
            let apex_time = peak_data.get_spectrum_lookup(apex_spectrum_idx).time();
            
            xic_times.clear();
            xic_intensities.clear();
            xic_mz_values.clear();
            xic_peak_keys.clear();
            xic_spectrum_indices.clear();
            
            // Walk both directions: right (+1) then left (-1)
            for direction in [1i32, -1i32] {
                let mut gap_count = 0usize;
                let mut half_gaps_count = 0usize;
                let mut offset = if direction > 0 { 1 } else { 0 };
                
                loop {
                    if half_gaps_count > half_of_max_total_gaps {
                        break;
                    }
                    
                    let target_idx = apex_spectrum_idx as i32 + direction * offset;
                    if target_idx < 0 || target_idx as usize >= peak_data.spectra_count() {
                        break;
                    }
                    let target_idx = target_idx as usize;
                    
                    let spectrum = peak_data.get_spectrum_lookup(target_idx);
                    
                    if (spectrum.time() - apex_time).abs() > max_half_duration {
                        break;
                    }
                    
                    if let Some((mz, intensity, peak_key)) = spectrum.find_nearest_peak(apex_mz, mz_tol_da, target_idx) {
                        if !used_peaks.contains(&peak_key) {
                            if direction > 0 {
                                xic_times.push(spectrum.time());
                                xic_intensities.push(intensity);
                                xic_mz_values.push(mz);
                                xic_peak_keys.push(peak_key);
                                xic_spectrum_indices.push(target_idx);
                            } else {
                                xic_times.insert(0, spectrum.time());
                                xic_intensities.insert(0, intensity);
                                xic_mz_values.insert(0, mz);
                                xic_peak_keys.insert(0, peak_key);
                                xic_spectrum_indices.insert(0, target_idx);
                            }
                            gap_count = 0;
                        } else {
                            gap_count += 1;
                            half_gaps_count += 1;
                        }
                    } else {
                        gap_count += 1;
                        half_gaps_count += 1;
                    }
                    
                    if gap_count > max_consecutive_gaps {
                        break;
                    }
                    
                    offset += 1;
                }
            }
            
            // Insert apex at correct position
            let apex_pos = xic_times.partition_point(|&t| t < apex_time);
            xic_times.insert(apex_pos, apex_time);
            xic_intensities.insert(apex_pos, apex_intensity);
            xic_mz_values.insert(apex_pos, apex_mz);
            xic_peak_keys.insert(apex_pos, apex_peak_key);
            xic_spectrum_indices.insert(apex_pos, apex_spectrum_idx);
            
            if xic_times.len() < min_peaks {
                continue;
            }
            
            let xic_pairs: Vec<(f32, f64)> = xic_times.iter()
                .zip(xic_intensities.iter())
                .map(|(&t, &i)| (t, i as f64))
                .collect();
            
            let ranges = finder.find_peakels_indices(&xic_pairs);
            
            let (matching_range, detected_indices) = find_matching_peakel_range(
                &ranges, &xic_times, apex_time
            );
            
            for (i, key) in xic_peak_keys.iter().enumerate() {
                if !detected_indices.contains(&i) {
                    used_peaks.insert(*key);
                }
            }
            
            if let Some((start, end)) = matching_range {
                if let Some(peakel) = Self::validate_and_build_peakel(
                    &xic_times[start..=end],
                    &xic_intensities[start..=end],
                    &xic_mz_values[start..=end],
                    &xic_spectrum_indices[start..=end],
                    apex_pos.saturating_sub(start),
                    config,
                    |idx| peak_data.get_spectrum_lookup(idx).spectrum_id(),
                ) {
                    for key in &xic_peak_keys[start..=end] {
                        used_peaks.insert(*key);
                    }
                    peakels.push(peakel);
                }
            }
        }
        
        peakels
    }
    
    /// Validate a peakel and build it if valid.
    /// 
    /// Override this method to customize peakel construction (e.g., for MS2
    /// which may need to attach isolation window metadata).
    /// 
    /// Default implementation performs standard validation and builds a basic Peakel.
    fn validate_and_build_peakel<F>(
        xic_times: &[f32],
        xic_intensities: &[f32],
        xic_mz_values: &[f32],
        spectrum_indices: &[usize],
        apex_index_in_peakel: usize,
        config: &Self::Config,
        get_spectrum_id: F,
    ) -> Option<Peakel>
    where
        F: Fn(usize) -> i64,
    {
        let peakel_len = xic_times.len();
        
        if peakel_len < config.min_peaks() {
            return None;
        }
        
        // Validation 1: apex must not be first or last peak
        if apex_index_in_peakel == 0 || apex_index_in_peakel >= peakel_len - 1 {
            return None;
        }
        
        // Validation 2: check amplitude (Scala: apex / intensityValues.filter(i => i > 0 && !i.isNaN).min)
        let min_intensity = xic_intensities.iter()
            .cloned()
            .filter(|&i| i > 0.0 && !i.is_nan())
            .fold(f32::INFINITY, f32::min);
        let apex_intensity = xic_intensities.iter()
            .cloned()
            .fold(f32::NEG_INFINITY, f32::max);
        let amplitude = if min_intensity > 0.0 && min_intensity < f32::INFINITY {
            apex_intensity / min_intensity
        } else {
            f32::NAN
        };
        
        if amplitude.is_nan() || amplitude < config.min_peakel_amplitude() {
            return None;
        }
        
        // Validation 3: check duration
        let first_time = xic_times.first().copied().unwrap_or(0.0);
        let last_time = xic_times.last().copied().unwrap_or(0.0);
        let duration = last_time - first_time;
        
        if duration < config.min_peakel_duration() {
            return None;
        }
        
        // All validations passed - collect spectrum IDs
        let spectrum_ids: Vec<i64> = (0..peakel_len)
            .map(|i| get_spectrum_id(spectrum_indices[i]))
            .collect();
        
        // Calculate gap count
        let first_spectrum_idx = spectrum_indices.first().copied().unwrap_or(0);
        let last_spectrum_idx = spectrum_indices.last().copied().unwrap_or(0);
        let spectrum_span = last_spectrum_idx.saturating_sub(first_spectrum_idx) + 1;
        let gap_count = spectrum_span.saturating_sub(peakel_len);
        
        Some(Peakel::new(
            SmallVec::from_iter(spectrum_ids.iter().copied()),
            SmallVec::from_iter(xic_times.iter().copied()),
            SmallVec::from_iter(xic_mz_values.iter().copied()),
            SmallVec::from_iter(xic_intensities.iter().copied()),
            None,
            None,
            gap_count,
        ))
    }
}

// ============================================================================
// Walking Algorithm Helper Functions
// ============================================================================

/// Find the peakel range containing the apex and collect all detected indices.
pub fn find_matching_peakel_range(
    ranges: &[(usize, usize)],
    xic_times: &[f32],
    apex_time: f32,
) -> (Option<(usize, usize)>, HashSet<usize>) {
    let mut matching_range: Option<(usize, usize)> = None;
    let mut detected_indices: HashSet<usize> = HashSet::new();
    
    for &(start, end) in ranges {
        let start_time = xic_times[start];
        let end_time = xic_times[end];
        if apex_time >= start_time && apex_time <= end_time {
            matching_range = Some((start, end));
        }
        for i in start..=end {
            detected_indices.insert(i);
        }
    }
    
    (matching_range, detected_indices)
}

// ============================================================================
// Peakel Finder Creation
// ============================================================================

/// Create a PeakelFinder based on algorithm name and minimum peaks count.
///
/// # Arguments
/// * `algorithm` - Algorithm name: "smart" for SmartPeakelFinder, anything else for BasicPeakelFinder
/// * `min_peaks` - Minimum number of peaks required for a peakel
///
/// # Returns
/// A boxed PeakelFinder trait object
pub fn create_peakel_finder(algorithm: &str, min_peaks: usize) -> Box<dyn PeakelFinder + Send + Sync> {
    match algorithm {
        "smart" => {
            let mut config = SmartPeakelFinderConfig::default();
            config.min_peaks_count = min_peaks;
            config.use_smoothing = true;
            config.use_baseline_remover = false;
            Box::new(SmartPeakelFinder::with_config(config))
        }
        _ => {
            Box::new(BasicPeakelFinder::new(2, min_peaks))
        }
    }
}

// ============================================================================
// Binary Search for Nearest Peak
// ============================================================================

/// Find the nearest peak within m/z tolerance using binary search on separate slices.
///
/// This is the core peak lookup algorithm used by MS1 detection.
/// It performs a binary search to find the starting position, then linearly
/// scans within the tolerance window to find the closest peak.
///
/// # Arguments
/// * `mz_values` - Slice of m/z values, must be sorted in ascending order
/// * `intensity_values` - Slice of intensity values, same length as mz_values
/// * `target_mz` - Target m/z to search for
/// * `mz_tol_da` - m/z tolerance in Daltons
///
/// # Returns
/// `Some((mz, intensity, index))` if a peak is found within tolerance, `None` otherwise
pub fn find_nearest_peak_from_slices(
    mz_values: &[f32],
    intensity_values: &[f32],
    target_mz: f32,
    mz_tol_da: f32,
) -> Option<(f32, f32, usize)> {
    if mz_values.is_empty() {
        return None;
    }

    let min_mz = target_mz - mz_tol_da;
    let max_mz = target_mz + mz_tol_da;

    // Binary search for start position
    let start = mz_values.partition_point(|&mz| mz < min_mz);

    let mut best: Option<(f32, f32, usize)> = None;
    let mut best_diff = mz_tol_da;

    for i in start..mz_values.len() {
        let mz = mz_values[i];
        if mz > max_mz {
            break;
        }
        let diff = (mz - target_mz).abs();
        if diff < best_diff {
            best_diff = diff;
            best = Some((mz, intensity_values[i], i));
        }
    }

    best
}

// ============================================================================
// Range Check
// ============================================================================

/// Fast range check using cached min/max bounds.
///
/// Returns true if the target m/z with tolerance could possibly overlap
/// with the [min_mz, max_mz] range. Used by MS1 detection to skip
/// peak lists that cannot contain matching peaks.
#[inline]
pub fn is_target_mz_within_range(target_mz: f32, mz_tol_da: f32, min_mz: f32, max_mz: f32) -> bool {
    let search_min = target_mz - mz_tol_da;
    let search_max = target_mz + mz_tol_da;
    search_max >= min_mz && search_min <= max_mz
}

// ============================================================================
// Index-Based Sorting
// ============================================================================

/// Sort indices by descending f32 value (Scala-style optimization).
///
/// This is significantly faster than sorting tuples directly because:
/// - Sorting indices (8 bytes) involves less memory movement than large tuples
/// - Better cache utilization during comparisons
///
/// Benchmarks show 2-3x speedup for typical run slice sizes (50k-100k peaks).
///
/// # Arguments
/// * `values` - Slice of f32 values to sort by (descending order)
///
/// # Returns
/// Vector of indices into the original slice, sorted by descending value
pub fn sort_indices_by_descending_f32_value(values: &[f32]) -> Vec<usize> {
    let mut indices: Vec<usize> = (0..values.len()).collect();
    indices.sort_by(|&a, &b| {
        values[b].partial_cmp(&values[a]).unwrap_or(std::cmp::Ordering::Equal)
    });
    indices
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_find_nearest_peak_from_slices() {
        let mz_values = vec![100.0, 200.0, 300.0, 400.0, 500.0];
        let intensities = vec![1000.0, 2000.0, 3000.0, 2000.0, 1000.0];

        // Exact match
        let result = find_nearest_peak_from_slices(&mz_values, &intensities, 300.0, 1.0);
        assert_eq!(result, Some((300.0, 3000.0, 2)));

        // Within tolerance
        let result = find_nearest_peak_from_slices(&mz_values, &intensities, 299.5, 1.0);
        assert_eq!(result, Some((300.0, 3000.0, 2)));

        // Outside tolerance
        let result = find_nearest_peak_from_slices(&mz_values, &intensities, 250.0, 1.0);
        assert_eq!(result, None);

        // Empty array
        let result = find_nearest_peak_from_slices(&[], &[], 300.0, 1.0);
        assert_eq!(result, None);
    }

    #[test]
    fn test_is_target_mz_within_range() {
        assert!(is_target_mz_within_range(300.0, 1.0, 100.0, 500.0));
        assert!(is_target_mz_within_range(300.0, 1.0, 299.5, 300.5));
        assert!(!is_target_mz_within_range(300.0, 1.0, 400.0, 500.0));
        assert!(!is_target_mz_within_range(300.0, 1.0, 100.0, 298.0));
    }

    #[test]
    fn test_sort_indices_by_descending_f32_value() {
        let values = vec![100.0f32, 500.0, 200.0, 400.0];
        let sorted = sort_indices_by_descending_f32_value(&values);
        assert_eq!(sorted, vec![1, 3, 2, 0]); // 500 > 400 > 200 > 100
    }

    #[test]
    fn test_create_peakel_finder() {
        // Just verify it doesn't panic
        let _smart = create_peakel_finder("smart", 5);
        let _basic = create_peakel_finder("basic", 3);
        let _default = create_peakel_finder("unknown", 4);
    }
    
    #[test]
    fn test_find_matching_peakel_range() {
        let ranges = vec![(0, 4), (6, 10)];
        let xic_times = vec![1.0, 2.0, 3.0, 4.0, 5.0, 5.5, 6.0, 7.0, 8.0, 9.0, 10.0];
        
        let (matching, detected) = find_matching_peakel_range(&ranges, &xic_times, 3.0);
        assert_eq!(matching, Some((0, 4)));
        assert!(detected.contains(&0));
        assert!(detected.contains(&4));
        assert!(detected.contains(&6));
        assert!(detected.contains(&10));
        assert!(!detected.contains(&5));
        
        let (matching, _) = find_matching_peakel_range(&ranges, &xic_times, 8.0);
        assert_eq!(matching, Some((6, 10)));
        
        let (matching, _) = find_matching_peakel_range(&ranges, &xic_times, 5.5);
        assert_eq!(matching, None);
    }
}
