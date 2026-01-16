//! Common Detection Utilities for MS1 and MS2 Peakel Detection
//!
//! This module provides shared components used by both MS1 and MS2 detection algorithms:
//! - Binary search for nearest peak within m/z tolerance
//! - Index-based sorting (2-3x faster than tuple sorting)
//! - Peakel finder creation based on algorithm selection

use super::finder::{
    BasicPeakelFinder, PeakelFinder, SmartPeakelFinder, SmartPeakelFinderConfig,
};

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
    mz_values: &[f64],
    intensity_values: &[f32],
    target_mz: f64,
    mz_tol_da: f64,
) -> Option<(f64, f32, usize)> {
    if mz_values.is_empty() {
        return None;
    }

    let min_mz = target_mz - mz_tol_da;
    let max_mz = target_mz + mz_tol_da;

    // Binary search for start position
    let start = mz_values.partition_point(|&mz| mz < min_mz);

    let mut best: Option<(f64, f32, usize)> = None;
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

/// Find the nearest peak in a sorted array of (mz, intensity, index) tuples.
///
/// This variant is used by MS2 detection which works with pre-sorted peak arrays
/// where the index represents the original position in the source spectrum.
///
/// # Arguments
/// * `peaks` - Slice of (mz, intensity, original_index) tuples, sorted by m/z
/// * `target_mz` - Target m/z to search for
/// * `mz_tol_da` - m/z tolerance in Daltons
///
/// # Returns
/// `Some((mz, intensity, original_index))` if found, `None` otherwise
pub fn find_nearest_peak(
    peaks: &[(f64, f32, usize)],
    target_mz: f64,
    mz_tol_da: f64,
) -> Option<(f64, f32, usize)> {
    if peaks.is_empty() {
        return None;
    }

    let min_mz = target_mz - mz_tol_da;
    let max_mz = target_mz + mz_tol_da;

    // Binary search for start position
    let start = peaks.partition_point(|p| p.0 < min_mz);

    let mut best: Option<(f64, f32, usize)> = None;
    let mut best_diff = mz_tol_da;

    for i in start..peaks.len() {
        let (mz, intensity, peak_idx) = peaks[i];
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

// ============================================================================
// Range Check
// ============================================================================

/// Fast range check using cached min/max bounds.
///
/// Returns true if the target m/z with tolerance could possibly overlap
/// with the [min_mz, max_mz] range. Used by MS1 detection to skip
/// peak lists that cannot contain matching peaks.
#[inline]
pub fn is_target_mz_within_range(target_mz: f64, mz_tol_da: f64, min_mz: f64, max_mz: f64) -> bool {
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
    fn test_find_nearest_peak() {
        let peaks = vec![
            (100.0, 1000.0f32, 0usize),
            (200.0, 2000.0, 1),
            (300.0, 3000.0, 2),
        ];

        let result = find_nearest_peak(&peaks, 199.8, 1.0);
        assert_eq!(result, Some((200.0, 2000.0, 1)));

        let result = find_nearest_peak(&peaks, 250.0, 1.0);
        assert_eq!(result, None);

        let result = find_nearest_peak(&[], 200.0, 1.0);
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
}