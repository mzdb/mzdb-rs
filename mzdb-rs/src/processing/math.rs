//! Mathematical utilities for signal processing
//!
//! This module provides mathematical functions used in signal processing,
//! including derivative analysis and histogram computation.

use std::collections::HashMap;

use itertools::Itertools;

// ============================================================================
// Derivative Analysis
// ============================================================================

/// Calculate ternary slopes (-1, 0, +1) from a signal
///
/// # Arguments
/// * `values` - Input signal values
/// * `derivative_level` - Level of derivative (1 = first derivative, 2 = second, etc.)
///
/// # Returns
/// Vector of slopes where each value is -1, 0, or +1
pub fn calc_ternary_slopes(values: &[f64], derivative_level: usize) -> Vec<f64> {
    if values.len() < 2 {
        return vec![];
    }

    let signums: Vec<f64> = values
        .windows(2)
        .map(|w| {
            let diff = w[1] - w[0];
            if diff == 0.0 {
                0.0
            } else {
                diff.signum()
            }
        })
        .collect();

    if derivative_level <= 1 {
        signums
    } else {
        calc_ternary_slopes(&signums, derivative_level - 1)
    }
}

/// Represents a local extremum (minimum or maximum)
#[derive(Clone, Debug, PartialEq)]
pub enum LocalExtremum {
    Minimum { value: f64, index: usize },
    Maximum { value: f64, index: usize },
}

impl LocalExtremum {
    pub fn value(&self) -> f64 {
        match self {
            LocalExtremum::Minimum { value, .. } => *value,
            LocalExtremum::Maximum { value, .. } => *value,
        }
    }

    pub fn index(&self) -> usize {
        match self {
            LocalExtremum::Minimum { index, .. } => *index,
            LocalExtremum::Maximum { index, .. } => *index,
        }
    }

    pub fn is_minimum(&self) -> bool {
        matches!(self, LocalExtremum::Minimum { .. })
    }

    pub fn is_maximum(&self) -> bool {
        matches!(self, LocalExtremum::Maximum { .. })
    }
}

/// Find all local minima and maxima in a signal
pub fn find_local_extrema(values: &[f64]) -> Vec<LocalExtremum> {
    if values.len() < 2 {
        return vec![];
    }

    let mut extrema: Vec<LocalExtremum> = Vec::new();
    let mut prev_slope = 0i32;
    let mut ascending_seen = false;

    for i in 0..values.len() - 1 {
        let diff = values[i + 1] - values[i];
        let cur_slope = if diff > 0.0 {
            1
        } else if diff < 0.0 {
            -1
        } else {
            0
        };

        if !ascending_seen && cur_slope == 1 {
            ascending_seen = true;
            extrema.push(LocalExtremum::Minimum {
                value: values[i],
                index: i,
            });
        } else if ascending_seen && cur_slope != 0 && cur_slope != prev_slope {
            if prev_slope == 1 {
                extrema.push(LocalExtremum::Maximum {
                    value: values[i],
                    index: i,
                });
            } else if prev_slope == -1 {
                extrema.push(LocalExtremum::Minimum {
                    value: values[i],
                    index: i,
                });
            }
        }

        if cur_slope != 0 {
            prev_slope = cur_slope;
        }
    }

    // Handle end of signal
    if ascending_seen && prev_slope == 1 {
        let last_idx = values.len() - 1;
        extrema.push(LocalExtremum::Maximum {
            value: values[last_idx],
            index: last_idx,
        });
    }

    extrema
}

/// Filter extrema to keep only significant ones
///
/// # Arguments
/// * `extrema` - List of extrema to filter
/// * `values` - Original signal values
/// * `distance_thresh` - Minimum distance between significant extrema
/// * `intensity_rel_thresh` - Minimum relative intensity for significance
pub fn filter_significant_extrema(
    extrema: &[LocalExtremum],
    _values: &[f64],
    distance_thresh: usize,
    intensity_rel_thresh: f64,
) -> Vec<LocalExtremum> {
    if extrema.len() < 3 {
        return extrema.to_vec();
    }

    let mut significant: Vec<LocalExtremum> = Vec::new();

    for (i, ext) in extrema.iter().enumerate() {
        let is_significant = match ext {
            LocalExtremum::Maximum { .. } => true, // Maxima are always kept
            LocalExtremum::Minimum { value, index } => {
                // Check distance and intensity relative to adjacent maxima
                let prev_max = extrema[..i].iter().rev().find(|e| e.is_maximum());
                let next_max = extrema[i + 1..].iter().find(|e| e.is_maximum());

                match (prev_max, next_max) {
                    (Some(pm), Some(nm)) => {
                        let dist_prev = index.abs_diff(pm.index());
                        let dist_next = nm.index().abs_diff(*index);

                        dist_prev >= distance_thresh
                            && dist_next >= distance_thresh
                            && *value < pm.value() * intensity_rel_thresh
                            && *value < nm.value() * intensity_rel_thresh
                    }
                    _ => true, // Keep edge extrema
                }
            }
        };

        if is_significant {
            significant.push(ext.clone());
        }
    }

    significant
}

// ============================================================================
// Histogram Computation
// ============================================================================

/// A histogram bin
#[derive(Clone, Debug)]
pub struct HistogramBin {
    pub lower_bound: f64,
    pub upper_bound: f64,
    pub count: usize,
    pub sum: f64,
}

impl HistogramBin {
    pub fn new(lower: f64, upper: f64) -> Self {
        Self {
            lower_bound: lower,
            upper_bound: upper,
            count: 0,
            sum: 0.0,
        }
    }

    pub fn center(&self) -> f64 {
        (self.lower_bound + self.upper_bound) / 2.0
    }

    pub fn width(&self) -> f64 {
        self.upper_bound - self.lower_bound
    }

    pub fn add(&mut self, value: f64) {
        self.count += 1;
        self.sum += value;
    }

    pub fn mean(&self) -> f64 {
        if self.count == 0 {
            0.0
        } else {
            self.sum / self.count as f64
        }
    }
}

/// Compute a histogram from values
///
/// # Arguments
/// * `values` - Input values
/// * `nb_bins` - Number of bins
///
/// # Returns
/// Vector of histogram bins
pub fn compute_histogram(values: &[f64], nb_bins: usize) -> Vec<HistogramBin> {
    if values.is_empty() || nb_bins == 0 {
        return vec![];
    }

    let (min_val, max_val) = match values.iter().cloned().minmax() {
        itertools::MinMaxResult::NoElements => return vec![],
        itertools::MinMaxResult::OneElement(v) => return vec![HistogramBin::new(v, v + 1.0)],
        itertools::MinMaxResult::MinMax(min, max) => (min, max),
    };

    if min_val == max_val {
        return vec![HistogramBin::new(min_val, max_val + 1.0)];
    }

    let range = max_val - min_val;
    let bin_width = range / nb_bins as f64;

    let mut bins: Vec<HistogramBin> = (0..nb_bins)
        .map(|i| {
            let lower = min_val + i as f64 * bin_width;
            let upper = lower + bin_width;
            HistogramBin::new(lower, upper)
        })
        .collect();

    // Assign values to bins
    for &value in values {
        let bin_idx = ((value - min_val) / bin_width) as usize;
        let bin_idx = bin_idx.min(bins.len() - 1);
        bins[bin_idx].add(value);
    }

    bins
}

/// Compute a 2D histogram from (x, y) pairs
pub fn compute_histogram_2d(
    x_values: &[f64],
    y_values: &[f64],
    nb_x_bins: usize,
    nb_y_bins: usize,
) -> HashMap<(usize, usize), f64> {
    if x_values.len() != y_values.len() || x_values.is_empty() {
        return HashMap::new();
    }

    let (x_min, x_max) = match x_values.iter().cloned().minmax() {
        itertools::MinMaxResult::MinMax(min, max) => (min, max),
        _ => return HashMap::new(),
    };
    let (y_min, y_max) = match y_values.iter().cloned().minmax() {
        itertools::MinMaxResult::MinMax(min, max) => (min, max),
        _ => return HashMap::new(),
    };

    let x_range = x_max - x_min;
    let y_range = y_max - y_min;

    if x_range == 0.0 || y_range == 0.0 {
        return HashMap::new();
    }

    let x_bin_width = x_range / nb_x_bins as f64;
    let y_bin_width = y_range / nb_y_bins as f64;

    let mut histogram: HashMap<(usize, usize), f64> = HashMap::new();

    for (&x, &y) in x_values.iter().zip(y_values.iter()) {
        let x_bin = ((x - x_min) / x_bin_width) as usize;
        let y_bin = ((y - y_min) / y_bin_width) as usize;

        let x_bin = x_bin.min(nb_x_bins - 1);
        let y_bin = y_bin.min(nb_y_bins - 1);

        *histogram.entry((x_bin, y_bin)).or_insert(0.0) += 1.0;
    }

    histogram
}

// ============================================================================
// Statistical Functions
// ============================================================================

/// Calculate the median of a slice
pub fn median(values: &[f64]) -> f64 {
    if values.is_empty() {
        return 0.0;
    }

    let mut sorted = values.to_vec();
    sorted.sort_by(|a, b| a.total_cmp(b));

    let n = sorted.len();
    if n % 2 == 0 {
        (sorted[n / 2 - 1] + sorted[n / 2]) / 2.0
    } else {
        sorted[n / 2]
    }
}

/// Calculate the median absolute deviation (MAD)
pub fn mad(values: &[f64]) -> f64 {
    if values.is_empty() {
        return 0.0;
    }

    let med = median(values);
    let deviations: Vec<f64> = values.iter().map(|&v| (v - med).abs()).collect();
    median(&deviations)
}

/// Calculate robust noise threshold using MAD
pub fn robust_noise_threshold(values: &[f64], k: f64) -> f64 {
    let med = median(values);
    let m = mad(values);
    med + k * m
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ternary_slopes() {
        let values = vec![1.0, 2.0, 3.0, 2.0, 1.0];
        let slopes = calc_ternary_slopes(&values, 1);

        assert_eq!(slopes.len(), 4);
        assert_eq!(slopes[0], 1.0); // Rising
        assert_eq!(slopes[1], 1.0); // Rising
        assert_eq!(slopes[2], -1.0); // Falling
        assert_eq!(slopes[3], -1.0); // Falling
    }

    #[test]
    fn test_ternary_slopes_second_derivative() {
        let values = vec![1.0, 2.0, 3.0, 2.0, 1.0];
        let slopes = calc_ternary_slopes(&values, 2);

        // Second derivative should be shorter
        assert_eq!(slopes.len(), 3);
    }

    #[test]
    fn test_find_local_extrema() {
        let values = vec![10.0, 50.0, 100.0, 50.0, 20.0, 60.0, 30.0];
        let extrema = find_local_extrema(&values);

        assert!(!extrema.is_empty());

        // Check alternation
        for i in 1..extrema.len() {
            assert_ne!(extrema[i].is_minimum(), extrema[i - 1].is_minimum());
        }
    }

    #[test]
    fn test_histogram() {
        let values = vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0];
        let bins = compute_histogram(&values, 5);

        assert_eq!(bins.len(), 5);

        // Total count should equal input length
        let total_count: usize = bins.iter().map(|b| b.count).sum();
        assert_eq!(total_count, values.len());
    }

    #[test]
    fn test_median() {
        assert_eq!(median(&[1.0, 2.0, 3.0, 4.0, 5.0]), 3.0);
        assert_eq!(median(&[1.0, 2.0, 3.0, 4.0]), 2.5);
        assert_eq!(median(&[5.0]), 5.0);
    }

    #[test]
    fn test_mad() {
        let values = vec![1.0, 2.0, 3.0, 4.0, 5.0];
        let m = mad(&values);
        assert!((m - 1.0).abs() < 1e-10);
    }

    #[test]
    fn test_robust_noise_threshold() {
        let values = vec![1.0, 2.0, 3.0, 100.0, 4.0, 5.0]; // 100 is an outlier
        let threshold = robust_noise_threshold(&values, 3.0);

        // Threshold should be somewhere reasonable
        assert!(threshold > median(&values));
        assert!(threshold < 100.0);
    }
}
