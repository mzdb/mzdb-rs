//! Signal filtering algorithms
//!
//! This module provides signal smoothing and filtering algorithms including:
//! - Savitzky-Golay smoothing
//! - Partial Savitzky-Golay smoothing (smooths only noisy regions)
//! - Baseline removal

use nalgebra::{DMatrix, DVector};

// ============================================================================
// Smoothing Trait
// ============================================================================

/// Trait for signal smoothers
pub trait SignalSmoother {
    /// Smooth time-intensity pairs
    fn smooth_time_intensity_pairs(&self, data: &[(f32, f64)]) -> Vec<(f32, f64)>;
}

// ============================================================================
// Savitzky-Golay Smoother
// ============================================================================

/// Configuration for Savitzky-Golay smoothing
#[derive(Clone, Debug)]
pub struct SavitzkyGolaySmoothingConfig {
    /// Number of points on each side (filter half-width)
    pub nb_points: usize,
    /// Polynomial order
    pub poly_order: usize,
    /// Number of smoothing iterations
    pub iteration_count: usize,
}

impl Default for SavitzkyGolaySmoothingConfig {
    fn default() -> Self {
        Self {
            nb_points: 5,
            poly_order: 4,
            iteration_count: 1,
        }
    }
}

/// Savitzky-Golay signal smoother
///
/// Implements the Savitzky-Golay filter for signal smoothing using
/// polynomial fitting within a moving window.
#[derive(Clone, Debug)]
pub struct SavitzkyGolaySmoother {
    config: SavitzkyGolaySmoothingConfig,
    coeffs: Vec<f64>,
}

impl SavitzkyGolaySmoother {
    /// Create a new Savitzky-Golay smoother with default configuration
    pub fn new(nb_points: usize, poly_order: usize, iteration_count: usize) -> Self {
        let config = SavitzkyGolaySmoothingConfig {
            nb_points,
            poly_order,
            iteration_count,
        };
        let coeffs = compute_sg_coefficients(nb_points, nb_points, poly_order);
        Self { config, coeffs }
    }

    /// Create from a configuration
    pub fn from_config(config: SavitzkyGolaySmoothingConfig) -> Self {
        let coeffs = compute_sg_coefficients(config.nb_points, config.nb_points, config.poly_order);
        Self { config, coeffs }
    }

    /// Smooth a vector of intensity values
    pub fn smooth(&self, data: &[f64]) -> Vec<f64> {
        let mut result = data.to_vec();
        for _ in 0..self.config.iteration_count {
            result = self.smooth_once(&result);
        }
        result
    }

    fn smooth_once(&self, data: &[f64]) -> Vec<f64> {
        let data_len = data.len();
        let nl = self.config.nb_points;
        let nr = self.config.nb_points;

        if data_len == 0 {
            return vec![];
        }

        // Create zero-padded data array (matching Java SGFilterMath3 behavior)
        // When no leftPad/rightPad is provided, the filter uses zeros
        let padded_len = data_len + nl + nr;
        let mut padded_data = vec![0.0; padded_len];
        
        // Copy actual data into the middle (left pad is zeros, right pad is zeros)
        for i in 0..data_len {
            padded_data[nl + i] = data[i];
        }

        // Convolution with Savitzky-Golay coefficients
        let mut result = vec![0.0; data_len];
        
        for x in nl..(padded_len - nr) {
            let mut sum = 0.0;
            for i in 0..self.coeffs.len() {
                let idx = x + i - nl;
                sum += padded_data[idx] * self.coeffs[i];
            }
            result[x - nl] = sum;
        }

        result
    }
}

impl SignalSmoother for SavitzkyGolaySmoother {
    fn smooth_time_intensity_pairs(&self, data: &[(f32, f64)]) -> Vec<(f32, f64)> {
        let intensities: Vec<f64> = data.iter().map(|&(_, i)| i).collect();
        let smoothed = self.smooth(&intensities);

        data.iter()
            .zip(smoothed.iter())
            .map(|(&(rt, _), &smoothed_i)| (rt, smoothed_i))
            .collect()
    }
}

/// Compute Savitzky-Golay coefficients
///
/// This function computes the convolution coefficients for the Savitzky-Golay
/// filter using least squares polynomial fitting.
pub fn compute_sg_coefficients(nl: usize, nr: usize, degree: usize) -> Vec<f64> {
    let m = nl + nr + 1;

    if m <= degree {
        return vec![1.0 / m as f64; m];
    }

    // Build the Vandermonde-like matrix
    let mut a = DMatrix::<f64>::zeros(degree + 1, degree + 1);

    for i in 0..=degree {
        for j in 0..=degree {
            let mut sum = if i == 0 && j == 0 { 1.0 } else { 0.0 };
            for k in 1..=nr {
                sum += (k as f64).powi((i + j) as i32);
            }
            for k in 1..=nl {
                sum += (-(k as f64)).powi((i + j) as i32);
            }
            a[(i, j)] = sum;
        }
    }

    // Right-hand side
    let mut b = DVector::<f64>::zeros(degree + 1);
    b[0] = 1.0;

    // Solve the system
    let decomp = a.lu();
    let solution = match decomp.solve(&b) {
        Some(s) => s,
        None => return vec![1.0 / m as f64; m],
    };

    // Compute coefficients
    let mut coeffs = vec![0.0; m];
    for n in -(nl as isize)..=(nr as isize) {
        let mut sum = solution[0];
        for k in 1..=degree {
            sum += solution[k] * (n as f64).powi(k as i32);
        }
        coeffs[(n + nl as isize) as usize] = sum;
    }

    coeffs
}

// ============================================================================
// Partial Savitzky-Golay Smoother
// ============================================================================

/// Partial Savitzky-Golay smoother that only smooths noisy regions
///
/// This smoother detects noisy regions in the signal using derivative analysis
/// and only applies Savitzky-Golay smoothing to those regions, preserving
/// the original signal in smooth regions.
#[derive(Clone, Debug)]
pub struct PartialSavitzkyGolaySmoother {
    sg_smoother: SavitzkyGolaySmoother,
    padding_offset: usize,
}

impl PartialSavitzkyGolaySmoother {
    /// Create a new partial smoother
    pub fn new(config: SavitzkyGolaySmoothingConfig) -> Self {
        Self {
            sg_smoother: SavitzkyGolaySmoother::from_config(config),
            padding_offset: 2,
        }
    }
}

impl SignalSmoother for PartialSavitzkyGolaySmoother {
    fn smooth_time_intensity_pairs(&self, data: &[(f32, f64)]) -> Vec<(f32, f64)> {
        let intensities: Vec<f64> = data.iter().map(|&(_, i)| i).collect();
        let n = intensities.len();

        if n < 3 {
            return data.to_vec();
        }

        // Calculate ternary slopes (second derivative)
        let slopes = calc_ternary_slopes(&intensities, 2);

        // Detect noisy parts (non-zero slopes)
        let non_zero_indices: Vec<usize> = slopes
            .iter()
            .enumerate()
            .filter(|&(_, ref s)| **s != 0.0)
            .map(|(i, _)| i + 2) // Shift by 2 due to second derivative
            .collect();

        if non_zero_indices.len() < 2 {
            return data.to_vec();
        }

        // Find noisy region boundaries
        let mut noisy_regions: Vec<(usize, usize)> = Vec::new();
        let mut region_start = non_zero_indices[0];

        for window in non_zero_indices.windows(2) {
            if window[1] - window[0] > 1 {
                noisy_regions.push((region_start.saturating_sub(1), window[0]));
                region_start = window[1];
            }
        }
        noisy_regions.push((
            region_start.saturating_sub(1),
            *non_zero_indices.last().unwrap(),
        ));

        // Smooth only the noisy regions
        let mut result = data.to_vec();

        for (start, end) in noisy_regions {
            if end - start <= 1 {
                continue;
            }

            let extended_start = start.saturating_sub(self.padding_offset);
            let extended_end = (end + self.padding_offset).min(n - 1);

            let extended_region: Vec<(f32, f64)> =
                data[extended_start..=extended_end].to_vec();
            let smoothed_region = self.sg_smoother.smooth_time_intensity_pairs(&extended_region);

            // Copy smoothed values back (only the non-extended part)
            let offset = start - extended_start;
            for (i, idx) in (start..=end).enumerate() {
                result[idx] = smoothed_region[i + offset];
            }
        }

        result
    }
}

/// Calculate ternary slopes (returns -1, 0, or +1)
fn calc_ternary_slopes(values: &[f64], derivative_level: usize) -> Vec<f64> {
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

// ============================================================================
// Baseline Remover
// ============================================================================

/// Baseline remover for noise filtering
///
/// Removes peaks below a noise threshold, identifying contiguous regions
/// of signal above the baseline.
#[derive(Clone, Debug)]
pub struct BaselineRemover {
    /// Tolerance for gaps in peak detection
    pub gap_tolerance: usize,
}

impl BaselineRemover {
    /// Create a new baseline remover
    pub fn new(gap_tolerance: usize) -> Self {
        Self { gap_tolerance }
    }

    /// Calculate the noise threshold for a signal
    ///
    /// Uses median absolute deviation (MAD) based estimation
    pub fn calc_noise_threshold(&self, data: &[(f32, f64)]) -> f64 {
        if data.is_empty() {
            return 0.0;
        }

        let intensities: Vec<f64> = data.iter().map(|&(_, i)| i).collect();

        // Calculate median
        let mut sorted = intensities.clone();
        sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        let median = if sorted.len() % 2 == 0 {
            (sorted[sorted.len() / 2 - 1] + sorted[sorted.len() / 2]) / 2.0
        } else {
            sorted[sorted.len() / 2]
        };

        // Calculate MAD
        let mut deviations: Vec<f64> = intensities.iter().map(|&i| (i - median).abs()).collect();
        deviations.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        let mad = if deviations.len() % 2 == 0 {
            (deviations[deviations.len() / 2 - 1] + deviations[deviations.len() / 2]) / 2.0
        } else {
            deviations[deviations.len() / 2]
        };

        // Return threshold as median + k * MAD (k = 3 is common)
        median + 3.0 * mad
    }

    /// Find indices of peak groups above noise threshold
    ///
    /// Returns pairs of (start_index, end_index) for each detected peak group
    pub fn find_noise_free_peak_groups_indices(
        &self,
        data: &[(f32, f64)],
        threshold: f64,
    ) -> Vec<(usize, usize)> {
        if data.is_empty() {
            return vec![];
        }

        let mut groups: Vec<(usize, usize)> = Vec::new();
        let mut in_peak = false;
        let mut group_start = 0;
        let mut gap_count = 0;

        for (i, &(_, intensity)) in data.iter().enumerate() {
            if intensity >= threshold {
                if !in_peak {
                    group_start = i;
                    in_peak = true;
                }
                gap_count = 0;
            } else if in_peak {
                gap_count += 1;
                if gap_count > self.gap_tolerance {
                    // End of peak group
                    groups.push((group_start, i - gap_count));
                    in_peak = false;
                    gap_count = 0;
                }
            }
        }

        // Handle peak at end of data
        if in_peak {
            groups.push((group_start, data.len() - 1 - gap_count.min(data.len() - 1 - group_start)));
        }

        groups
    }
}

// ============================================================================
// XIC Binner
// ============================================================================

/// Configuration for XIC binning
#[derive(Clone, Debug)]
pub struct XicBinnerConfig {
    /// Expected number of data points per bin
    pub expected_bin_data_points_count: usize,
}

impl Default for XicBinnerConfig {
    fn default() -> Self {
        Self {
            expected_bin_data_points_count: 5,
        }
    }
}

/// A bin with statistical information
#[derive(Clone, Debug)]
pub struct ExtendedBin {
    pub bin: Bin,
    pub sum: f64,
    pub count: usize,
}

impl ExtendedBin {
    pub fn new(lower: f64, upper: f64) -> Self {
        Self {
            bin: Bin::new(lower, upper),
            sum: 0.0,
            count: 0,
        }
    }

    pub fn add(&mut self, value: f64) {
        self.sum += value;
        self.count += 1;
    }
}

/// A simple bin with lower and upper bounds
#[derive(Clone, Debug)]
pub struct Bin {
    pub lower_bound: f64,
    pub upper_bound: f64,
}

impl Bin {
    pub fn new(lower_bound: f64, upper_bound: f64) -> Self {
        Self {
            lower_bound,
            upper_bound,
        }
    }

    pub fn center(&self) -> f64 {
        (self.lower_bound + self.upper_bound) / 2.0
    }

    pub fn width(&self) -> f64 {
        self.upper_bound - self.lower_bound
    }

    pub fn contains(&self, value: f64) -> bool {
        value >= self.lower_bound && value < self.upper_bound
    }
}

/// XIC binner for grouping data points
#[derive(Clone, Debug)]
pub struct XicBinner {
    config: XicBinnerConfig,
}

impl XicBinner {
    /// Create a new XIC binner
    pub fn new(config: XicBinnerConfig) -> Self {
        Self { config }
    }

    /// Calculate bins for the given data
    pub fn calc_bins(&self, data: &[(f32, f64)]) -> Vec<ExtendedBin> {
        if data.is_empty() {
            return vec![];
        }

        let min_rt = data.iter().map(|&(rt, _)| rt).fold(f32::INFINITY, f32::min);
        let max_rt = data
            .iter()
            .map(|&(rt, _)| rt)
            .fold(f32::NEG_INFINITY, f32::max);

        let range = max_rt - min_rt;
        if range <= 0.0 {
            return vec![];
        }

        let nb_bins = (data.len() / self.config.expected_bin_data_points_count).max(1);
        let bin_width = range / nb_bins as f32;

        let mut bins: Vec<ExtendedBin> = (0..nb_bins)
            .map(|i| {
                let lower = min_rt as f64 + (i as f64 * bin_width as f64);
                let upper = lower + bin_width as f64;
                ExtendedBin::new(lower, upper)
            })
            .collect();

        // Assign data points to bins
        for &(rt, intensity) in data {
            let bin_idx = ((rt - min_rt) / bin_width) as usize;
            let bin_idx = bin_idx.min(bins.len() - 1);
            bins[bin_idx].add(intensity);
        }

        bins
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sg_coefficients() {
        let coeffs = compute_sg_coefficients(2, 2, 2);
        assert_eq!(coeffs.len(), 5);
        // Sum should be approximately 1
        let sum: f64 = coeffs.iter().sum();
        assert!((sum - 1.0).abs() < 1e-10);
    }

    #[test]
    fn test_sg_smoother() {
        let smoother = SavitzkyGolaySmoother::new(2, 2, 1);
        let data = vec![1.0, 2.0, 3.0, 2.0, 1.0, 2.0, 3.0, 2.0, 1.0];
        let smoothed = smoother.smooth(&data);
        assert_eq!(smoothed.len(), data.len());
    }

    #[test]
    fn test_sg_smoother_rt_pairs() {
        let smoother = SavitzkyGolaySmoother::new(2, 2, 1);
        let data: Vec<(f32, f64)> = vec![
            (1.0, 100.0),
            (2.0, 200.0),
            (3.0, 500.0),
            (4.0, 400.0),
            (5.0, 100.0),
            (6.0, 150.0),
            (7.0, 200.0),
        ];
        let smoothed = smoother.smooth_time_intensity_pairs(&data);
        assert_eq!(smoothed.len(), data.len());

        // Check that RT values are preserved
        for (orig, smooth) in data.iter().zip(smoothed.iter()) {
            assert_eq!(orig.0, smooth.0);
        }
    }

    #[test]
    fn test_baseline_remover() {
        let remover = BaselineRemover::new(1);
        let data: Vec<(f32, f64)> = vec![
            (1.0, 10.0),
            (2.0, 20.0),
            (3.0, 100.0),
            (4.0, 150.0),
            (5.0, 80.0),
            (6.0, 15.0),
            (7.0, 10.0),
        ];

        let threshold = remover.calc_noise_threshold(&data);
        assert!(threshold > 0.0);

        let groups = remover.find_noise_free_peak_groups_indices(&data, 50.0);
        assert!(!groups.is_empty());
    }

    #[test]
    fn test_xic_binner() {
        let binner = XicBinner::new(XicBinnerConfig {
            expected_bin_data_points_count: 2,
        });
        let data: Vec<(f32, f64)> = vec![
            (1.0, 100.0),
            (2.0, 200.0),
            (3.0, 300.0),
            (4.0, 200.0),
            (5.0, 100.0),
        ];

        let bins = binner.calc_bins(&data);
        assert!(!bins.is_empty());
    }

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
}
