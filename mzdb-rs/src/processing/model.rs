//! Core data models for LC-MS processing
//!
//! This module contains the fundamental data structures used throughout
//! the mzdb-processing library.

use serde::{Deserialize, Serialize};
use smallvec::SmallVec;
use std::sync::atomic::{AtomicI64, Ordering};
use anyhow::Context;
use anyhow_ext::*;

// ============================================================================
// ID Generation
// ============================================================================

static PEAKEL_ID_COUNTER: AtomicI64 = AtomicI64::new(1);
static FEATURE_ID_COUNTER: AtomicI64 = AtomicI64::new(1);

/// Generate a new unique peakel ID
pub fn generate_peakel_id() -> i64 {
    PEAKEL_ID_COUNTER.fetch_add(1, Ordering::SeqCst)
}

/// Generate a new unique feature ID
pub fn generate_feature_id() -> i64 {
    FEATURE_ID_COUNTER.fetch_add(1, Ordering::SeqCst)
}

// ============================================================================
// HasPeakelData Trait
// ============================================================================

/// Trait for types that contain peakel data arrays.
/// 
/// This trait provides a common interface for accessing the raw peaks data
/// (spectrum IDs, elution times, m/z values, intensities) from various
/// peakel representations:
/// - `Peakel`: Core processing type
/// - `ExtendedPeakel`: Database record type (in peakeldb module)
/// 
/// # Example
/// 
/// ```ignore
/// fn print_peakel_info<T: HasPeakelData>(peakel: &T) {
///     println!("Points: {}, m/z range: {:.2}-{:.2}", 
///         peakel.len(), peakel.min_mz(), peakel.max_mz());
/// }
/// ```
pub trait HasPeakelData {
    /// Get spectrum IDs slice
    fn spectrum_ids(&self) -> &[i64];
    /// Get elution times slice (seconds)
    fn elution_times(&self) -> &[f32];
    /// Get m/z values slice (32-bit for centroid data)
    fn mz_values(&self) -> &[f32];
    /// Get intensity values slice
    fn intensity_values(&self) -> &[f32];
    /// Get the index of the apex (highest intensity) data point
    fn apex_index(&self) -> Option<usize>;

    /// Get the number of data points
    #[inline]
    fn len(&self) -> usize {
        self.spectrum_ids().len()
    }

    /// Check if empty
    #[inline]
    fn is_empty(&self) -> bool {
        self.spectrum_ids().is_empty()
    }

    /// Find the index of a specific spectrum ID (uses binary search)
    fn find_spectrum_index(&self, spectrum_id: i64) -> Option<usize> {
        self.spectrum_ids().binary_search(&spectrum_id).ok()
    }

    /// Get min m/z value
    fn min_mz(&self) -> f32 {
        self.mz_values().iter().cloned().fold(f32::INFINITY, f32::min)
    }

    /// Get max m/z value
    fn max_mz(&self) -> f32 {
        self.mz_values().iter().cloned().fold(f32::NEG_INFINITY, f32::max)
    }

    /// Get min elution time
    fn min_time(&self) -> f32 {
        self.elution_times().first().copied().unwrap_or(0f32)
    }

    /// Get max elution time
    fn max_time(&self) -> f32 {
        self.elution_times().last().copied().unwrap_or(f32::MAX)
    }

    /// Get first spectrum ID
    fn first_spectrum_id(&self) -> Option<i64> {
        self.spectrum_ids().first().copied()
    }

    /// Get last spectrum ID
    fn last_spectrum_id(&self) -> Option<i64> {
        self.spectrum_ids().last().copied()
    }

    /// Get apex intensity
    fn apex_intensity(&self) -> Option<f32> {
        self.apex_index().map(|i| self.intensity_values()[i])
    }

    /// Get apex m/z
    fn apex_mz(&self) -> Option<f32> {
        self.apex_index().map(|i| self.mz_values()[i])
    }

    /// Get apex elution time
    fn apex_elution_time(&self) -> Option<f32> {
        self.apex_index().map(|i| self.elution_times()[i])
    }

    /// Get apex spectrum ID
    fn apex_spectrum_id(&self) -> Option<i64> {
        self.apex_index().map(|i| self.spectrum_ids()[i])
    }

    /// Calculate duration (max_time - min_time)
    fn calc_duration(&self) -> f32 {
        if self.is_empty() {
            0.0
        } else {
            self.max_time() - self.min_time()
        }
    }

    /// Calculate the weighted average elution time
    fn calc_weighted_average_time(&self) -> f32 {
        let sum_intensity: f32 = self.intensity_values().iter().sum();
        if sum_intensity == 0.0 {
            return self.apex_elution_time().unwrap_or(0.0);
        }

        self
            .elution_times()
            .iter()
            .zip(self.intensity_values().iter())
            .map(|(&rt, &intensity)| rt * intensity)
            .sum::<f32>()
            / sum_intensity
    }


    /// Calculate weighted average m/z
    fn calc_weighted_mz(&self) -> f32 {
        if self.is_empty() {
            return f32::NAN;
        }

        let sum_intensity: f32 = self.intensity_values().iter().sum();
        if sum_intensity == 0.0 {
            return self.apex_mz().unwrap_or(f32::NAN);
        }

        self.mz_values()
            .iter()
            .zip(self.intensity_values().iter())
            .map(|(&mz, &intensity)| mz * intensity)
            .sum::<f32>()
            / sum_intensity
    }

    /// Calculate area using trapezoidal integration.
    /// 
    /// This matches the Scala mzdb-processing implementation:
    /// ```scala
    /// computedArea += (intensity + prevPeakIntensity) * deltaTime / 2
    /// ```
    /// 
    /// For peakels with fewer than 2 points, returns the sum of intensities.
    fn calc_area(&self) -> f32 {
        let times = self.elution_times();
        let intensities = self.intensity_values();
        
        if times.len() < 2 {
            return intensities.iter().sum();
        }
        
        let mut area = 0.0f32;
        for i in 1..times.len() {
            let delta_time = times[i] - times[i - 1];
            area += (intensities[i] + intensities[i - 1]) * delta_time / 2.0;
        }

        area
    }
    
    /// Calculate minimum positive intensity (filtering out zeros and NaN)
    /// 
    /// Returns `None` if no valid positive intensities exist.
    fn calc_min_intensity(&self) -> f32 {
        let min = self.intensity_values().iter()
            .cloned()
            .filter(|&i| i > 0.0 && !i.is_nan())
            .fold(f32::INFINITY, f32::min);
        
        if min > 0.0 && min < f32::INFINITY {
            min
        } else {
            0.0
        }
    }
    
    /// Calculate amplitude (apex intensity / min positive intensity)
    /// 
    /// Matches Scala: `getApexIntensity / intensityValues.filter(i => i > 0 && !i.isNaN).min`
    fn calc_amplitude(&self) -> f32 {
        match self.calc_min_intensity() {
            0f32 => f32::NAN,
            min_intensity => self.apex_intensity().unwrap_or(0.0) / min_intensity,
        }
    }
    
    /// Calculate intensity coefficient of variation (CV) as percentage.
    /// 
    /// CV = 100 * standard_deviation / mean
    /// 
    /// Matches Scala implementation:
    /// ```scala
    /// def calcCv(values: Array[Double], mean: Double): Float = {
    ///     val variance = StatUtils.variance(values, mean)
    ///     val sd = math.sqrt(variance)
    ///     val cv = if (sd > 0) 100 * sd / mean else 0f
    ///     cv.toFloat
    /// }
    /// ```
    fn calc_intensity_cv(&self) -> f32 {
        let intensities = self.intensity_values();
        if intensities.is_empty() {
            return 0.0;
        }
        
        let n = intensities.len() as f32;
        let mean: f32 = intensities.iter().sum::<f32>() / n;
        
        if mean == 0.0 {
            return 0.0;
        }
        
        // Calculate variance (population variance to match Apache Commons StatUtils.variance with known mean)
        // Note: StatUtils.variance(values, mean) computes sum((x - mean)^2) / (n - 1) when mean is provided
        let variance: f32 = intensities.iter()
            .map(|&x| {
                let diff = x - mean;
                diff * diff
            })
            .sum::<f32>() / (n - 1.0).max(1.0);
        
        let sd = variance.sqrt();
        
        if sd > 0.0 {
            100.0 * sd / mean
        } else {
            0.0
        }
    }
}

// ============================================================================
// Peak
// ============================================================================

/// A single mass spectrometry peak with LC context
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Peak {
    /// m/z value (32-bit for centroid data)
    pub mz: f32,
    /// Intensity value
    pub intensity: f32,
    /// Left half-width at half-maximum
    pub left_hwhm: f32,
    /// Right half-width at half-maximum
    pub right_hwhm: f32,
    /// LC context (spectrum ID and elution time)
    pub lc_context: Option<LcContext>,
}

impl Peak {
    /// Create a new peak
    pub fn new(mz: f32, intensity: f32) -> Self {
        Self {
            mz,
            intensity,
            left_hwhm: 0.0,
            right_hwhm: 0.0,
            lc_context: None,
        }
    }

    /// Create a new peak with all fields
    pub fn with_hwhm(
        mz: f32,
        intensity: f32,
        left_hwhm: f32,
        right_hwhm: f32,
        lc_context: Option<LcContext>,
    ) -> Self {
        Self {
            mz,
            intensity,
            left_hwhm,
            right_hwhm,
            lc_context,
        }
    }

    /// Get the elution time (if LC context is available)
    pub fn elution_time(&self) -> Option<f32> {
        self.lc_context.as_ref().map(|ctx| ctx.elution_time)
    }

    /// Get the spectrum ID (if LC context is available)
    pub fn spectrum_id(&self) -> Option<i64> {
        self.lc_context.as_ref().map(|ctx| ctx.spectrum_id)
    }
}

// ============================================================================
// LC Context
// ============================================================================

/// Liquid chromatography context for a peak
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct LcContext {
    /// Spectrum identifier
    pub spectrum_id: i64,
    /// Elution time in seconds
    pub elution_time: f32,
}

impl LcContext {
    /// Create a new LC context
    pub fn new(spectrum_id: i64, elution_time: f32) -> Self {
        Self {
            spectrum_id,
            elution_time,
        }
    }
}

// ============================================================================
// Time-Intensity Pair
// ============================================================================

/// A simple time-intensity pair for XIC data
pub type RtIntensityPair = (f32, f32);

/// Collection of RT-intensity pairs
pub type RtIntensityPairs = Vec<RtIntensityPair>;

// ============================================================================
// Peakel (Chromatographic Peak)
// ============================================================================

/// A peakel is a chromatographic peak - a series of peaks across spectra
/// representing the elution of a single analyte.
///
/// Uses SmallVec to store up to 16 points on the stack, spilling to heap
/// for larger peakels. Most peakels have fewer than 16 data points.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Peakel {
    /// Unique identifier
    pub id: i64,
    /// Spectrum IDs for each data point
    pub spectrum_ids: SmallVec<[i64; 16]>,
    /// Elution times for each data point
    pub elution_times: SmallVec<[f32; 16]>,
    /// m/z values for each data point (32-bit for centroid data)
    pub mz_values: SmallVec<[f32; 16]>,
    /// Intensity values for each data point
    pub intensity_values: SmallVec<[f32; 16]>,
    /// Left HWHM values (optional)
    pub left_hwhms: Option<SmallVec<[f32; 16]>>,
    /// Right HWHM values (optional)
    pub right_hwhms: Option<SmallVec<[f32; 16]>>,
    /// Index of the apex (most intense point)
    apex_index: usize,
    /// Number of gaps (missing spectra) in the peakel
    pub gap_count: usize,
}

impl Peakel {
    /// Create a new peakel from SmallVec data
    ///
    /// This is the primary constructor. For peakels with 16 or fewer points,
    /// data stays on the stack avoiding heap allocation.
    pub fn new(
        spectrum_ids: SmallVec<[i64; 16]>,
        elution_times: SmallVec<[f32; 16]>,
        mz_values: SmallVec<[f32; 16]>,
        intensity_values: SmallVec<[f32; 16]>,
        left_hwhms: Option<SmallVec<[f32; 16]>>,
        right_hwhms: Option<SmallVec<[f32; 16]>>,
        apex_index: usize,
        gap_count: usize,
    ) -> Result<Self> {
        if spectrum_ids.is_empty() {
            bail!("can't create a Peakel with empty spectrum_ids");
        }

        Ok(Self {
            id: generate_peakel_id(),
            spectrum_ids,
            elution_times,
            mz_values,
            intensity_values,
            left_hwhms,
            right_hwhms,
            apex_index,
            gap_count,
        })
    }

    /// Create a new peakel from Vec data
    ///
    /// The vectors are converted to SmallVec internally.
    /// Use this for convenience when working with existing Vec data.
    pub fn from_vectors(
        spectrum_ids: Vec<i64>,
        elution_times: Vec<f32>,
        mz_values: Vec<f32>,
        intensity_values: Vec<f32>,
        left_hwhms: Option<Vec<f32>>,
        right_hwhms: Option<Vec<f32>>,
        gap_count: usize,
    ) -> Result<Self> {
        if spectrum_ids.is_empty() {
            bail!("can't create a Peakel with empty spectrum_ids");
        }

        let apex_index = Peakel::calc_apex_index(&intensity_values).context("undefined peakel apex index")?;

        Ok(Self {
            id: generate_peakel_id(),
            spectrum_ids: SmallVec::from_vec(spectrum_ids),
            elution_times: SmallVec::from_vec(elution_times),
            mz_values: SmallVec::from_vec(mz_values),
            intensity_values: SmallVec::from_vec(intensity_values),
            left_hwhms: left_hwhms.map(SmallVec::from_vec),
            right_hwhms: right_hwhms.map(SmallVec::from_vec),
            apex_index,
            gap_count,
        })
    }

    /// Get the index of the apex (highest intensity) data point
    pub fn calc_apex_index(intensities :&[f32]) -> Option<usize> {
        intensities
            .iter()
            .enumerate()
            .max_by(|(_, a), (_, b)| a.total_cmp(b))
            .map(|(i, _)| i)
    }

    /// Get the number of peaks in this peakel (alias for `len()`)
    #[inline]
    pub fn peaks_count(&self) -> usize {
        self.len()
    }

/*    /// Calculate the peakel area (alias for `calc_area()`)
    #[inline]
    pub fn area(&self) -> f32 {
        self.calc_area()
    }*/

    /// Get the mean left HWHM
    pub fn left_hwhm_mean(&self) -> f32 {
        match &self.left_hwhms {
            Some(hwhms) if !hwhms.is_empty() => hwhms.iter().sum::<f32>() / hwhms.len() as f32,
            _ => 0.0,
        }
    }

    /// Get the mean right HWHM
    pub fn right_hwhm_mean(&self) -> f32 {
        match &self.right_hwhms {
            Some(hwhms) if !hwhms.is_empty() => hwhms.iter().sum::<f32>() / hwhms.len() as f32,
            _ => 0.0,
        }
    }

    /// Get RT-intensity pairs
    pub fn elution_time_intensity_pairs(&self) -> Vec<(f32, f32)> {
        self.elution_times
            .iter()
            .zip(self.intensity_values.iter())
            .map(|(&rt, &intensity)| (rt, intensity))
            .collect()
    }
}

impl HasPeakelData for Peakel {
    fn spectrum_ids(&self) -> &[i64] {
        &self.spectrum_ids
    }

    fn elution_times(&self) -> &[f32] {
        &self.elution_times
    }

    fn mz_values(&self) -> &[f32] {
        &self.mz_values
    }

    fn intensity_values(&self) -> &[f32] {
        &self.intensity_values
    }

    // Override to use cached apex_index for better performance
    fn apex_index(&self) -> Option<usize> {
        if self.intensity_values.is_empty() {
            None
        } else {
            Some(self.apex_index)
        }
    }

    fn apex_intensity(&self) -> Option<f32> {
        if self.intensity_values.is_empty() {
            None
        } else {
            Some(self.intensity_values[self.apex_index])
        }
    }

    fn apex_mz(&self) -> Option<f32> {
        if self.mz_values.is_empty() {
            None
        } else {
            Some(self.mz_values[self.apex_index])
        }
    }

    fn apex_elution_time(&self) -> Option<f32> {
        if self.elution_times.is_empty() {
            None
        } else {
            Some(self.elution_times[self.apex_index])
        }
    }

    fn apex_spectrum_id(&self) -> Option<i64> {
        if self.spectrum_ids.is_empty() {
            None
        } else {
            Some(self.spectrum_ids[self.apex_index])
        }
    }
}

// ============================================================================
// Peakel Builder
// ============================================================================

/// Builder for constructing Peakels from individual peaks
///
/// Uses SmallVec internally to match Peakel's storage, avoiding
/// conversion overhead when building small peakels.
#[derive(Clone, Debug)]
pub struct PeakelBuilder {
    spectrum_ids: SmallVec<[i64; 16]>,
    elution_times: SmallVec<[f32; 16]>,
    mz_values: SmallVec<[f32; 16]>,
    intensity_values: SmallVec<[f32; 16]>,
    left_hwhms: SmallVec<[f32; 16]>,
    right_hwhms: SmallVec<[f32; 16]>,
    gap_count: usize,
}

impl Default for PeakelBuilder {
    fn default() -> Self {
        Self {
            spectrum_ids: SmallVec::new(),
            elution_times: SmallVec::new(),
            mz_values: SmallVec::new(),
            intensity_values: SmallVec::new(),
            left_hwhms: SmallVec::new(),
            right_hwhms: SmallVec::new(),
            gap_count: 0,
        }
    }
}

impl PeakelBuilder {
    /// Create a new peakel builder
    pub fn new() -> Self {
        Self::default()
    }

    /// Create a new peakel builder with pre-allocated capacity
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            spectrum_ids: SmallVec::with_capacity(capacity),
            elution_times: SmallVec::with_capacity(capacity),
            mz_values: SmallVec::with_capacity(capacity),
            intensity_values: SmallVec::with_capacity(capacity),
            left_hwhms: SmallVec::with_capacity(capacity),
            right_hwhms: SmallVec::with_capacity(capacity),
            gap_count: 0,
        }
    }

    /// Set the gap count for this peakel
    pub fn set_gap_count(&mut self, gap_count: usize) -> &mut Self {
        self.gap_count = gap_count;
        self
    }

    /// Add a peak to the builder
    pub fn add(&mut self, peak: &Peak) -> &mut Self {
        if let Some(ctx) = &peak.lc_context {
            self.spectrum_ids.push(ctx.spectrum_id);
            self.elution_times.push(ctx.elution_time);
        } else {
            self.spectrum_ids.push(0);
            self.elution_times.push(0.0);
        }
        self.mz_values.push(peak.mz);
        self.intensity_values.push(peak.intensity);
        self.left_hwhms.push(peak.left_hwhm);
        self.right_hwhms.push(peak.right_hwhm);
        self
    }

    /// Add a data point directly
    pub fn add_point(
        &mut self,
        spectrum_id: i64,
        elution_time: f32,
        mz: f32,
        intensity: f32,
        left_hwhm: f32,
        right_hwhm: f32,
    ) -> &mut Self {
        self.spectrum_ids.push(spectrum_id);
        self.elution_times.push(elution_time);
        self.mz_values.push(mz);
        self.intensity_values.push(intensity);
        self.left_hwhms.push(left_hwhm);
        self.right_hwhms.push(right_hwhm);
        self
    }

    /// Get the current number of peaks
    pub fn peaks_count(&self) -> usize {
        self.intensity_values.len()
    }

    /// Build the peakel
    pub fn build(self) -> Result<Peakel> {
        let has_hwhms = self.left_hwhms.iter().any(|&h| h > 0.0)
            || self.right_hwhms.iter().any(|&h| h > 0.0);

        let apex_index = Peakel::calc_apex_index(&self.intensity_values).context("undefined peakel apex index")?;

        Peakel::new(
            self.spectrum_ids,
            self.elution_times,
            self.mz_values,
            self.intensity_values,
            if has_hwhms {
                Some(self.left_hwhms)
            } else {
                None
            },
            if has_hwhms {
                Some(self.right_hwhms)
            } else {
                None
            },
            apex_index,
            self.gap_count,
        )
    }
}

impl FromIterator<Peak> for PeakelBuilder {
    fn from_iter<T: IntoIterator<Item = Peak>>(iter: T) -> Self {
        let mut builder = PeakelBuilder::new();
        for peak in iter {
            builder.add(&peak);
        }
        builder
    }
}

// ============================================================================
// Feature
// ============================================================================

/// A feature represents a detected analyte in LC-MS data
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Feature {
    /// Unique identifier
    pub id: i64,
    /// m/z value
    pub mz: f64,
    /// Charge state
    pub charge: i32,
    /// Elution time at apex
    pub elution_time: f32,
    /// Intensity at apex
    pub intensity: f32,
    /// MS level where this feature was detected
    pub evidence_ms_level: i32,
    /// Whether this is a predicted feature
    pub is_predicted: bool,
    /// Associated peakels (isotopic pattern)
    pub peakels: Vec<Peakel>,
}

impl Feature {
    /// Create a new feature
    pub fn new(
        mz: f64,
        charge: i32,
        elution_time: f32,
        intensity: f32,
        evidence_ms_level: i32,
        is_predicted: bool,
    ) -> Self {
        Self {
            id: generate_feature_id(),
            mz,
            charge,
            elution_time,
            intensity,
            evidence_ms_level,
            is_predicted,
            peakels: Vec::new(),
        }
    }

    /// Calculate the weighted average elution time
    pub fn weighted_average_time(&self) -> f32 {
        if self.peakels.is_empty() {
            return self.elution_time;
        }

        let total_intensity: f32 = self
            .peakels
            .iter()
            .flat_map(|p| p.intensity_values.iter())
            .sum();

        if total_intensity == 0.0 {
            return self.elution_time;
        }

        let weighted_sum: f32 = self
            .peakels
            .iter()
            .flat_map(|p| p.elution_times.iter().zip(p.intensity_values.iter()))
            .map(|(&rt, &intensity)| rt * intensity)
            .sum();

        weighted_sum / total_intensity
    }
}

// ============================================================================
// Putative Feature (for targeted extraction)
// ============================================================================

/// A putative feature for targeted extraction
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct PutativeFeature {
    /// Unique identifier
    pub id: i64,
    /// Target m/z value
    pub mz: f64,
    /// Expected charge state
    pub charge: i32,
    /// Predicted elution time
    pub elution_time: f32,
    /// MS level for evidence
    pub evidence_ms_level: i32,
    /// Whether this is a predicted feature
    pub is_predicted: bool,
}

impl PutativeFeature {
    /// Create a new putative feature
    pub fn new(mz: f64, charge: i32, elution_time: f32, evidence_ms_level: i32) -> Self {
        Self {
            id: generate_feature_id(),
            mz,
            charge,
            elution_time,
            evidence_ms_level,
            is_predicted: true,
        }
    }
}

// ============================================================================
// XIC Peak (for extracted ion chromatograms)
// ============================================================================

// Re-export XicPeak from the main model module to avoid duplication
pub use crate::model::XicPeak;

// ============================================================================
// Detected Peak (result of peak detection on XIC)
// ============================================================================

/// A detected peak from XIC analysis
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct DetectedPeak {
    /// m/z value
    pub mz: f64,
    /// Apex peak
    pub apex_intensity: f32,
    /// Apex retention time
    pub apex_time: f32,
    /// Duration of the peak
    pub duration: f32,
    /// Area under the curve
    pub area: f32,
}

impl DetectedPeak {
    /// Create a new detected peak
    pub fn new(mz: f64, apex_intensity: f32, apex_time: f32, duration: f32, area: f32) -> Self {
        Self {
            mz,
            apex_intensity,
            apex_time,
            duration,
            area,
        }
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_peak_creation() {
        let peak = Peak::new(500.0, 1000.0);
        assert_eq!(peak.mz, 500.0);
        assert_eq!(peak.intensity, 1000.0);
        assert!(peak.lc_context.is_none());
    }

    #[test]
    fn test_peakel_creation() -> Result<()>{
        let peakel = Peakel::from_vectors(
            vec![1, 2, 3],
            vec![1.0, 2.0, 3.0],
            vec![500.0, 500.1, 500.2],
            vec![100.0, 200.0, 150.0],
            None,
            None,
            0,
        )?;

        assert_eq!(peakel.peaks_count(), 3);
        assert_eq!(peakel.apex_index(), Some(1));
        assert_eq!(peakel.apex_intensity(), Some(200.0));
        assert_eq!(peakel.apex_elution_time(), Some(2.0));
        assert_eq!(peakel.gap_count, 0);

        Ok(())
    }

    #[test]
    fn test_peakel_with_gaps() -> Result<()> {
        let peakel = Peakel::from_vectors(
            vec![1, 3, 5],  // spectrum IDs with gaps (2 and 4 missing)
            vec![1.0, 3.0, 5.0],
            vec![500.0, 500.1, 500.2],
            vec![100.0, 200.0, 150.0],
            None,
            None,
            2,  // 2 gaps
        )?;

        assert_eq!(peakel.peaks_count(), 3);
        assert_eq!(peakel.gap_count, 2);

        Ok(())
    }

    #[test]
    fn test_peakel_builder() -> Result<()> {
        let mut builder = PeakelBuilder::new();
        builder.add(&Peak::with_hwhm(
            500.0,
            100.0,
            0.0,
            0.0,
            Some(LcContext::new(1, 1.0)),
        ));
        builder.add(&Peak::with_hwhm(
            500.1,
            200.0,
            0.0,
            0.0,
            Some(LcContext::new(2, 2.0)),
        ));
        builder.add(&Peak::with_hwhm(
            500.2,
            150.0,
            0.0,
            0.0,
            Some(LcContext::new(3, 3.0)),
        ));

        let peakel = builder.build()?;
        assert_eq!(peakel.peaks_count(), 3);
        assert_eq!(peakel.apex_intensity(), Some(200.0));
        assert_eq!(peakel.gap_count, 0);

        Ok(())
    }

    #[test]
    fn test_peakel_builder_with_gaps() -> Result<()> {
        let mut builder = PeakelBuilder::new();
        builder.set_gap_count(3);
        builder.add(&Peak::with_hwhm(
            500.0,
            100.0,
            0.0,
            0.0,
            Some(LcContext::new(1, 1.0)),
        ));

        let peakel = builder.build()?;
        assert_eq!(peakel.gap_count, 3);

        Ok(())
    }

    #[test]
    fn test_peakel_calculations() -> Result<()> {
        let peakel = Peakel::from_vectors(
            vec![1, 2, 3],
            vec![1.0, 2.0, 3.0],
            vec![500.0, 500.0, 500.0],
            vec![100.0, 200.0, 100.0],
            None,
            None,
            0,
        )?;

        assert_eq!(peakel.calc_duration(), 2.0);
        assert_eq!(peakel.calc_area(), 300.0);

        Ok(())
    }
    
    #[test]
    fn test_intensity_cv() -> Result<()> {
        // Test with known values
        // Values: [100, 200, 300] -> mean = 200, variance = 10000, sd = 100, cv = 50%
        let peakel = Peakel::from_vectors(
            vec![1, 2, 3],
            vec![1.0, 2.0, 3.0],
            vec![500.0, 500.0, 500.0],
            vec![100.0, 200.0, 300.0],
            None,
            None,
            0,
        )?;
        
        let cv = peakel.calc_intensity_cv();
        // With sample variance (n-1): variance = ((100-200)^2 + (200-200)^2 + (300-200)^2) / 2 = 10000
        // sd = 100, mean = 200, cv = 100 * 100 / 200 = 50%
        assert!((cv - 50.0).abs() < 0.01, "Expected CV ~50%, got {}", cv);
        
        // Test with uniform values (CV should be 0)
        let uniform_peakel = Peakel::from_vectors(
            vec![1, 2, 3],
            vec![1.0, 2.0, 3.0],
            vec![500.0, 500.0, 500.0],
            vec![100.0, 100.0, 100.0],
            None,
            None,
            0,
        )?;
        
        let uniform_cv = uniform_peakel.calc_intensity_cv();
        assert_eq!(uniform_cv, 0.0, "Expected CV 0% for uniform values, got {}", uniform_cv);

        Ok(())
    }
}