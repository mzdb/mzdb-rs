//! Core data models for LC-MS processing
//!
//! This module contains the fundamental data structures used throughout
//! the mzdb-processing library.

use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicI64, Ordering};

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
// Peak
// ============================================================================

/// A single mass spectrometry peak with LC context
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Peak {
    /// m/z value
    pub mz: f64,
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
    pub fn new(mz: f64, intensity: f32) -> Self {
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
        mz: f64,
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
pub type RtIntensityPair = (f32, f64);

/// Collection of RT-intensity pairs
pub type RtIntensityPairs = Vec<RtIntensityPair>;

// ============================================================================
// Peakel (Chromatographic Peak)
// ============================================================================

/// A peakel is a chromatographic peak - a series of peaks across spectra
/// representing the elution of a single analyte.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Peakel {
    /// Unique identifier
    pub id: i64,
    /// Spectrum IDs for each data point
    pub spectrum_ids: Vec<i64>,
    /// Elution times for each data point
    pub elution_times: Vec<f32>,
    /// m/z values for each data point
    pub mz_values: Vec<f64>,
    /// Intensity values for each data point
    pub intensity_values: Vec<f32>,
    /// Left HWHM values (optional)
    pub left_hwhms: Option<Vec<f64>>,
    /// Right HWHM values (optional)
    pub right_hwhms: Option<Vec<f64>>,
    /// Index of the apex (most intense point)
    apex_index: usize,
}

impl Peakel {
    /// Create a new peakel from data vectors
    pub fn new(
        spectrum_ids: Vec<i64>,
        elution_times: Vec<f32>,
        mz_values: Vec<f64>,
        intensity_values: Vec<f32>,
        left_hwhms: Option<Vec<f64>>,
        right_hwhms: Option<Vec<f64>>,
    ) -> Self {
        let apex_index = intensity_values
            .iter()
            .enumerate()
            .max_by(|(_, a), (_, b)| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal))
            .map(|(i, _)| i)
            .unwrap_or(0);

        Self {
            id: generate_peakel_id(),
            spectrum_ids,
            elution_times,
            mz_values,
            intensity_values,
            left_hwhms,
            right_hwhms,
            apex_index,
        }
    }

    /// Get the number of peaks in this peakel
    pub fn peaks_count(&self) -> usize {
        self.intensity_values.len()
    }

    /// Get the apex index
    pub fn apex_index(&self) -> usize {
        self.apex_index
    }

    /// Get the apex m/z
    pub fn apex_mz(&self) -> f64 {
        self.mz_values[self.apex_index]
    }

    /// Get the apex intensity
    pub fn apex_intensity(&self) -> f32 {
        self.intensity_values[self.apex_index]
    }

    /// Get the apex elution time
    pub fn apex_elution_time(&self) -> f32 {
        self.elution_times[self.apex_index]
    }

    /// Get the apex spectrum ID
    pub fn apex_spectrum_id(&self) -> i64 {
        self.spectrum_ids[self.apex_index]
    }

    /// Calculate the weighted average m/z
    pub fn calc_mz(&self) -> f64 {
        let sum_intensity: f64 = self.intensity_values.iter().map(|&i| i as f64).sum();
        if sum_intensity == 0.0 {
            return self.apex_mz();
        }

        self.mz_values
            .iter()
            .zip(self.intensity_values.iter())
            .map(|(&mz, &intensity)| mz * intensity as f64)
            .sum::<f64>()
            / sum_intensity
    }

    /// Calculate the weighted average elution time
    pub fn calc_weighted_average_time(&self) -> f32 {
        let sum_intensity: f64 = self.intensity_values.iter().map(|&i| i as f64).sum();
        if sum_intensity == 0.0 {
            return self.apex_elution_time();
        }

        (self
            .elution_times
            .iter()
            .zip(self.intensity_values.iter())
            .map(|(&rt, &intensity)| rt as f64 * intensity as f64)
            .sum::<f64>()
            / sum_intensity) as f32
    }

    /// Calculate the peakel duration
    pub fn calc_duration(&self) -> f32 {
        if self.elution_times.is_empty() {
            return 0.0;
        }
        let min_rt = self.elution_times.iter().cloned().fold(f32::INFINITY, f32::min);
        let max_rt = self.elution_times.iter().cloned().fold(f32::NEG_INFINITY, f32::max);
        max_rt - min_rt
    }

    /// Calculate the peakel area (sum of intensities)
    pub fn area(&self) -> f32 {
        self.intensity_values.iter().sum()
    }

    /// Get the mean left HWHM
    pub fn left_hwhm_mean(&self) -> f64 {
        match &self.left_hwhms {
            Some(hwhms) if !hwhms.is_empty() => hwhms.iter().sum::<f64>() / hwhms.len() as f64,
            _ => 0.0,
        }
    }

    /// Get the mean right HWHM
    pub fn right_hwhm_mean(&self) -> f64 {
        match &self.right_hwhms {
            Some(hwhms) if !hwhms.is_empty() => hwhms.iter().sum::<f64>() / hwhms.len() as f64,
            _ => 0.0,
        }
    }

    /// Get RT-intensity pairs
    pub fn elution_time_intensity_pairs(&self) -> Vec<(f32, f64)> {
        self.elution_times
            .iter()
            .zip(self.intensity_values.iter())
            .map(|(&rt, &intensity)| (rt, intensity as f64))
            .collect()
    }
}

// ============================================================================
// Peakel Builder
// ============================================================================

/// Builder for constructing Peakels from individual peaks
#[derive(Clone, Debug, Default)]
pub struct PeakelBuilder {
    spectrum_ids: Vec<i64>,
    elution_times: Vec<f32>,
    mz_values: Vec<f64>,
    intensity_values: Vec<f32>,
    left_hwhms: Vec<f64>,
    right_hwhms: Vec<f64>,
}

impl PeakelBuilder {
    /// Create a new peakel builder
    pub fn new() -> Self {
        Self::default()
    }

    /// Create a new peakel builder with capacity
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            spectrum_ids: Vec::with_capacity(capacity),
            elution_times: Vec::with_capacity(capacity),
            mz_values: Vec::with_capacity(capacity),
            intensity_values: Vec::with_capacity(capacity),
            left_hwhms: Vec::with_capacity(capacity),
            right_hwhms: Vec::with_capacity(capacity),
        }
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
        self.left_hwhms.push(peak.left_hwhm as f64);
        self.right_hwhms.push(peak.right_hwhm as f64);
        self
    }

    /// Add a data point directly
    pub fn add_point(
        &mut self,
        spectrum_id: i64,
        elution_time: f32,
        mz: f64,
        intensity: f32,
        left_hwhm: f64,
        right_hwhm: f64,
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
    pub fn build(self) -> Peakel {
        let has_hwhms = self.left_hwhms.iter().any(|&h| h > 0.0)
            || self.right_hwhms.iter().any(|&h| h > 0.0);

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

        let total_intensity: f64 = self
            .peakels
            .iter()
            .flat_map(|p| p.intensity_values.iter())
            .map(|&i| i as f64)
            .sum();

        if total_intensity == 0.0 {
            return self.elution_time;
        }

        let weighted_sum: f64 = self
            .peakels
            .iter()
            .flat_map(|p| p.elution_times.iter().zip(p.intensity_values.iter()))
            .map(|(&rt, &intensity)| rt as f64 * intensity as f64)
            .sum();

        (weighted_sum / total_intensity) as f32
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
    fn test_peakel_creation() {
        let peakel = Peakel::new(
            vec![1, 2, 3],
            vec![1.0, 2.0, 3.0],
            vec![500.0, 500.1, 500.2],
            vec![100.0, 200.0, 150.0],
            None,
            None,
        );

        assert_eq!(peakel.peaks_count(), 3);
        assert_eq!(peakel.apex_index(), 1);
        assert_eq!(peakel.apex_intensity(), 200.0);
        assert_eq!(peakel.apex_elution_time(), 2.0);
    }

    #[test]
    fn test_peakel_builder() {
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

        let peakel = builder.build();
        assert_eq!(peakel.peaks_count(), 3);
        assert_eq!(peakel.apex_intensity(), 200.0);
    }

    #[test]
    fn test_peakel_calculations() {
        let peakel = Peakel::new(
            vec![1, 2, 3],
            vec![1.0, 2.0, 3.0],
            vec![500.0, 500.0, 500.0],
            vec![100.0, 200.0, 100.0],
            None,
            None,
        );

        assert_eq!(peakel.calc_duration(), 2.0);
        assert_eq!(peakel.area(), 400.0);
    }
}
