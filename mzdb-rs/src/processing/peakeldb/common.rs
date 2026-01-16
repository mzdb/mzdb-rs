//! Common utilities for peakelDB operations
//!
//! This module provides shared types and functions for peakelDB operations:
//! - `PeakelData`: Raw peaks data arrays (spectrum_ids, elution_times, mz_values, intensities)
//! - `ExtendedPeakel`: Complete peakel with summary fields + raw data (for DB read/write)
//! - MessagePack blob parsing utilities
//! - Timestamp generation

use anyhow_ext::Result;
use smallvec::SmallVec;

use crate::processing::model::HasPeakelData;

// ============================================================================
// PeakelData - Raw peaks data arrays
// ============================================================================

/// Raw peaks data for a peakel using stack-optimized SmallVec.
/// 
/// Stores up to 16 data points on the stack, spilling to heap for larger peakels.
/// Most LC-MS peakels have fewer than 16 data points, making this efficient.
/// 
/// This type is used for:
/// - Storing raw peaks in `ExtendedPeakel`
/// - Converting to/from MessagePack blobs in peakelDB
/// - Conversion from/to the core `Peakel` type
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct PeakelData {
    /// Spectrum IDs at each data point
    pub spectrum_ids: SmallVec<[i64; 16]>,
    /// Elution times at each data point (seconds)
    pub elution_times: SmallVec<[f32; 16]>,
    /// m/z values at each data point
    pub mz_values: SmallVec<[f64; 16]>,
    /// Intensities at each data point
    pub intensities: SmallVec<[f32; 16]>,
}

impl PeakelData {
    /// Create empty peaks data
    pub fn new() -> Self {
        Self {
            spectrum_ids: SmallVec::new(),
            elution_times: SmallVec::new(),
            mz_values: SmallVec::new(),
            intensities: SmallVec::new(),
        }
    }

    /// Create with pre-allocated capacity
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            spectrum_ids: SmallVec::with_capacity(capacity),
            elution_times: SmallVec::with_capacity(capacity),
            mz_values: SmallVec::with_capacity(capacity),
            intensities: SmallVec::with_capacity(capacity),
        }
    }

    /// Create from Vec data (converts to SmallVec)
    pub fn from_vectors(
        spectrum_ids: Vec<i64>,
        elution_times: Vec<f32>,
        mz_values: Vec<f64>,
        intensities: Vec<f32>,
    ) -> Self {
        Self {
            spectrum_ids: SmallVec::from_vec(spectrum_ids),
            elution_times: SmallVec::from_vec(elution_times),
            mz_values: SmallVec::from_vec(mz_values),
            intensities: SmallVec::from_vec(intensities),
        }
    }

    /// Get the number of data points
    #[inline]
    pub fn len(&self) -> usize {
        self.spectrum_ids.len()
    }

    /// Check if empty
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.spectrum_ids.is_empty()
    }

    /// Serialize to MessagePack bytes using rmp_serde
    /// 
    /// Format: tuple of 4 arrays [spectrum_ids, elution_times, mz_values, intensities]
    pub fn to_msgpack(&self) -> Result<Vec<u8>> {
        let data = (
            self.spectrum_ids.as_slice(),
            self.elution_times.as_slice(),
            self.mz_values.as_slice(),
            self.intensities.as_slice(),
        );
        rmp_serde::to_vec(&data)
            .map_err(|e| anyhow_ext::anyhow!("msgpack serialization error: {}", e))
    }

    /// Deserialize from MessagePack bytes
    pub fn from_msgpack(bytes: &[u8]) -> Result<Self> {
        let (spectrum_ids, elution_times, mz_values, intensities): 
            (Vec<i64>, Vec<f32>, Vec<f64>, Vec<f32>) = 
            rmp_serde::from_slice(bytes)
                .map_err(|e| anyhow_ext::anyhow!("msgpack deserialization error: {}", e))?;
        
        Ok(Self::from_vectors(spectrum_ids, elution_times, mz_values, intensities))
    }

    /// Push a single data point
    pub fn push(&mut self, spectrum_id: i64, elution_time: f32, mz: f64, intensity: f32) {
        self.spectrum_ids.push(spectrum_id);
        self.elution_times.push(elution_time);
        self.mz_values.push(mz);
        self.intensities.push(intensity);
    }
}

impl Default for PeakelData {
    fn default() -> Self {
        Self::new()
    }
}

impl HasPeakelData for PeakelData {
    fn spectrum_ids(&self) -> &[i64] {
        &self.spectrum_ids
    }

    fn elution_times(&self) -> &[f32] {
        &self.elution_times
    }

    fn mz_values(&self) -> &[f64] {
        &self.mz_values
    }

    fn intensities(&self) -> &[f32] {
        &self.intensities
    }
}

// ============================================================================
// ExtendedPeakel - Complete peakel for DB operations
// ============================================================================

/// Extended peakel with summary fields and raw peaks data.
/// 
/// This type is the unified representation for reading/writing peakels 
/// from/to peakelDB files (both MS1 and MS2 formats).
/// 
/// It contains:
/// - Pre-computed summary fields (mz, elution_time, area, etc.)
/// - Raw peaks data for detailed analysis
/// - Optional MS2 DIA fields (isolation_window_id, precursor_mz)
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct ExtendedPeakel {
    pub id: i64,
    /// Weighted average m/z
    pub mz: f64,
    /// Elution time at apex (seconds)
    pub elution_time: f32,
    /// Total duration (seconds)
    pub duration: f32,
    /// Number of gaps (missing spectra) in the peakel
    pub gap_count: i32,
    /// Intensity at apex
    pub apex_intensity: f32,
    /// Integrated area under the curve
    pub area: f32,
    /// Amplitude (apex / baseline ratio)
    pub amplitude: f32,
    /// Number of data points
    pub peaks_count: i32,
    /// First spectrum ID in the peakel
    pub first_spectrum_id: i64,
    /// Spectrum ID at the apex
    pub apex_spectrum_id: i64,
    /// Last spectrum ID in the peakel
    pub last_spectrum_id: i64,
    
    // MS2 DIA specific fields (optional)
    /// Isolation window ID (for MS2 DIA peakels)
    pub isolation_window_id: Option<i64>,
    /// Precursor m/z (for MS2 DIA peakels)
    pub precursor_mz: Option<f64>,
    
    /// Raw peaks data
    pub data: PeakelData,
}

impl ExtendedPeakel {
    /// Create a new MS1 ExtendedPeakel
    pub fn new(
        id: i64,
        mz: f64,
        elution_time: f32,
        duration: f32,
        gap_count: i32,
        apex_intensity: f32,
        area: f32,
        amplitude: f32,
        peaks_count: i32,
        first_spectrum_id: i64,
        apex_spectrum_id: i64,
        last_spectrum_id: i64,
        data: PeakelData,
    ) -> Self {
        Self {
            id,
            mz,
            elution_time,
            duration,
            gap_count,
            apex_intensity,
            area,
            amplitude,
            peaks_count,
            first_spectrum_id,
            apex_spectrum_id,
            last_spectrum_id,
            isolation_window_id: None,
            precursor_mz: None,
            data,
        }
    }

    /// Create an MS2 DIA peakel with isolation window info
    pub fn new_ms2_dia(
        id: i64,
        mz: f64,
        elution_time: f32,
        duration: f32,
        gap_count: i32,
        apex_intensity: f32,
        area: f32,
        amplitude: f32,
        peaks_count: i32,
        first_spectrum_id: i64,
        apex_spectrum_id: i64,
        last_spectrum_id: i64,
        isolation_window_id: i64,
        precursor_mz: f64,
        data: PeakelData,
    ) -> Self {
        Self {
            id,
            mz,
            elution_time,
            duration,
            gap_count,
            apex_intensity,
            area,
            amplitude,
            peaks_count,
            first_spectrum_id,
            apex_spectrum_id,
            last_spectrum_id,
            isolation_window_id: Some(isolation_window_id),
            precursor_mz: Some(precursor_mz),
            data,
        }
    }

    /// Get the index of the apex spectrum in the peaks data (by stored apex_spectrum_id)
    pub fn apex_data_index(&self) -> Option<usize> {
        self.data.find_spectrum_index(self.apex_spectrum_id)
    }

    /// Check if this is an MS2 DIA peakel
    #[inline]
    pub fn is_ms2_dia(&self) -> bool {
        self.isolation_window_id.is_some()
    }

    /// Check if peakel's m/z matches a given m/z within ppm tolerance
    pub fn contains_mz(&self, mz: f64, tolerance_ppm: f64) -> bool {
        let tolerance = self.mz * tolerance_ppm / 1_000_000.0;
        (self.mz - mz).abs() <= tolerance
    }

    /// Check if peakel contains a given spectrum_id (uses binary search)
    pub fn contains_spectrum(&self, spectrum_id: i64) -> bool {
         self.data.find_spectrum_index(spectrum_id).is_some()
    }

    /// Get intensity at a specific spectrum, if present
    pub fn intensity_at_spectrum(&self, spectrum_id: i64) -> Option<f32> {
        self.data.find_spectrum_index(spectrum_id)
            .map(|idx| self.data.intensities[idx])
    }
}

impl HasPeakelData for ExtendedPeakel {
    fn spectrum_ids(&self) -> &[i64] {
        self.data.spectrum_ids()
    }

    fn elution_times(&self) -> &[f32] {
        self.data.elution_times()
    }

    fn mz_values(&self) -> &[f64] {
        self.data.mz_values()
    }

    fn intensities(&self) -> &[f32] {
        self.data.intensities()
    }
}

/// Convert from the core Peakel type to ExtendedPeakel
impl From<&crate::processing::Peakel> for ExtendedPeakel {
    fn from(peakel: &crate::processing::Peakel) -> Self {
        let data = PeakelData {
            spectrum_ids: peakel.spectrum_ids.clone(),
            elution_times: peakel.elution_times.clone(),
            mz_values: peakel.mz_values.clone(),
            intensities: peakel.intensity_values.clone(),
        };

        let min_intensity = peakel.intensity_values.iter().cloned().fold(f32::INFINITY, f32::min).max(1.0);
        let apex_intensity = peakel.apex_intensity().unwrap_or(0.0);

        Self {
            id: peakel.id,
            mz: peakel.calc_mz(),
            elution_time: peakel.apex_elution_time().unwrap_or(0.0),
            duration: peakel.calc_duration(),
            gap_count: 0, // Not tracked in core Peakel
            apex_intensity,
            area: peakel.area(),
            amplitude: apex_intensity / min_intensity,
            peaks_count: peakel.peaks_count() as i32,
            first_spectrum_id: peakel.spectrum_ids.first().copied().unwrap_or(0),
            apex_spectrum_id: peakel.apex_spectrum_id().unwrap_or(0),
            last_spectrum_id: peakel.spectrum_ids.last().copied().unwrap_or(0),
            isolation_window_id: None,
            precursor_mz: None,
            data,
        }
    }
}

// ============================================================================
// Timestamp Utilities
// ============================================================================

/// Generate a lightweight ISO 8601 timestamp string
///
/// Returns format: "YYYY-MM-DD HH:MM:SS"
pub fn chrono_lite_timestamp() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let duration = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
    let secs = duration.as_secs();
    let days = secs / 86400;
    let years = 1970 + days / 365;
    let remaining_days = days % 365;
    let months = remaining_days / 30 + 1;
    let day = remaining_days % 30 + 1;
    let hour = (secs % 86400) / 3600;
    let min = (secs % 3600) / 60;
    let sec = secs % 60;
    format!("{:04}-{:02}-{:02} {:02}:{:02}:{:02}", years, months, day, hour, min, sec)
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_peakel_data_new() {
        let data = PeakelData::new();
        assert!(data.is_empty());
        assert_eq!(data.len(), 0);
    }

    #[test]
    fn test_peakel_data_from_vecs() {
        let data = PeakelData::from_vectors(
            vec![1, 2, 3],
            vec![10.0, 20.0, 30.0],
            vec![500.0, 500.1, 500.2],
            vec![100.0, 200.0, 150.0],
        );
        assert_eq!(data.len(), 3);
        assert_eq!(data.apex_index(), Some(1)); // index of 200.0
        assert_eq!(data.first_spectrum_id(), Some(1));
        assert_eq!(data.last_spectrum_id(), Some(3));
    }

    #[test]
    fn test_peakel_data_min_max() {
        let data = PeakelData::from_vectors(
            vec![1, 2, 3],
            vec![10.0, 20.0, 30.0],
            vec![500.0, 500.5, 500.2],
            vec![100.0, 200.0, 150.0],
        );
        assert_eq!(data.min_mz(), 500.0);
        assert_eq!(data.max_mz(), 500.5);
        assert_eq!(data.min_time(), 10.0);
        assert_eq!(data.max_time(), 30.0);
    }

    #[test]
    fn test_extended_peakel_is_ms2_dia() {
        let data = PeakelData::new();

        let ms1 = ExtendedPeakel::new(
            1, 500.0, 100.0, 10.0, 0, 1000.0, 5000.0, 10.0, 5,
            1, 3, 5, data.clone()
        );
        assert!(!ms1.is_ms2_dia());

        let ms2 = ExtendedPeakel::new_ms2_dia(
            1, 500.0, 100.0, 10.0, 0, 1000.0, 5000.0, 10.0, 5,
            1, 3, 5, 1, 400.0, data
        );
        assert!(ms2.is_ms2_dia());
    }

}