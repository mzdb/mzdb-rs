//! Common utilities for peakelDB operations
//!
//! This module provides shared types and functions for peakelDB operations:
//! - `PeakelSerializer`: Static methods for MessagePack serialization/deserialization of peakel data
//! - `ExtendedPeakel`: Complete peakel with summary fields + raw data (for DB read/write)
//! - `PeakelWriterStats`: Running statistics accumulated during batch writes
//! - `PeakelDbWriter`: Trait shared by MS1 and MS2 peakelDB writers
//! - Timestamp generation

use anyhow_ext::Result;
use rusqlite::Connection;

use crate::processing::model::HasPeakelData;
use crate::processing::Peakel;

// ============================================================================
// PeakelDbWriter trait
// ============================================================================

/// Trait shared by MS1 and MS2 peakelDB writers.
///
/// Provides a common interface for batch writing, finalization, and statistics access.
pub trait PeakelDbWriter {
    /// The peakel record type written by this writer.
    type Record;

    fn connection(&mut self) -> &Connection;

    /// Finalize a peakelDB: update the peakel count in lcms_map and commit the transaction.
    fn finalize_peakeldb(&mut self) -> Result<i64> {
        let peakel_count = self.stats().peakel_count();
        self.connection().execute(
            "UPDATE lcms_map SET peakel_count = ? WHERE id = 1",
            rusqlite::params![peakel_count as i32],
        )?;
        self.connection().execute("COMMIT", [])?;

        Ok(peakel_count)
    }

    /// Write a batch of peakels to the database.
    fn write_peakels_batch(&mut self, peakels: &[Self::Record]) -> Result<()>;

    /// Convenience method to write all peakels in a single batch.
    fn write_all_peakels(&mut self, peakels: &[Self::Record]) -> Result<()> {
        self.write_peakels_batch(peakels)
    }

    /// Finalize the database: update peakel count and commit the transaction.
    ///
    /// Users should call this explicitly. If not called, `Drop` will attempt
    /// to finalize but cannot propagate errors.
    fn close(&mut self) -> Result<()>;

    /// Get the running statistics accumulated during writes.
    fn stats(&self) -> &PeakelWriterStats;
}

// ============================================================================
// PeakelWriterStats - Running statistics
// ============================================================================

/// Running statistics accumulated during peakelDB batch writes.
///
/// Updated incrementally as peakels are written, so statistics are available
/// without keeping all peakels in memory.
///
/// # Example
/// ```ignore
/// let writer = Ms1PeakelDbWriter::create("output.peakelDB", "input.mzDB", false)?;
/// // ... write batches ...
/// writer.stats().print_summary("MS1 Peakel Statistics");
/// ```
#[derive(Debug, Clone)]
pub struct PeakelWriterStats {
    count: i64,
    total_area: f64,
    sum_duration: f64,
    sum_peaks: i64,
    min_mz: f32,
    max_mz: f32,
    min_rt: f32,
    max_rt: f32,
}

impl Default for PeakelWriterStats {
    fn default() -> Self {
        Self {
            count: 0,
            total_area: 0.0,
            sum_duration: 0.0,
            sum_peaks: 0,
            min_mz: f32::INFINITY,
            max_mz: f32::NEG_INFINITY,
            min_rt: f32::INFINITY,
            max_rt: f32::NEG_INFINITY,
        }
    }
}

impl PeakelWriterStats {
    /// Create a new empty stats accumulator.
    pub fn new() -> Self {
        Self::default()
    }

    /// Update stats from a single peakel implementing `HasPeakelData`.
    pub fn add_peakel<T: HasPeakelData>(&mut self, peakel: &T) {
        self.count += 1;
        self.total_area += peakel.calc_area() as f64;
        self.sum_duration += peakel.calc_duration() as f64;
        self.sum_peaks += peakel.len() as i64;

        let mz = peakel.apex_mz().unwrap_or(f32::NAN);
        if !mz.is_nan() {
            self.min_mz = self.min_mz.min(mz);
            self.max_mz = self.max_mz.max(mz);
        }

        let rt = peakel.apex_elution_time().unwrap_or(f32::NAN);
        if !rt.is_nan() {
            self.min_rt = self.min_rt.min(rt);
            self.max_rt = self.max_rt.max(rt);
        }
    }

    /// Update stats from a batch of peakels.
    pub fn add_peakels<T: HasPeakelData>(&mut self, peakels: &[T]) {
        for peakel in peakels {
            self.add_peakel(peakel);
        }
    }

    /// Total number of peakels written.
    pub fn peakel_count(&self) -> i64 {
        self.count
    }

    /// Total integrated area across all peakels.
    pub fn total_area(&self) -> f64 {
        self.total_area
    }

    /// Average peakel duration in seconds.
    pub fn avg_duration(&self) -> f64 {
        if self.count > 0 { self.sum_duration / self.count as f64 } else { 0.0 }
    }

    /// Average number of peaks per peakel.
    pub fn avg_peaks(&self) -> f64 {
        if self.count > 0 { self.sum_peaks as f64 / self.count as f64 } else { 0.0 }
    }

    /// M/z range as (min, max). Returns (NaN, NaN) if no peakels.
    pub fn mz_range(&self) -> (f32, f32) {
        (self.min_mz, self.max_mz)
    }

    /// Retention time range in seconds as (min, max). Returns (NaN, NaN) if no peakels.
    pub fn rt_range(&self) -> (f32, f32) {
        (self.min_rt, self.max_rt)
    }
}

// ============================================================================
// PeakelSerializer - MessagePack serialization/deserialization
// ============================================================================

/// Utility struct for MessagePack serialization and deserialization of peakel data.
/// 
/// Provides static methods to serialize any `HasPeakelData` implementor to MessagePack
/// and deserialize MessagePack bytes back to `Peakel`.
/// 
/// # Format
/// MessagePack tuple of 4 arrays: `[spectrum_ids, elution_times, mz_values, intensities]`
/// Compatible with Scala mzdb-processing MessagePack format.
/// 
/// # Example
/// ```ignore
/// use mzdb::processing::{Peakel, PeakelSerializer};
/// 
/// // Serialize any HasPeakelData implementor
/// let peakel: Peakel = /* ... */;
/// let blob = PeakelSerializer::to_msgpack(&peakel)?;
/// 
/// // Deserialize back to Peakel
/// let restored = PeakelSerializer::from_msgpack(&blob)?;
/// ```
pub struct PeakelSerializer;

impl PeakelSerializer {
    /// Serialize peakel data to MessagePack bytes.
    /// 
    /// Works with any type implementing `HasPeakelData` (Peakel, ExtendedPeakel, etc.)
    pub fn to_msgpack<T: HasPeakelData>(peakel: &T) -> Result<Vec<u8>> {
        let data = (
            peakel.spectrum_ids(),
            peakel.elution_times(),
            peakel.mz_values(),
            peakel.intensity_values(),
        );
        rmp_serde::to_vec(&data)
            .map_err(|e| anyhow_ext::anyhow!("msgpack serialization error: {}", e))
    }
    
    /// Deserialize MessagePack bytes to a new Peakel.
    /// 
    /// Creates a Peakel with gap_count=0 and no HWHM data.
    /// Note: gap_count and other metadata come from database columns, not the msgpack blob.
    pub fn from_msgpack(bytes: &[u8]) -> Result<Peakel> {
        let (spectrum_ids, elution_times, mz_values_f64, intensity_values):
            (Vec<i64>, Vec<f32>, Vec<f64>, Vec<f32>) = 
            rmp_serde::from_slice(bytes)
                .map_err(|e| anyhow_ext::anyhow!("msgpack deserialization error: {}", e))?;

        // mz_values: f64 in peakelDB, f32 in memory
        let mz_values: Vec<f32> = mz_values_f64.into_iter().map(|mz| mz as f32).collect();
        
        Peakel::from_vectors(
            spectrum_ids,
            elution_times,
            mz_values,
            intensity_values,
            None,
            None,
            0, // FIXME: this is fragile and may create some inconsistencies
        )
    }
}

// ============================================================================
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
/// - Raw peaks data via embedded `Peakel`
/// - Optional MS2 DIA fields (isolation_window_id, precursor_mz)
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct ExtendedPeakel {
    pub id: i64,
    /// Weighted average m/z (32-bit for centroid data)
    pub mz: f32,
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
    pub data: Peakel,
}

impl ExtendedPeakel {
    /// Create a new MS1 ExtendedPeakel
    pub fn new(
        id: i64,
        mz: f32,
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
        data: Peakel,
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
        mz: f32,
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
        data: Peakel,
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
    pub fn apex_index(&self) -> Option<usize> {
        self.data.find_spectrum_index(self.apex_spectrum_id)
    }

    /// Check if this is an MS2 DIA peakel
    #[inline]
    pub fn is_ms2_dia(&self) -> bool {
        self.isolation_window_id.is_some()
    }

    /// Check if peakel's m/z matches a given m/z within ppm tolerance
    pub fn contains_mz(&self, mz: f32, tolerance_ppm: f32) -> bool {
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
            .map(|idx| self.data.intensity_values[idx])
    }
}

impl HasPeakelData for ExtendedPeakel {
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

/// Convert from the core Peakel type to ExtendedPeakel
impl From<&Peakel> for ExtendedPeakel {
    fn from(peakel: &Peakel) -> Self {
        let apex_intensity = peakel.apex_intensity().unwrap_or(0.0);
        let amplitude = peakel.calc_amplitude();

        Self {
            id: peakel.id,
            mz: peakel.apex_mz().unwrap_or(f32::NAN),
            elution_time: peakel.apex_elution_time().unwrap_or(0.0),
            duration: peakel.calc_duration(),
            gap_count: peakel.gap_count as i32,
            apex_intensity,
            area: peakel.calc_area(),
            amplitude: if amplitude.is_nan() { 1.0 } else { amplitude },
            peaks_count: peakel.peaks_count() as i32,
            first_spectrum_id: peakel.spectrum_ids.first().copied().unwrap_or(0),
            apex_spectrum_id: peakel.apex_spectrum_id().unwrap_or(0),
            last_spectrum_id: peakel.spectrum_ids.last().copied().unwrap_or(0),
            isolation_window_id: None,
            precursor_mz: None,
            data: peakel.clone(),
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
    fn test_peakel_from_vecs() -> Result<()> {
        let data = Peakel::from_vectors(
            vec![1, 2, 3],
            vec![10.0, 20.0, 30.0],
            vec![500.0, 500.1, 500.2],
            vec![100.0, 200.0, 150.0],
            None,
            None,
            0,
        )?;
        assert_eq!(data.peaks_count(), 3);
        assert_eq!(data.apex_index(), Some(1)); // index of 200.0
        assert_eq!(data.first_spectrum_id(), Some(1));
        assert_eq!(data.last_spectrum_id(), Some(3));

        Ok(())
    }

    #[test]
    fn test_peakel_min_max() -> Result<()> {
        let data = Peakel::from_vectors(
            vec![1, 2, 3],
            vec![10.0, 20.0, 30.0],
            vec![500.0, 500.5, 500.2],
            vec![100.0, 200.0, 150.0],
            None,
            None,
            0,
        )?;
        assert_eq!(data.min_mz(), 500.0);
        assert_eq!(data.max_mz(), 500.5);
        assert_eq!(data.min_time(), 10.0);
        assert_eq!(data.max_time(), 30.0);

        Ok(())
    }

    #[test]
    fn test_extended_peakel_is_ms2_dia() -> Result<()> {
        let data = Peakel::from_vectors(
            vec![1, 3, 5],
            vec![95.0, 100.0, 105.0],
            vec![500.0, 500.0, 500.0],
            vec![500.0, 1000.0, 500.0],
            None,
            None,
            0,
        )?;

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

        Ok(())
    }

}