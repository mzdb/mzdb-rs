//! Data model types for mzDB files
//!
//! This module contains all the data structures used to represent mass spectrometry
//! data stored in mzDB format, including spectra, peaks, data encodings, and various
//! metadata types.
#![allow(unused)]

use anyhow_ext::{anyhow, Result};
use roxmltree::Document;
use serde::{Deserialize, Serialize};
use smallvec::SmallVec;
use std::collections::HashMap;

use crate::model::DataMode::Fitted;

// ============================================================================
// Data Point Trait
// ============================================================================

/// Trait for types that provide access to spectrum data points (m/z and intensity arrays)
/// 
/// This trait allows generic serialization of spectrum data without requiring
/// a specific struct type. Both `SpectrumData` and simpler types like 
/// `SimpleSpectrumData` can implement this trait.
pub trait DataPointProvider {
    /// Get a reference to the m/z values array
    fn mz_array(&self) -> &[f32];
    
    /// Get a reference to the intensity values array
    fn intensity_array(&self) -> &[f32];
    
    /// Get the number of data points
    fn data_points_count(&self) -> usize {
        self.mz_array().len()
    }
}

// ============================================================================
// Simple Spectrum Data
// ============================================================================

/// Simplified spectrum data containing only m/z and intensity arrays
///
/// This is a lightweight alternative to `SpectrumData` for use cases that don't
/// need data encoding information or fitted peak parameters (lwhm/rwhm).
/// Commonly used in processing pipelines like DDA-to-DIA conversion.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct SimpleSpectrumData {
    /// m/z values (32-bit for centroid data)
    pub mz_array: Vec<f32>,
    /// Intensity values
    pub intensity_array: Vec<f32>,
}

impl SimpleSpectrumData {
    /// Create new empty spectrum data
    pub fn new() -> Self {
        Self::default()
    }

    /// Create spectrum data with pre-allocated capacity
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            mz_array: Vec::with_capacity(capacity),
            intensity_array: Vec::with_capacity(capacity),
        }
    }

    /// Get number of peaks
    pub fn peaks_count(&self) -> usize {
        self.mz_array.len()
    }

    /// Check if empty
    pub fn is_empty(&self) -> bool {
        self.mz_array.is_empty()
    }

    /// Scale all intensities by a factor
    pub fn scale_intensities(&mut self, factor: f32) {
        for intensity in &mut self.intensity_array {
            *intensity *= factor;
        }
    }

    /// Clear all data, keeping allocated capacity
    pub fn clear(&mut self) {
        self.mz_array.clear();
        self.intensity_array.clear();
    }
}

impl DataPointProvider for SimpleSpectrumData {
    fn mz_array(&self) -> &[f32] {
        &self.mz_array
    }

    fn intensity_array(&self) -> &[f32] {
        &self.intensity_array
    }
}

// Note: From<SpectrumData> impls are defined after SpectrumData struct

// ============================================================================
// Acquisition mode constants and enum
// ============================================================================

/// Data Dependent Acquisition mode description
pub const ACQUISITION_MODE_DDA: &str = "Data Dependant Acquisition (Thermo designation), Warning: in ABI this is called IDA (Information Dependant Acquisition)";
/// SWATH acquisition mode description
pub const ACQUISITION_MODE_SWATH: &str = "ABI Swath acquisition or Thermo swath acquisition";
/// Multiple Reaction Monitoring mode description
pub const ACQUISITION_MODE_MRM: &str = "Multiple reaction monitoring";
/// Single Reaction Monitoring mode description
pub const ACQUISITION_MODE_SRM: &str = "SRM (Single reaction monitoring) acquisition";
/// Unknown acquisition mode description
pub const ACQUISITION_MODE_UNKNOWN: &str = "unknown acquisition mode";

/// Mass spectrometry acquisition mode
#[derive(Copy, Clone, Debug, PartialEq)]
#[allow(clippy::upper_case_acronyms)]
pub enum AcquisitionMode {
    /// Data Dependent Acquisition
    DDA,
    /// SWATH acquisition
    SWATH,
    /// Multiple Reaction Monitoring
    MRM,
    /// Single Reaction Monitoring
    SRM,
    /// Unknown acquisition mode
    Unknown,
}

/// Data precision format for m/z and intensity values
#[derive(Copy, Clone, Debug, PartialEq, strum_macros::Display)]
pub enum DataPrecision {
    /// Unknown precision
    Unknown = 0,
    /// 64-bit m/z, 64-bit intensity
    Float64Float64 = 1,
    /// 64-bit m/z, 32-bit intensity
    Float64Float32 = 2,
    /// 32-bit m/z, 32-bit intensity
    Float32Float32 = 3,
    /// Fitted peaks with 64-bit m/z, 32-bit intensity
    Fitted64Float32 = 4,
}

/// A peak with 32-bit m/z and 32-bit intensity
#[derive(Copy, Clone, Debug, PartialEq)]
pub struct DataPoint3232 {
    pub x: f32,
    pub y: f32,
}

/// A peak with 64-bit m/z and 32-bit intensity
#[derive(Copy, Clone, Debug, PartialEq)]
pub struct DataPoint6432 {
    pub x: f64,
    pub y: f32,
}

/// A peak with 64-bit m/z and 64-bit intensity
#[derive(Copy, Clone, Debug, PartialEq)]
pub struct DataPoint6464 {
    pub x: f64,
    pub y: f64,
}

/// A fitted peak with half-width at half-maximum values
#[derive(Copy, Clone, Debug, PartialEq)]
pub struct FittedPeak {
    /// m/z value
    pub x: f64,
    /// Intensity value
    pub y: f32,
    /// Left half-width at half-maximum
    pub left_hwhm: f32,
    /// Right half-width at half-maximum
    pub right_hwhm: f32,
}

/// A peak in an extracted ion chromatogram (XIC)
#[derive(Copy, Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct XicPeak {
    /// m/z value (32-bit for centroid data)
    pub mz: f32,
    /// Intensity value
    pub intensity: f32,
    /// Retention time
    pub rt: f32,
}

impl XicPeak {
    /// Create a new XIC peak
    pub fn new(mz: f32, intensity: f32, rt: f32) -> Self {
        Self { mz, intensity, rt }
    }
}

// Note: CvParam, UserParam, UserText, and ParamTree are defined in xml.rs
// with proper Optional fields for XML parsing. Re-exported from lib.rs.

/// Data acquisition mode for spectra
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
#[repr(i32)]
pub enum DataMode {
    /// Profile (continuous) data
    Profile = -1,
    /// Centroided (discrete peaks) data
    Centroid = 12,
    /// Fitted peaks with peak shape parameters
    Fitted = 20,
}

/// Peak encoding format specifying byte sizes
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
#[repr(i32)]
pub enum PeakEncoding {
    /// Low resolution: 32-bit m/z (8 bytes per peak)
    LowRes = 8,
    /// High resolution: 64-bit m/z, 32-bit intensity (12 bytes per peak)
    HighRes = 12,
    /// No loss: 64-bit m/z, 64-bit intensity (16 bytes per peak)
    NoLoss = 16,
}

/// Byte order for binary data
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
pub enum ByteOrder {
    /// Big-endian byte order
    BigEndian,
    /// Little-endian byte order
    LittleEndian,
}

/// Data encoding specification for spectrum data
#[derive(Clone, Debug, PartialEq)]
pub struct DataEncoding {
    /// Unique identifier
    pub id: i64,
    /// Data mode (profile, centroid, or fitted)
    pub mode: DataMode,
    /// Peak encoding format
    pub peak_encoding: PeakEncoding,
    /// Compression algorithm (e.g., "none", "zlib")
    pub compression: String,
    /// Byte order for numeric values
    pub byte_order: ByteOrder,
}

impl DataEncoding {
    pub fn get_peak_size(&self) -> usize {
        let pe = self.peak_encoding as usize;
        if self.mode == Fitted {
            pe + 8
        } else {
            pe
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct DataEncodingsCache {
    data_encoding_by_id: HashMap<i64, DataEncoding>,
    data_encoding_id_by_spectrum_id: HashMap<i64, i64>,
}

impl DataEncodingsCache {
    pub fn new(
        data_encoding_by_id: HashMap<i64, DataEncoding>,
        data_encoding_id_by_spectrum_id: HashMap<i64, i64>,
    ) -> Self {
        Self {
            data_encoding_by_id,
            data_encoding_id_by_spectrum_id,
        }
    }

    pub fn get_data_encoding_by_id(&self, de_id: &i64) -> Option<&DataEncoding> {
        self.data_encoding_by_id.get(de_id)
    }

    pub fn get_data_encoding_by_spectrum_id(&self, spectrum_id: &i64) -> Option<&DataEncoding> {
        let de_id = self.data_encoding_id_by_spectrum_id.get(spectrum_id)?;
        self.data_encoding_by_id.get(de_id)
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct DataPoints32x32 {
    pub x_list: Vec<f32>,
    pub y_list: Vec<f32>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct DataPoints64x32 {
    pub x_list: Vec<f64>,
    pub y_list: Vec<f32>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct DataPoints64x64 {
    pub x_list: Vec<f64>,
    pub y_list: Vec<f64>,
}

/// Raw spectrum data containing peaks
#[derive(Clone, Debug, PartialEq)]
pub struct SpectrumData {
    /// Data encoding used for this spectrum
    pub data_encoding: DataEncoding,
    /// Number of peaks
    pub peaks_count: usize,
    /// m/z values array (32-bit for centroid data)
    pub mz_array: Vec<f32>,
    /// Intensity values array
    pub intensity_array: Vec<f32>,
    /// Left half-width at half-maximum (for fitted peaks)
    pub lwhm_array: Vec<f32>,
    /// Right half-width at half-maximum (for fitted peaks)
    pub rwhm_array: Vec<f32>,
}

impl SpectrumData {
    /// Create new spectrum data
    pub fn new(
        data_encoding: DataEncoding,
        mz_list: Vec<f32>,
        intensity_list: Vec<f32>,
        left_hwhm_list: Option<Vec<f32>>,
        right_hwhm_list: Option<Vec<f32>>,
    ) -> Self {
        let peaks_count = mz_list.len();
        SpectrumData {
            data_encoding,
            peaks_count,
            mz_array: mz_list,
            intensity_array: intensity_list,
            lwhm_array: left_hwhm_list.unwrap_or_default(),
            rwhm_array: right_hwhm_list.unwrap_or_default(),
        }
    }

    /// Get m/z value at a specific index
    #[cfg(feature = "writer")]
    pub fn get_mz_at(&self, index: usize) -> Result<f32, anyhow_ext::Error> {
        self.mz_array.get(index)
            .copied()
            .ok_or_else(|| anyhow!("Index {} out of bounds for m/z array", index))
    }

    /// Get intensity value at a specific index
    #[cfg(feature = "writer")]
    pub fn get_intensity_at(&self, index: usize) -> Result<f32, anyhow_ext::Error> {
        self.intensity_array.get(index)
            .copied()
            .ok_or_else(|| anyhow!("Index {} out of bounds for intensity array", index))
    }

    /// Get left HWHM value at a specific index
    #[cfg(feature = "writer")]
    pub fn get_left_hwhm_at(&self, index: usize) -> Option<f32> {
        self.lwhm_array.get(index).copied()
    }

    /// Get right HWHM value at a specific index
    #[cfg(feature = "writer")]
    pub fn get_right_hwhm_at(&self, index: usize) -> Option<f32> {
        self.rwhm_array.get(index).copied()
    }

    /// Find the nearest peak to a given m/z within tolerance
    pub fn get_nearest_peak(&self, mz: f32, mz_tol_ppm: f32, rt: f32) -> Option<XicPeak> {
        if self.peaks_count == 0 {
            return None;
        }

        let mz_da = mz * mz_tol_ppm / 1_000_000.0;
        let idx = self
            .mz_array
            .binary_search_by(|&probe| probe.total_cmp(&mz))
            .unwrap_or_else(|i| i);

        let new_idx = if idx == self.peaks_count {
            let prev_val = self.mz_array[self.peaks_count - 1];
            if (mz - prev_val).abs() > mz_da {
                return None;
            }
            idx - 1
        } else if idx == 0 {
            let next_val = self.mz_array[idx];
            if (mz - next_val).abs() > mz_da {
                return None;
            }
            idx
        } else {
            let next_val = self.mz_array[idx];
            let prev_val = self.mz_array[idx - 1];
            let diff_next_val = (mz - next_val).abs();
            let diff_prev_val = (mz - prev_val).abs();
            if diff_next_val < diff_prev_val {
                if diff_next_val > mz_da {
                    return None;
                }
                idx
            } else {
                if diff_prev_val > mz_da {
                    return None;
                }
                idx - 1
            }
        };

        Some(XicPeak {
            mz: self.mz_array[new_idx],
            intensity: self.intensity_array[new_idx],
            rt,
        })
    }
}

impl DataPointProvider for SpectrumData {
    fn mz_array(&self) -> &[f32] {
        &self.mz_array
    }
    
    fn intensity_array(&self) -> &[f32] {
        &self.intensity_array
    }
    
    fn data_points_count(&self) -> usize {
        self.peaks_count
    }
}

impl From<SpectrumData> for SimpleSpectrumData {
    fn from(sd: SpectrumData) -> Self {
        Self {
            mz_array: sd.mz_array,
            intensity_array: sd.intensity_array,
        }
    }
}

impl From<&SpectrumData> for SimpleSpectrumData {
    fn from(sd: &SpectrumData) -> Self {
        Self {
            mz_array: sd.mz_array.clone(),
            intensity_array: sd.intensity_array.clone(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SpectrumHeader {
    pub id: i64,
    pub initial_id: i64,
    pub title: String,
    pub cycle: i64,
    pub time: f32,
    pub ms_level: i64,
    pub activation_type: Option<String>,
    pub tic: f32,
    pub base_peak_mz: f64,
    pub base_peak_intensity: f32,
    #[serde(rename = "main_precursor_mz")]
    pub precursor_mz: Option<f64>,
    #[serde(rename = "main_precursor_charge")]
    pub precursor_charge: Option<i32>,
    #[serde(rename = "data_points_count")]
    pub peaks_count: i64,
    #[serde(rename = "param_tree")]
    pub param_tree_str: Option<String>,
    #[serde(rename = "scan_list")]
    pub scan_list_str: Option<String>,
    #[serde(rename = "precursor_list")]
    pub precursor_list_str: Option<String>,
    #[serde(rename = "product_list")]
    pub product_list_str: Option<String>,
    pub shared_param_tree_id: Option<i64>,
    pub instrument_configuration_id: i64,
    pub source_file_id: i64,
    pub run_id: i64,
    pub data_processing_id: i64,
    pub data_encoding_id: i64,
    pub bb_first_spectrum_id: Option<i64>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct Spectrum {
    pub header: SpectrumHeader,
    pub data: SpectrumData,
}

#[derive(Clone, Debug, PartialEq)]
pub struct SpectrumSlice {
    pub spectrum: Spectrum,
    pub run_slice_id: i64,
}

#[derive(Clone, Debug, PartialEq)]
pub struct RunSliceHeader {
    pub id: i64,
    pub ms_level: i64,
    pub number: i64,
    pub begin_mz: f64,
    pub end_mz: f64,
    pub run_id: i64,
}

#[derive(Copy, Clone, Debug, PartialEq)]
pub struct BBSizes {
    pub bb_mz_height_ms1: f64,
    pub bb_mz_height_msn: f64,
    pub bb_rt_width_ms1: f32,
    pub bb_rt_width_msn: f32,
}

impl BBSizes {
    pub fn from_xml(xml: &str) -> Result<Self> {
        let doc = Document::parse(xml)?;

        let mut bb_mz_height_ms1 = 0.0;
        let mut bb_mz_height_msn = 0.0;
        let mut bb_rt_width_ms1 = 0.0;
        let mut bb_rt_width_msn = 0.0;

        for user_param in doc
            .descendants()
            .filter(|n| n.tag_name().name() == "userParam")
        {
            if let Some(name) = user_param.attribute("name") {
                match name {
                    "ms1_bb_mz_width" => {
                        if let Some(value) = user_param.attribute("value") {
                            bb_mz_height_ms1 = value.parse::<f64>()?;
                        }
                    }
                    "msn_bb_mz_width" => {
                        if let Some(value) = user_param.attribute("value") {
                            bb_mz_height_msn = value.parse::<f64>()?;
                        }
                    }
                    "ms1_bb_time_width" => {
                        if let Some(value) = user_param.attribute("value") {
                            bb_rt_width_ms1 = value.parse::<f32>()?;
                        }
                    }
                    "msn_bb_time_width" => {
                        if let Some(value) = user_param.attribute("value") {
                            bb_rt_width_msn = value.parse::<f32>()?;
                        }
                    }
                    _ => {}
                }
            }
        }

        Ok(BBSizes {
            bb_mz_height_ms1,
            bb_mz_height_msn,
            bb_rt_width_ms1,
            bb_rt_width_msn,
        })
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct BoundingBox {
    pub id: i64,
    pub first_spectrum_id: i64,
    pub last_spectrum_id: i64,
    pub run_slice_id: i64,
    pub blob_data: Vec<u8>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct BoundingBoxIndex {
    pub bb_id: i64,
    pub spectrum_slices_count: usize,
    /// Spectrum IDs in this bounding box (typically 1-16 spectra per BB)
    pub spectra_ids: SmallVec<[i64; 16]>,
    /// Byte offsets for each spectrum slice in the blob data
    pub slices_indexes: SmallVec<[usize; 16]>,
    /// Peak counts for each spectrum slice
    pub peaks_counts: SmallVec<[usize; 16]>,
}

#[derive(Copy, Clone, Debug, PartialEq)]
pub enum XicMethod {
    Max = 0,
    Nearest = 1,
}

#[derive(Copy, Clone, Debug)]
pub struct MzRange {
    pub min_mz: f64,
    pub max_mz: f64,
}

// Manual PartialEq using bit comparison for HashMap compatibility
impl PartialEq for MzRange {
    fn eq(&self, other: &Self) -> bool {
        self.min_mz.to_bits() == other.min_mz.to_bits() &&
        self.max_mz.to_bits() == other.max_mz.to_bits()
    }
}

impl Eq for MzRange {}

#[cfg(feature = "writer")]
impl std::hash::Hash for MzRange {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.min_mz.to_bits().hash(state);
        self.max_mz.to_bits().hash(state);
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct EntityCache {
    pub bb_sizes: BBSizes,
    pub data_encodings_cache: DataEncodingsCache,
    pub spectrum_headers: Vec<SpectrumHeader>,
    /// Map from spectrum ID to index in spectrum_headers vec
    pub spectrum_id_to_index: HashMap<i64, usize>,
    /// Cached msn_bb_time_width from mzDB metadata (None if not found)
    pub msn_bb_time_width: Option<f64>,
}

impl EntityCache {
    /// Get spectrum header by ID (handles non-consecutive IDs)
    pub fn get_spectrum_header(&self, spectrum_id: i64) -> Option<&SpectrumHeader> {
        self.spectrum_id_to_index
            .get(&spectrum_id)
            .and_then(|&idx| self.spectrum_headers.get(idx))
    }
}

// ============================================================================
// RunSlice - For m/z-sliced iteration
// ============================================================================

/// Run slice data containing all spectrum slices for a given m/z range
///
/// This is the Rust port of Java's `fr.profi.mzdb.model.RunSliceData`
#[derive(Clone, Debug)]
pub struct RunSliceData {
    /// Run slice ID
    pub run_slice_id: i64,
    /// All spectrum slices in this run slice
    pub spectrum_slices: Vec<SpectrumSlice>,
}

impl RunSliceData {
    /// Create a new RunSliceData
    pub fn new(run_slice_id: i64, spectrum_slices: Vec<SpectrumSlice>) -> Self {
        Self {
            run_slice_id,
            spectrum_slices,
        }
    }
}

/// Complete run slice with header and data
///
/// This is the Rust port of Java's `fr.profi.mzdb.model.RunSlice`
///
/// # Java Reference
/// ```java
/// public class RunSlice {
///     protected final RunSliceHeader header;
///     protected final RunSliceData data;
///     
///     public RunSliceHeader getHeader() { return header; }
///     public RunSliceData getData() { return data; }
/// }
/// ```
#[derive(Clone, Debug)]
pub struct RunSlice {
    /// Run slice metadata
    pub header: RunSliceHeader,
    /// Run slice spectrum data
    pub data: RunSliceData,
}

impl RunSlice {
    /// Create a new RunSlice
    pub fn new(header: RunSliceHeader, data: RunSliceData) -> Self {
        Self { header, data }
    }
    
    /// Get the run slice header
    pub fn get_header(&self) -> &RunSliceHeader {
        &self.header
    }
    
    /// Get the run slice data
    pub fn get_data(&self) -> &RunSliceData {
        &self.data
    }
}

