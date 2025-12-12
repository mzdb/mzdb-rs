//! Data model types for mzDB files
//!
//! This module contains all the data structures used to represent mass spectrometry
//! data stored in mzDB format, including spectra, peaks, data encodings, and various
//! metadata types.
#![allow(unused)]

use anyhow::*;
use roxmltree::Document;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use crate::model::DataMode::Fitted;

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

// PSI-MS controlled vocabulary accession numbers
pub const PSI_MS_32_BIT_FLOAT: &str = "*0521";
pub const PSI_MS_64_BIT_FLOAT: &str = "*0523";
pub const ACQUISITION_PARAMETER: &str = "*1954";
pub const ISOLATION_WINDOW_TARGET_MZ: &str = "MS:*0827";
pub const ISOLATION_WINDOW_LOWER_OFFSET: &str = "MS:*0828";
pub const ISOLATION_WINDOW_UPPER_OFFSET: &str = "MS:*0829";
pub const SELECTED_ION_MZ: &str = "MS:*0744";

/// Mass spectrometry acquisition mode
#[derive(Copy, Clone, Debug, PartialEq)]
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
#[derive(Copy, Clone, Debug, PartialEq)]
pub struct XicPeak {
    /// m/z value
    pub mz: f64,
    /// Intensity value
    pub intensity: f32,
    /// Retention time
    pub rt: f32,
}

#[derive(Clone, Debug, PartialEq)]
pub struct CvParam {
    pub cv_ref: String,
    pub accession: String,
    pub name: String,
    pub value: String,
    pub unit_cv_ref: String,
    pub unit_accession: String,
    pub unit_name: String,
}

#[derive(Clone, Debug, PartialEq)]
pub struct UserParam {
    pub cv_ref: String,
    pub accession: String,
    pub name: String,
    pub value: String,
    pub r#type: String,
}

#[derive(Clone, Debug, PartialEq)]
pub struct UserText {
    pub cv_ref: String,
    pub accession: String,
    pub name: String,
    pub text: String,
    pub r#type: String,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ParamTree {
    pub cv_params: Vec<CvParam>,
    pub user_params: Vec<UserParam>,
    pub user_texts: Vec<UserText>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct MzdbParamTree {
    pub ms1_bb_mz_width: f32,
    pub msn_bb_mz_width: f32,
    pub ms1_bb_time_width: f32,
    pub msn_bb_time_width: f32,
    pub is_loss_less: i64,
    pub origin_file_format: String,
}

/// Data acquisition mode for spectra
#[derive(Copy, Clone, Debug, PartialEq)]
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
#[derive(Copy, Clone, Debug, PartialEq)]
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
#[derive(Copy, Clone, Debug, PartialEq)]
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
    /// m/z values array
    pub mz_array: Vec<f64>,
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
        mz_list: Vec<f64>,
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

    /// Convert ppm tolerance to Daltons at a given m/z
    fn ppm_to_da(&self, mz: f64, ppm: f64) -> f64 {
        mz * ppm / 1_000_000.0
    }

    /// Find the nearest peak to a given m/z within tolerance
    pub fn get_nearest_peak(&self, mz: f64, mz_tol_ppm: f64, rt: f32) -> Option<XicPeak> {
        if self.peaks_count == 0 {
            return None;
        }

        let mz_da = self.ppm_to_da(mz, mz_tol_ppm);
        let idx = self
            .mz_array
            .binary_search_by(|&probe| probe.partial_cmp(&mz).unwrap_or(std::cmp::Ordering::Equal))
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
    pub bb_first_spectrum_id: i64,
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

#[derive(Clone, Debug, PartialEq)]
pub struct RunSliceData {
    pub id: i64,
    pub spectrum_slice: Vec<SpectrumSlice>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct RunSlice {
    pub header: RunSliceHeader,
    pub data: RunSliceData,
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
    pub spectra_ids: Vec<i64>,
    pub slices_indexes: Vec<usize>,
    pub peaks_counts: Vec<usize>,
}

#[derive(Copy, Clone, Debug, PartialEq)]
pub enum XicMethod {
    Max = 0,
    Nearest = 1,
}

#[derive(Copy, Clone, Debug, PartialEq)]
pub struct IsolationWindow {
    pub min_mz: f64,
    pub max_mz: f64,
}

#[derive(Clone, Debug, PartialEq)]
pub struct EntityCache {
    pub bb_sizes: BBSizes,
    pub data_encodings_cache: DataEncodingsCache,
    pub spectrum_headers: Vec<SpectrumHeader>,
}
