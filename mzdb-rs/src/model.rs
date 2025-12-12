//use itertools::Itertools;
//use rusqlite::{Connection, Result};

use anyhow::*;
use roxmltree::Document;
use serde::{Deserialize, Serialize};
//use serde_rusqlite::*;
use std::collections::HashMap;

use crate::model::DataMode::FITTED;


pub const ACQUISITION_MODE_DDA: &str = "Data Dependant Acquisition (Thermo designation), Warning: in ABI this is called IDA (Information Dependant Acquisition)";
pub const ACQUISITION_MODE_SWATH: &str = "ABI Swath acquisition or Thermo swath acquisition";
pub const ACQUISITION_MODE_MRM: &str = "Multiple reaction monitoring";
pub const ACQUISITION_MODE_SRM: &str = "SRM (Single reaction monitoring) acquisition";
pub const ACQUISITION_MODE_UNKNOWN: &str = "unknown acquisition mode";

//verifier que la bien chaine de caractère
pub const PSI_MS_32_BIT_FLOAT: &str = "*0521";
pub const PSI_MS_64_BIT_FLOAT: &str = "*0523";
pub const ACQUISITION_PARAMETER: &str = "*1954";
pub const ISOLATION_WINDOW_TARGET_MZ: &str = "MS:*0827";
pub const ISOLATION_WINDOW_LOWER_OFFSET: &str = "MS:*0828";
pub const ISOLATION_WINDOW_UPPER_OFFSET: &str = "MS:*0829";
pub const SELECTED_ION_MZ: &str = "MS:*0744";

//the acquisition mode
#[derive(Copy, Clone, Debug, PartialEq)]
pub enum AquisitionModeEnum {
    DDA,
    SWATH,
    MRM,
    SRM,
    UNKNOW,
}

//an array of each acquisition mode decription, match with the pub enumeration

#[derive(Copy, Clone, Debug, PartialEq, strum_macros::Display)]//strum_macros::EnumString
pub enum DataPrecisionEnum {
    //use to know which precision is use
    //#[strum(serialize = "DataPrecisionUnknown")]
    DataPrecisionUnknown = 0,
    DataPrecision6464 = 1,
    DataPrecision6432 = 2,
    DataPrecision3232 = 3,
    DataPrecisionFitted6432 = 4,
}

#[derive(Copy, Clone, Debug, PartialEq)]
pub struct DataPoint3232 {
    pub x: f32,
    pub y: f32,
}

#[derive(Copy, Clone, Debug, PartialEq)]
pub struct DataPoint6432 {
    pub x: f64,
    pub y: f32,
}

#[derive(Copy, Clone, Debug, PartialEq)]
pub struct DataPoint6464 {
    pub x: f64,
    pub y: f64,
}

#[derive(Copy, Clone, Debug, PartialEq)]
pub struct FittedPeak {
    pub x: f64,
    pub y: f32,
    pub left_hwhm: f32,
    pub right_hwhm: f32,
}

#[derive(Copy, Clone, Debug, PartialEq)]
pub struct XicPeak {
    pub mz: f64,
    pub intensity: f32,
    pub rt: f32,
}

//ParamTree.h
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

#[derive(Copy, Clone, Debug, PartialEq)]
#[repr(i32)]
pub enum DataMode {
    PROFILE = -1,
    CENTROID = 12,
    FITTED = 20,
}

#[allow(non_camel_case_types)]
#[derive(Copy, Clone, Debug, PartialEq)]
#[repr(i32)]
pub enum PeakEncoding {
    LOW_RES_PEAK = 8,
    HIGH_RES_PEAK = 12,
    NO_LOSS_PEAK = 16,
}

#[allow(non_camel_case_types)]
#[derive(Copy, Clone, Debug, PartialEq)]
pub enum ByteOrder {
    BIG_ENDIAN,
    LITTLE_ENDIAN,
}

#[derive(Clone, Debug, PartialEq)]
pub struct DataEncoding {
    pub id: i64,
    pub mode: DataMode,
    pub peak_encoding: PeakEncoding,
    pub compression: String,
    pub byte_order: ByteOrder,
}

impl DataEncoding {
    pub fn get_peak_size(&self) -> usize {
        let pe = self.peak_encoding as usize;

        let peak_size= if self.mode == FITTED {
            pe + 8
        }else {
            pe
        };

        peak_size

        /*
      /*if self.mode ==20{
            let peak_size=self.PeakEncoding + 8.get(peak_size);
        } else {
            self.PeakEncoding;
    }
       */
        /*let peak_size= if  self.mode == 20 {
            self.PeakEncoding + 8;
        } else {
           self.PeakEncoding;
        };
         */
        let mode =self.mode;
       /* return match mode {
            DataMode::FITTED => {
                pe + 8
            }
            DataMode::CENTROID => {
                pe
            }
            DataMode::PROFILE => {
                pe
            }
        }
        */
        if mode==DataMode::FITTED {
            pe = pe + 8
        }*/
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
        data_encoding_id_by_spectrum_id: HashMap<i64, i64>
    ) -> Self {
        Self { data_encoding_by_id, data_encoding_id_by_spectrum_id }
    }

    pub fn get_data_encoding_by_id(&self, de_id: &i64) -> Option<&DataEncoding> {
        self.data_encoding_by_id.get(de_id)
    }

    pub fn get_data_encoding_by_spectrum_id(&self, spectrum_id: &i64) -> Option<&DataEncoding> {
        let de_id_opt = self.data_encoding_id_by_spectrum_id.get(spectrum_id);
        if de_id_opt.is_none() {
            return None
        }

        let de_opt =  self.data_encoding_by_id.get(de_id_opt.unwrap());
        de_opt
    }
}

// TODO: delete ???
/*pub union Peak {
    dp_64_64: DataPoint6464,
    dp_64_32: DataPoint6432,
    dp_32_32: DataPoint3232,
    fdp_64_64: FittedPeak,
}*/

/*
pub struct SpectrumPeaks {
    pub data_precision: DataPrecisionEnum,
    pub peak_count: i64,
    pub peaks: Vec<Peak>,
    /*union {
    libmzdb_data_point_64_64_t* peaks64_64;
    libmzdb_data_point_64_32_t* peaks64_32;
    libmzdb_data_point_32_32_t* peaks32_32;
    libmzdb_fitted_peak_t* fitted_peaks;
    //peak_t* peaks; // upt to peak* -- ???
    }

     */
}*/

#[allow(non_camel_case_types)]
#[derive(Clone, Debug, PartialEq)]
pub struct DataPoints_32_32 {
    pub x_list: Vec<f32>,
    pub y_list: Vec<f32>,
}

#[allow(non_camel_case_types)]
#[derive(Clone, Debug, PartialEq)]
pub struct DataPoints_64_32 {
    pub x_list: Vec<f64>,
    pub y_list: Vec<f32>,
}

#[allow(non_camel_case_types)]
#[derive(Clone, Debug, PartialEq)]
pub struct DataPoints_64_64 {
    pub x_list: Vec<f64>,
    pub y_list: Vec<f64>,
}

/*union MzArray {
    mz_array_as_doubles: Vec<f64>,
    mz_array_as_floats: Vec<f32>,
}

union IntensityArray {
    intensity_array_as_doubles: Vec<f64>,
    intensity_array_as_floats: Vec<f32>,
}*/

#[derive(Clone, Debug, PartialEq)]
pub struct SpectrumData {
    pub data_encoding: DataEncoding,
    pub peaks_count: usize,
    pub mz_array: Vec<f64>,
    pub intensity_array: Vec<f32>,
    pub lwhm_array: Vec<f32>, // warning: can be NULL
    pub rwhm_array: Vec<f32>, // warning: can be NULL
}

impl SpectrumData {
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
            peaks_count: mz_list.len(),
            mz_array: mz_list,
            intensity_array: intensity_list,
            lwhm_array: left_hwhm_list.unwrap_or_default(),
            rwhm_array: right_hwhm_list.unwrap_or_default()
        }
    }

    // Convert ppm to Da (this is a placeholder function)
    fn _ppm_to_da(&self, mz: f64, ppm: f64) -> f64 {
        mz * ppm / 1_000_000.0
    }

    // Get the nearest peak based on mz and tolerance
    pub fn get_nearest_peak(
        &self,
        mz: f64,
        mz_tol_ppm: f64,
        rt: f32,
    ) -> Option<XicPeak> {
        if self.peaks_count == 0 {
            return None;
        }

        let mz_da = self._ppm_to_da(mz, mz_tol_ppm);
        let bin_search_index = self.mz_array.binary_search_by(|&probe|
            probe.partial_cmp(&mz).unwrap_or(std::cmp::Ordering::Equal)
        );

        let idx = match bin_search_index {
            Result::Ok(i) => i,
            Err(i) => i,
        };

        let (prev_val, next_val, new_idx) = if idx == self.peaks_count {
            let prev_val = self.mz_array[self.peaks_count - 1];
            if (mz - prev_val).abs() > mz_da {
                return None;
            }
            (prev_val, 0.0, idx - 1)
        } else if idx == 0 {
            let next_val = self.mz_array[idx];
            if (mz - next_val).abs() > mz_da {
                return None;
            }
            (0.0, next_val, idx)
        } else {
            let next_val = self.mz_array[idx];
            let prev_val = self.mz_array[idx - 1];
            let diff_next_val = (mz - next_val).abs();
            let diff_prev_val = (mz - prev_val).abs();
            if diff_next_val < diff_prev_val {
                if diff_next_val > mz_da {
                    return None;
                }
                (prev_val, next_val, idx)
            } else {
                if diff_prev_val > mz_da {
                    return None;
                }
                (prev_val, next_val, idx - 1)
            }
        };

        Some(XicPeak {
            mz: self.mz_array[new_idx],
            intensity: self.intensity_array[new_idx],
            rt
        })
    }
}

/*
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SpectrumHeaderRecord {
    pub id: i64,
    pub initial_id: Option<i64>,
    pub title: Option<String>,
    pub cycle: Option<i64>,
    pub time: Option<f32>,
    pub ms_level: Option<i64>,
    pub activation_type: Option<String>,
    pub tic: Option<f32>,
    pub base_peak_mz: Option<f64>,
    pub base_peak_intensity: Option<f32>,
    pub main_precursor_mz: Option<f64>,
    pub main_precursor_charge: Option<i32>,
    pub data_points_count: Option<i64>,
    pub param_tree: Option<String>,
    pub scan_list: Option<String>,
    pub precursor_list: Option<String>,
    pub product_list: Option<String>,
    pub shared_param_tree_id: Option<i64>,
    pub instrument_configuration_id: Option<i64>,
    pub source_file_id: Option<i64>,
    pub run_id: Option<i64>,
    pub data_processing_id: Option<i64>,
    pub data_encoding_id: Option<i64>,
    pub bb_first_spectrum_id: Option<i64>,
}*/

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
    /// Parses an XML string to extract BBSizes parameters.
    pub fn from_xml(xml: &str) -> Result<Self> {
        let doc = Document::parse(xml)?;

        let mut bb_mz_height_ms1 = 0.0;
        let mut bb_mz_height_msn = 0.0;
        let mut bb_rt_width_ms1 = 0.0;
        let mut bb_rt_width_msn = 0.0;

        // Traverse each <userParam> in <userParams>
        for user_param in doc.descendants().filter(|n| n.tag_name().name() == "userParam") {
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
    //spectrum_slice_t* spectrum_slices;
    pub spectrum_slices_count: usize, // number of spectrum slices in the blob
    pub spectra_ids: Vec<i64>,// list of spectra ids in the blob
    pub slices_indexes: Vec<usize>,// list of spectrum slice starting positions in the blob
    pub peaks_counts: Vec<usize>,// number of peaks in each spectrum slice of the blob
}

#[derive(Copy, Clone, Debug, PartialEq)]
pub enum XicMethod {
    MAX = 0,
    NEAREST = 1,
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
    pub spectrum_headers: Vec<SpectrumHeader>
}