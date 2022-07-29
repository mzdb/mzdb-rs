#![allow(
dead_code
)]

mod reader;
mod sqlite_helper;

use extendr_api::prelude::*;
use rusqlite::Connection;

use anyhow;
use mzdb::anyhow_ext::*;
use mzdb::model::*;
use mzdb::mzdb::create_entity_cache;
use mzdb::queries;

//use crate::reader::*;

use crate::sqlite_helper::*;

/// Return string `"Hello world!"` to R.
/// @export
#[extendr]
fn hello_world() -> &'static str {
    "Hello world!"
}

#[extendr]
fn get_mzdb_version(path: String) -> String { //&'static str
    let version_res = _get_mzdb_version(path);

    let res = match version_res {
        Ok(ok) => ok,
        Err(err) => { throw_r_error(err.to_string() ); "".to_string() },
    };

    res
}

fn _get_mzdb_version(path: String) -> anyhow::Result<String> {

    let db = unsafe {
        Connection::from_handle(_connection_to_dbh( Connection::open(path)?)?)?
    };

    Ok(queries::get_mzdb_version(&db).map(|v_opt| v_opt.unwrap_or("".to_string()))?)
}


fn _create_mzdb_reader(path: String) -> anyhow::Result<MzdbReader> {
    let db = Connection::open(path)?;
    let entity_cache = create_entity_cache(&db)?;

    Ok(MzdbReader {
        //name: "".to_string(),
        is_closed: false,
        _dbh_address: {
            let mem_address =  unsafe {
                let sqlite3_handle = _connection_to_dbh(db)?;
                std::mem::transmute::<*mut rusqlite::ffi::sqlite3, i64>(sqlite3_handle)
            };
            mem_address
        },
        _entity_cache: entity_cache
    })
}

#[derive(Clone, Debug)]
pub struct MzdbReader {
    //pub name: String,
    is_closed: bool,
    _dbh_address: i64, // memory address of *mut rusqlite::ffi::sqlite3
    _entity_cache: EntityCache
}

#[extendr]
impl MzdbReader {

    fn new(path: String) -> Result<Self> {
        _create_mzdb_reader(path).map_err(|e| {
            Error::from(e.to_string())
        })
    }

    unsafe fn close(&mut self) -> Result<()> {
        let dbh: *mut rusqlite::ffi::sqlite3 = std::mem::transmute::<i64, *mut rusqlite::ffi::sqlite3>(self._dbh_address);

        let rc = rusqlite::ffi::sqlite3_close(dbh);

        if rc == rusqlite::ffi::SQLITE_OK {
            self.is_closed = true;
            Ok(())
        } else {
            let msg = _sqlite_errmsg_from_handle(dbh).unwrap_or(format!("unkown error (code={}", rc));

            Err(Error::from(msg))
        }
    }

    /*fn set_name(&mut self, name: &str) {
        self.name = name.to_string();
    }

    fn name(&self) -> &str {
        self.name.as_str()
    }*/

    fn is_closed(&self) -> bool {
        self.is_closed
    }

    fn get_mzdb_version(&self) -> String {
        _unwrap_result_safely(self._get_mzdb_version())
    }

    fn get_pwiz_mzdb_version(&self) -> String {
        _unwrap_result_safely(self._get_pwiz_mzdb_version())
    }

    fn get_param_tree_chromatogram(&self) -> Vec<String> {
        _unwrap_result_safely(self._get_param_tree_chromatogram())
    }

    fn get_param_tree_spectrum(&self,spectrum_id: i64) -> String {
        _unwrap_result_safely(self._get_param_tree_spectrum(spectrum_id))
    }

    fn get_param_tree_mzdb(&self) -> String {
    _unwrap_result_safely(self._get_param_tree_mzdb())
    }

    fn get_last_cycle_number(&self) -> i64 {
        _unwrap_result_safely(self._get_last_cycle_number())
    }

    fn get_last_time(&self) -> f32 {
        _unwrap_result_safely(self._get_last_time())
    }

    fn get_max_ms_level(&self) -> i64 {
        _unwrap_result_safely(self._get_max_ms_level())
    }

    fn get_run_slice_bounding_boxes_count(&self, run_slice_id: i64) -> i64 {
        _unwrap_result_safely(self._get_run_slice_bounding_boxes_count(run_slice_id))
    }

    fn get_spectra_count_single_ms_level(&self, ms_level: i64) -> i64 {
        _unwrap_result_safely(self._get_spectra_count_single_ms_level(ms_level))
    }

    fn get_data_encodings_count(&self) -> i64 {
        _unwrap_result_safely(self._get_data_encodings_count())
    }

    fn get_bounding_boxes_count(&self) -> i64 {
        _unwrap_result_safely(self._get_bounding_boxes_count())
    }

    fn get_spectra_count(&self) -> i64 {
        _unwrap_result_safely(self._get_spectra_count())
    }

    fn get_bounding_box_first_spectrum_id(&self, first_id: i64) -> i64 {
        _unwrap_result_safely(self._get_bounding_box_first_spectrum_id(first_id))
    }

    fn get_bounding_box_min_mz(&self, bb_r_tree_id: i64) -> f32 {
        _unwrap_result_safely(self._get_bounding_box_min_mz(bb_r_tree_id))
    }

    fn get_bounding_box_min_time(&self, bb_r_tree_id: i64) -> f64 {
        _unwrap_result_safely(self._get_bounding_box_min_time(bb_r_tree_id))
    }

    fn get_run_slice_id(&self, bb_id: i64) -> i64 {
        _unwrap_result_safely(self._get_run_slice_id(bb_id))
    }

    fn get_ms_level_from_run_slice_id(&self, run_slice_id: i64)  -> i64 {
        _unwrap_result_safely(self._get_ms_level_from_run_slice_id(run_slice_id))
    }

    fn get_bounding_box_ms_level(&self, bb_id: i64) -> i64 {
        _unwrap_result_safely(self._get_bounding_box_ms_level(bb_id))
    }

    fn get_data_encoding_id(&self, bb_id: i64)  -> i64 {
        _unwrap_result_safely(self._get_data_encoding_id(bb_id))
    }

    fn get_data_encoding_count(&self) -> i64 {
        _unwrap_result_safely(self._get_data_encoding_count())
    }

    fn get_spectrum(&self, spectrum_id: i64) -> MzdbSpectrum {
        _unwrap_result_safely(self._get_spectrum(spectrum_id))
    }

    /*fn run_function(&self, func: Function) -> () {
        /*
        let function = R!("function(a, b) a + b").unwrap().as_function().unwrap();
    assert_eq!(function.call(pairlist!(a=1, b=2)).unwrap(), r!(3));
         */
        let func_res = func.call(pairlist!());

        //_unwrap_result_safely(self._get_spectrum(spectrum_id))


    }*/

    fn for_each_spectrum(&self, ms_level: Option<i32>, on_each_spectrum_fn: Function)-> () {

        let iteration_res = self._for_each_spectrum(ms_level.map(|m| m as u8), |mzdb_spectrum: MzdbSpectrum| {

            let _func_result = on_each_spectrum_fn.call(pairlist!(mzdb_spectrum)).map_err(|err| {
                throw_r_error(err.to_string());
                err
            }).unwrap();

            Ok(())
        });

        _unwrap_result_safely(iteration_res)
    }

    /*fn get_spectra_headers_as_dataframe(&self, ms_level: Option<i32>) -> Dataframe<MzdbSpectrumHeader> {

        let mut spectra_headers = Vec::new();
        let iteration_res = self._for_each_spectrum(ms_level.map(|m| m as u8), |mzdb_spectrum: MzdbSpectrum| {

            let h = mzdb_spectrum.header.clone();
            spectra_headers.push(h);

            Ok(())
        });

        if iteration_res.is_err() {
            _unwrap_result_safely(iteration_res);
        }

        let df = spectra_headers.into_dataframe().map_err(|err| {
            throw_r_error(err.to_string());
            err
        }).unwrap();

        df
    }*/

}

fn _unwrap_result_safely<V>(wrapped_value: anyhow::Result<V>) -> V {
    wrapped_value.map_err(|err| {
        throw_r_error(err.to_string());
        err
    }).unwrap()
}


#[derive(Clone, Debug, PartialEq)]
pub struct MzdbSpectrum {
    pub header: MzdbSpectrumHeader,
    pub data: MzdbSpectrumData
}

#[extendr]
impl MzdbSpectrum {
    fn header(&self) -> MzdbSpectrumHeader { self.header.clone() }
    fn data(&self) -> MzdbSpectrumData { self.data.clone() }
}

#[derive(Clone, Debug, PartialEq, IntoDataFrameRow)]
pub struct MzdbSpectrumHeader {
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
    pub precursor_mz: Option<f64>,
    pub precursor_charge: Option<i32>,
    pub peaks_count: i64,
    pub param_tree_str: String,
    pub scan_list_str: Option<String>,
    pub precursor_list_str: Option<String>,
    pub product_list_str: Option<String>,
    pub shared_param_tree_id: Option<i64>,
    pub instrument_configuration_id: i64,
    pub source_file_id: i64,
    pub run_id: i64,
    pub data_processing_id: i64,
    pub data_encoding_id: i64,
    pub bb_first_spectrum_id: i64,
}

impl MzdbSpectrumHeader {
    fn new(spectrum_header: &SpectrumHeader) -> Self {
        MzdbSpectrumHeader {
            id: spectrum_header.id,
            initial_id: spectrum_header.initial_id,
            title: spectrum_header.title.clone(),
            cycle: spectrum_header.cycle,
            time: spectrum_header.time,
            ms_level: spectrum_header.ms_level,
            activation_type: spectrum_header.activation_type.clone(),
            tic: spectrum_header.tic,
            base_peak_mz: spectrum_header.base_peak_mz,
            base_peak_intensity: spectrum_header.base_peak_intensity,
            precursor_mz: spectrum_header.precursor_mz,
            precursor_charge: spectrum_header.precursor_charge,
            peaks_count: spectrum_header.peaks_count,
            param_tree_str: spectrum_header.param_tree_str.clone(),
            scan_list_str: spectrum_header.scan_list_str.clone(),
            precursor_list_str: spectrum_header.precursor_list_str.clone(),
            product_list_str: spectrum_header.product_list_str.clone(),
            shared_param_tree_id: spectrum_header.shared_param_tree_id,
            instrument_configuration_id: spectrum_header.instrument_configuration_id,
            source_file_id: spectrum_header.source_file_id,
            run_id: spectrum_header.run_id,
            data_processing_id: spectrum_header.data_processing_id,
            data_encoding_id: spectrum_header.data_encoding_id,
            bb_first_spectrum_id: spectrum_header.data_encoding_id,
        }
    }
}

#[extendr]
impl MzdbSpectrumHeader {
    fn id(&self) -> i64 { self.id }
    fn initial_id(&self) -> i64 { self.initial_id }
    fn title(&self) -> String { self.title.clone() }
    fn cycle(&self) -> i64 { self.cycle }
    fn time(&self) -> f32 { self.time }
    fn ms_level(&self) -> i64 { self.ms_level }
    fn activation_type(&self) -> String { self.activation_type.as_ref().unwrap_or(&"".to_string()).clone() }
    fn tic(&self) -> f32 { self.tic }
    fn base_peak_mz(&self) -> f64 { self.base_peak_mz }
    fn base_peak_intensity(&self) -> f32 { self.base_peak_intensity }
    fn precursor_mz(&self) -> Option<f64> { self.precursor_mz }
    fn precursor_charge(&self) -> Option<i32> { self.precursor_charge }
    fn peaks_count(&self) -> i64 { self.peaks_count }
    fn param_tree_str(&self) -> String { self.param_tree_str.clone() }
    fn scan_list_str(&self) -> String { self.scan_list_str.as_ref().unwrap_or(&"".to_string()).clone() }
    fn precursor_list_str(&self) -> String { self.precursor_list_str.as_ref().unwrap_or(&"".to_string()).clone() }
    fn product_list_str(&self) -> String { self.product_list_str.as_ref().unwrap_or(&"".to_string()).clone() }
    fn shared_param_tree_id(&self) -> Option<i64> { self.shared_param_tree_id }
    fn instrument_configuration_id(&self) -> i64 { self.instrument_configuration_id }
    fn source_file_id(&self) -> i64 { self.source_file_id }
    fn run_id(&self) -> i64 { self.run_id }
    fn data_processing_id(&self) -> i64 { self.data_processing_id }
    fn data_encoding_id(&self) -> i64 { self.data_encoding_id }
    fn bb_first_spectrum_id(&self) -> i64 { self.bb_first_spectrum_id }
}

#[derive(Clone, Debug, PartialEq)]
pub struct MzdbSpectrumData {
    pub mz_list: Vec<f64>,
    pub intensity_list: Vec<f32>,
}

impl MzdbSpectrumData {
    fn new(spectrum_data: &SpectrumData) -> Self {
        MzdbSpectrumData {
            mz_list: spectrum_data.mz_array.clone(),
            intensity_list: spectrum_data.intensity_array.clone(),
        }
    }
}

#[extendr]
impl MzdbSpectrumData {

    fn mz_list(&self) -> Vec<f64> {
        self.mz_list.clone()
    }

    fn intensity_list(&self) -> Vec<f32> {
        self.intensity_list.clone()
    }

    fn as_matrix(&self) -> RMatrix<f64> {
        let n_rows = self.mz_list.len();
        let matrix = RMatrix::new_matrix(n_rows, 2, |r, c| [
            self.mz_list.as_slice(),
            self.intensity_list.iter().map(|&intensity| intensity as f64).collect::<Vec<f64>>().as_slice()
        ][c][r]);

        matrix
    }
}


// Macro to generate exports.
// This ensures exported functions are registered with R.
// See corresponding C code in `entrypoint.c`.
extendr_module! {
    mod rmzdb;
    fn hello_world;
    fn get_mzdb_version;
    impl MzdbReader;
    impl MzdbSpectrum;
    impl MzdbSpectrumHeader;
    impl MzdbSpectrumData;
}

