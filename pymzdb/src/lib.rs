#![allow(
dead_code,
unused_imports
)]

mod sqlite_helper;

extern crate mzdb;

use std::fmt::format;
use anyhow::*;
use std::io::Write;
use mzdb::anyhow_ext::ErrorLocation;

use pyo3::prelude::*;
use rusqlite::Connection;

use crate::sqlite_helper::*;

use mzdb::anyhow_ext::Location;
use mzdb::{here, iterator};
use mzdb::model::*;
use mzdb::mzdb::create_entity_cache;
use mzdb::queries;
use mzdb::iterator::*;
use mzdb::queries::{BOUNDING_BOX_TABLE_NAME, DATA_ENCODING_TABLE_NAME, SPECTRUM_TABLE_NAME};


/// Formats the sum of two numbers as string.
#[pyfunction]
fn sum_as_string(a: usize, b: usize) -> Result<String> {
    Result::Ok((a + b).to_string())
}

#[derive(Clone)]
#[pyclass]
pub struct MzdbReader {
    #[pyo3(get)]
    pub is_closed: bool,
    _dbh_address: i64, // memory address of *mut rusqlite::ffi::sqlite3
    _entity_cache: EntityCache
}

#[pymethods]
impl MzdbReader {

    #[new]
    fn new(path: String) -> Result<Self> {
        let db = Connection::open(path)?;
        let entity_cache = create_entity_cache(&db)?;

       Ok(MzdbReader {
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

    unsafe fn close(&mut self) -> Result<()> {
        let dbh: *mut rusqlite::ffi::sqlite3 = std::mem::transmute::<i64, *mut rusqlite::ffi::sqlite3>(self._dbh_address);

        let rc = rusqlite::ffi::sqlite3_close(dbh);

        if rc == rusqlite::ffi::SQLITE_OK {
            self.is_closed = true;
            Ok(())
        } else {
            let msg = _sqlite_errmsg_from_handle(dbh).unwrap_or(format!("unkown error (code={}", rc));

            Err(anyhow!("can't close connection because {}",msg))
        }
    }

    //----------------------------------------------------------------------//

    //#[pyo3(text_signature = "($self)")]
    fn get_mzdb_version(&self) -> Result<String> {
        let db = self._connection().location(here!())?;
        queries::get_mzdb_version(&db).map(|v_opt| v_opt.unwrap_or("".to_string()))
    }

    //#[pyo3(text_signature = "($self)")]
    fn get_pwiz_mzdb_version(&self) -> Result<String> {
        let db = self._connection().location(here!())?;
        queries::get_pwiz_mzdb_version(&db).map(|v_opt| v_opt.unwrap_or("".to_string()))
    }

    fn get_param_tree_chromatogram(&self) -> Result<Vec<String>> {
        let db = self._connection().location(here!())?;
        queries::get_param_tree_chromatogram_res(&db)
    }

    fn get_param_tree_spectrum(&self, spectrum_id: i64) -> Result<String> {
        let db = self._connection().location(here!())?;
        queries::get_param_tree_spectrum(&db, spectrum_id).map(|v_opt| v_opt.unwrap_or("".to_string()))
    }

    fn get_param_tree_mzdb(&self) -> Result<String> {
        let db = self._connection().location(here!())?;
        queries::get_param_tree_mzdb(&db).map(|v_opt| v_opt.unwrap_or("".to_string()))
    }

    fn get_last_cycle_number(&self) -> Result<i64>{
        let db = self._connection().location(here!())?;

        _result_option_to_result(
            queries::get_last_cycle_number(&db),
            || "unexpected error: no spectrum.cycle found".to_string()
        )
    }

    fn get_last_time(&self)-> Result<f32>{
        let db = self._connection().location(here!())?;

        _result_option_to_result(
            queries::get_last_time(&db),
            || "unexpected error: no spectrum.time found".to_string()
        )
    }

    fn get_max_ms_level(&self) -> Result<i64> {
        let db = self._connection().location(here!())?;

        _result_option_to_result(
            queries::get_max_ms_level(&db),
            || "unexpected error: no spectrum.ms_level found".to_string()
        )
    }

    fn get_run_slice_bounding_boxes_count(&self, run_slice_id: i64) -> Result<i64> {
        let db = self._connection().location(here!())?;

        _result_option_to_result(
            queries::get_run_slice_bounding_boxes_count(&db, run_slice_id),
            || "unexpected error: no spectrum.id found".to_string(),
        )
    }

    fn get_spectra_count_single_ms_level(&self, ms_level: i64) -> Result<i64> {
        let db = self._connection().location(here!())?;

        _result_option_to_result(
            queries::get_spectra_count_single_ms_level(&db, ms_level),
            || "unexpected error: no spectrum.id found".to_string(),
        )
    }

    fn get_data_encodings_count(&self)-> Result<i64> {
        self._get_table_records_count(DATA_ENCODING_TABLE_NAME)
    }

    fn get_bounding_boxes_count(&self)-> Result<i64> {
        self._get_table_records_count(BOUNDING_BOX_TABLE_NAME)
    }

    fn get_spectra_count(&self)-> Result<i64> {
        self._get_table_records_count(SPECTRUM_TABLE_NAME)
    }

    fn get_bounding_box_first_spectrum_id(&self, first_id: i64) -> Result<i64> {
        let db = self._connection().location(here!())?;

        _result_option_to_result(
            queries::get_bounding_box_first_spectrum_id(&db, first_id),
            || "unexpected error: no spectrum.id found".to_string(),
        )
    }

    fn get_bounding_box_min_mz(&self, bb_r_tree_id: i64) -> Result<f32> {
        let db = self._connection().location(here!())?;

        _result_option_to_result(
            queries::get_bounding_box_min_mz(&db, bb_r_tree_id),
            || "unexpected error: no bb_r_tree_id found".to_string(),
        )
    }

    fn get_bounding_box_min_time(&self, bb_r_tree_id: i64) -> Result<f64> {
        let db = self._connection().location(here!())?;

        _result_option_to_result(
            queries::get_bounding_box_min_time(&db, bb_r_tree_id),
            || "unexpected error: no bb_r_tree_id found".to_string(),
        )
    }

    fn get_run_slice_id(&self, bb_id: i64) -> Result<i64> {
        let db = self._connection().location(here!())?;

        _result_option_to_result(
            queries::get_run_slice_id(&db, bb_id),
            || "unexpected error: no spectrum.id found".to_string(),
        )
    }

    fn get_ms_level_from_run_slice_id(&self, run_slice_id: i64) -> Result<i64> {
        let db = self._connection().location(here!())?;

        _result_option_to_result(
            queries::get_ms_level_from_run_slice_id(&db, run_slice_id),
            || "unexpected error: no spectrum.id found".to_string(),
        )
    }

    fn get_bounding_box_ms_level(&self, bb_id: i64) -> Result<i64> {
        let db = self._connection().location(here!())?;

        _result_option_to_result(
            queries::get_bounding_box_ms_level(&db, bb_id),
            || "unexpected error: no spectrum.id found".to_string(),
        )
    }

    fn get_data_encoding_id(&self, bb_id: i64)-> Result<i64> {
        let db = self._connection().location(here!())?;

        _result_option_to_result(
            queries::get_data_encoding_id(&db, bb_id),
            || "unexpected error: no bounding_box.data_encoding_id found".to_string()
        )
    }

    fn get_data_encoding_count(&self)-> Result<i64> {
        let db = self._connection().location(here!())?;

        _result_option_to_result(
            queries::get_data_encoding_count(&db),
            || "unexpected error: no bounding_box.data_encoding_id found".to_string()
        )
    }


    fn get_spectrum(&self, spectrum_id: i64)-> Result<MzdbSpectrum> {
        let db = self._connection().location(here!())?;

        let spectrum = queries::get_spectrum(&db, spectrum_id, &self._entity_cache).location(here!())?;
        let mzdb_spectrum = MzdbSpectrum {
            header: MzdbSpectrumHeader::new(&spectrum.header),
            data: MzdbSpectrumData::new(&spectrum.data),
        };

        Ok(mzdb_spectrum)

    }

    fn get_spectrum_data(&self, spectrum_id: i64)-> Result<MzdbSpectrumData> {
        let db = self._connection().location(here!())?;

        //let entity_cache = create_entity_cache(&db).location(here!())?;
        let spectrum = queries::get_spectrum(&db, spectrum_id, &self._entity_cache).location(here!())?;

        Ok(MzdbSpectrumData::new(&spectrum.data))
    }

    fn for_each_spectrum(&self, py: Python<'_>, ms_level: Option<u8>, on_each_spectrum: PyObject)-> Result<()> {
        let db = self._connection().location(here!())?;

        let mut count = 0;
        for_each_spectrum(&db, &self._entity_cache, ms_level, |s: &Spectrum| {

            // WARNNING: this only works for more than one single parameter
            let args = (MzdbSpectrum{
                header: MzdbSpectrumHeader::new(&s.header),
                data: MzdbSpectrumData::new(&s.data),
            }, count);

            on_each_spectrum.call(py, args, None)?;

            count += 1;

            Ok(())
        }).location(here!())?;

        Ok(())

    }

    fn for_each_spectrum_data(&self, py: Python<'_>, ms_level: Option<u8>, on_each_spectrum_data: PyObject)-> Result<()> {
        let db = self._connection().location(here!())?;

        let mut count = 0;
        for_each_spectrum(&db, &self._entity_cache, ms_level, |s: &Spectrum| {
            // WARNNING: this only works for more than one single parameter
            let args = (MzdbSpectrumData::new(&s.data), count);
            on_each_spectrum_data.call(py, args, None)?;

            count += 1;

            Ok(())
        })?;

        Ok(())
    }
}

fn _result_option_to_result<'a, V, F>(wrapped_value: anyhow::Result<Option<V>>, error_msg: F) -> anyhow::Result<V>
    where F: Fn() -> String, V: std::fmt::Debug {

    if wrapped_value.is_ok() {
        match wrapped_value.unwrap() {
            Some(v) => Ok(v),
            None => Err(anyhow!(error_msg())),
        }
    }
    else {
        Err(wrapped_value.unwrap_err())
    }
}

impl MzdbReader {

    fn _connection(&self) -> Result<Connection> {
        if self.is_closed {
            bail!("database is closed");
        }

        let db = unsafe { _cast_connection(self._dbh_address).location(here!())? };

        Ok(db)
    }

    fn _get_table_records_count(&self, table_name: &str) -> Result<i64> {
        let db = self._connection().location(here!())?;

        _result_option_to_result(
            queries::get_table_records_count(&db, table_name),
            || format!("unexpected error: no record found for table {}", table_name)
        )
    }
}

#[derive(Clone, Debug, PartialEq)]
#[pyclass]
pub struct MzdbSpectrum {
    #[pyo3(get)]
    pub header: MzdbSpectrumHeader,
    #[pyo3(get)]
    pub data: MzdbSpectrumData
}

#[derive(Clone, Debug, PartialEq)]
#[pyclass]
pub struct MzdbSpectrumHeader {
    #[pyo3(get)]
    pub id: i64,
    #[pyo3(get)]
    pub initial_id: i64,
    #[pyo3(get)]
    pub title: String,
    #[pyo3(get)]
    pub cycle: i64,
    #[pyo3(get)]
    pub time: f32,
    #[pyo3(get)]
    pub ms_level: i64,
    #[pyo3(get)]
    pub activation_type: Option<String>,
    #[pyo3(get)]
    pub tic: f32,
    #[pyo3(get)]
    pub base_peak_mz: f64,
    #[pyo3(get)]
    pub base_peak_intensity: f32,
    #[pyo3(get)]
    pub precursor_mz: Option<f64>,
    #[pyo3(get)]
    pub precursor_charge: Option<i32>,
    #[pyo3(get)]
    pub peaks_count: i64,
    #[pyo3(get)]
    pub param_tree_str: String,
    #[pyo3(get)]
    pub scan_list_str: Option<String>,
    #[pyo3(get)]
    pub precursor_list_str: Option<String>,
    #[pyo3(get)]
    pub product_list_str: Option<String>,
    #[pyo3(get)]
    pub shared_param_tree_id: Option<i64>,
    #[pyo3(get)]
    pub instrument_configuration_id: i64,
    #[pyo3(get)]
    pub source_file_id: i64,
    #[pyo3(get)]
    pub run_id: i64,
    #[pyo3(get)]
    pub data_processing_id: i64,
    #[pyo3(get)]
    pub data_encoding_id: i64,
    #[pyo3(get)]
    pub bb_first_spectrum_id: i64,
}

use pyo3::types::{IntoPyDict, PyDict};
use std::collections::HashMap;

#[pymethods]
impl MzdbSpectrumHeader {

    fn as_dict(&self, py: Python<'_>) -> Result<PyObject> {

        // TODO: alternatively use structmap = "0.1.5", to convert the struct into a generic Map
        let fun: Py<PyAny> = PyModule::from_code(
            py,
            "def spectrum_header_to_dict(*args, **kwargs):
                sh = args[0]
                return dict((name, getattr(sh, name)) for name in dir(sh) if not name.startswith('__'))
                ",
            "",
            "",
        )?.getattr("spectrum_header_to_dict")?.into();

        let args = (self.clone(),false);
        let res = fun.call1(py, args)?;

        Ok(res)
    }

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

#[derive(Clone, Debug, PartialEq)]
#[pyclass]
pub struct MzdbSpectrumData {
    #[pyo3(get)]
    pub mz_list: Vec<f64>,
    #[pyo3(get)]
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

// TODO: delete me
#[pyfunction]
fn get_mzdb_version(path: String) -> Result<String> {
    let db = unsafe {
        Connection::from_handle(_connection_to_dbh( Connection::open(path)?).location(here!())?).location(here!())?
    };

    queries::get_mzdb_version(&db).map(|v_opt| v_opt.unwrap_or("".to_string()))
}

// TODO: delete me
#[pyfunction]
fn print_spectrum(path: String) -> Result<()> {

    let db = Connection::open(path).location(here!())?;
    let entity_cache = create_entity_cache(&db).location(here!())?;

    let s = queries::get_spectrum(&db, 1, &entity_cache).location(here!())?;

    _print_spectrum(&s).location(here!())?;

    Result::Ok(())
}

// TODO: delete me
fn _print_spectrum(spectrum: &Spectrum) -> Result<()> {
    print!("Spectrum: {} --------------------------------------------------------\n\n", spectrum.header.id);

    println!("MZ\tIntensity");
    for peak_idx in 0..spectrum.data.peak_count {
        let d_mz = *spectrum.data.mz_array.get(peak_idx).unwrap();
        let f_in = *spectrum.data.intensity_array.get(peak_idx).unwrap();

        if d_mz < 0.0 || d_mz > 10e10 || d_mz.is_infinite() || d_mz.is_nan() {
            print!("FATAL ERROR\t");
        } else {
            print!("{}\t", d_mz)
        }

        if f_in < 0.0 || f_in > 10e10 || f_in.is_infinite() || f_in.is_nan() {
            println!("FATAL ERROR");
        } else {
            println!("{}", f_in)
        }
    }

    print!("\n");

    std::io::stdout().flush().location(here!())?;

    Ok(())
}

/// A Python module implemented in Rust.
#[pymodule]
fn pymzdb(_py: Python, m: &PyModule) -> PyResult<()> {

    m.add_class::<MzdbReader>().location(here!())?;

    //m.add_function(wrap_pyfunction!(connect, m)?)?;
    m.add_function(wrap_pyfunction!(get_mzdb_version, m).location(here!())?).location(here!())?;
    m.add_function(wrap_pyfunction!(print_spectrum, m).location(here!())?).location(here!())?;
    m.add_function(wrap_pyfunction!(sum_as_string, m).location(here!())?).location(here!())?;

    PyResult::Ok(())
}