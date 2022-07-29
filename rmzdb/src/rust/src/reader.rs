use rusqlite::Connection;

use anyhow::*;

use mzdb::anyhow_ext::*;
use mzdb::model::*;
use mzdb::{iterator, queries};
use mzdb::queries::{BOUNDING_BOX_TABLE_NAME, DATA_ENCODING_TABLE_NAME, SPECTRUM_TABLE_NAME};

use crate::{MzdbReader, MzdbSpectrum, MzdbSpectrumHeader, MzdbSpectrumData};
use crate::sqlite_helper::*;

#[macro_export]
macro_rules! here {
    () => {
        &Location {
            file: file!(),
            line: line!(),
            column: column!(),
        }
    };
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

    pub(crate) fn _get_mzdb_version(&self) -> Result<String> {
        let db = self._connection().location(here!())?;
        queries::get_mzdb_version(&db).map(|v_opt| v_opt.unwrap_or("".to_string()))
    }

    pub(crate) fn _get_pwiz_mzdb_version(&self) -> anyhow::Result<String> {
        let db = self._connection().location(here!())?;

        queries::get_pwiz_mzdb_version(&db).map(|v_opt| v_opt.unwrap_or("".to_string()))
    }

    pub(crate) fn _get_param_tree_chromatogram(&self) -> anyhow::Result<Vec<String>> {
        let db = self._connection().location(here!())?;

        queries::get_param_tree_chromatogram_res(&db)
    }

    pub (crate) fn _get_param_tree_spectrum(&self, spectrum_id: i64) -> Result<String> {
        let db = self._connection().location(here!())?;

        queries::get_param_tree_spectrum(&db, spectrum_id).map(|v_opt| v_opt.unwrap_or("".to_string()))
    }

    pub (crate) fn _get_param_tree_mzdb(&self) -> Result<String> {
        let db = self._connection().location(here!())?;

        queries::get_param_tree_mzdb(&db).map(|v_opt| v_opt.unwrap_or("".to_string()))
    }


    pub (crate)fn _get_last_cycle_number(&self) -> Result<i64>{
        let db = self._connection().location(here!())?;

        _result_option_to_result(
            queries::get_last_cycle_number(&db),
            || "unexpected error: no spectrum.cycle found".to_string()
        )
    }

    pub (crate)fn _get_last_time(&self)-> Result<f32>{
        let db = self._connection().location(here!())?;

        _result_option_to_result(
            queries::get_last_time(&db),
            || "unexpected error: no spectrum.time found".to_string()
        )
    }


    pub(crate) fn _get_max_ms_level(&self) -> Result<i64> {
        let db = self._connection().location(here!())?;

        _result_option_to_result(
            queries::get_max_ms_level(&db),
            || "unexpected error: no spectrum.ms_level found".to_string()
        )
    }

    pub(crate)fn _get_run_slice_bounding_boxes_count(&self, run_slice_id: i64) -> Result<i64> {
        let db = self._connection().location(here!())?;

        _result_option_to_result(
            queries::get_run_slice_bounding_boxes_count(&db, run_slice_id),
            || "unexpected error: no spectrum.id found".to_string(),
        )
    }

    pub(crate) fn _get_spectra_count_single_ms_level(&self, ms_level: i64) -> Result<i64> {
        let db = self._connection().location(here!())?;

        _result_option_to_result(
            queries::get_spectra_count_single_ms_level(&db, ms_level),
            || "unexpected error: no spectrum.id found".to_string(),
        )
    }

    pub(crate) fn _get_data_encodings_count(&self)-> Result<i64> {
        self._get_table_records_count(DATA_ENCODING_TABLE_NAME)
    }

    pub(crate) fn _get_bounding_boxes_count(&self)-> Result<i64> {
        self._get_table_records_count(BOUNDING_BOX_TABLE_NAME)
    }

    pub(crate) fn _get_spectra_count(&self)-> Result<i64> {
        self._get_table_records_count(SPECTRUM_TABLE_NAME)
    }

    pub(crate) fn _get_bounding_box_first_spectrum_id(&self, first_id: i64) -> Result<i64> {
        let db = self._connection().location(here!())?;

        _result_option_to_result(
            queries::get_bounding_box_first_spectrum_id(&db, first_id),
            || "unexpected error: no spectrum.id found".to_string(),
        )
    }

    pub(crate) fn _get_bounding_box_min_mz(&self, bb_r_tree_id: i64) -> Result<f32> {
        let db = self._connection().location(here!())?;

        _result_option_to_result(
            queries::get_bounding_box_min_mz(&db, bb_r_tree_id),
            || "unexpected error: no bb_r_tree_id found".to_string(),
        )
    }

    pub(crate) fn _get_bounding_box_min_time(&self, bb_r_tree_id: i64) -> Result<f64> {
        let db = self._connection().location(here!())?;

        _result_option_to_result(
            queries::get_bounding_box_min_time(&db, bb_r_tree_id),
            || "unexpected error: no bb_r_tree_id found".to_string(),
        )
    }

    pub(crate) fn _get_run_slice_id(&self, bb_id: i64) -> Result<i64> {
        let db = self._connection().location(here!())?;

        _result_option_to_result(
            queries::get_run_slice_id(&db, bb_id),
            || "unexpected error: no spectrum.id found".to_string(),
        )
    }

    pub(crate) fn _get_ms_level_from_run_slice_id(&self, run_slice_id: i64) -> Result<i64> {
        let db = self._connection().location(here!())?;

        _result_option_to_result(
            queries::get_ms_level_from_run_slice_id(&db, run_slice_id),
            || "unexpected error: no spectrum.id found".to_string(),
        )
    }

    pub(crate) fn _get_bounding_box_ms_level(&self, bb_id: i64) -> Result<i64> {
        let db = self._connection().location(here!())?;

        _result_option_to_result(
            queries::get_bounding_box_ms_level(&db, bb_id),
            || "unexpected error: no spectrum.id found".to_string(),
        )
    }

    pub(crate) fn _get_data_encoding_id(&self, bb_id: i64)-> Result<i64> {
        let db = self._connection().location(here!())?;

        _result_option_to_result(
            queries::get_data_encoding_id(&db, bb_id),
            || "unexpected error: no bounding_box.data_encoding_id found".to_string()
        )
    }

    pub(crate) fn _get_data_encoding_count(&self)-> Result<i64> {
        let db = self._connection().location(here!())?;

        _result_option_to_result(
            queries::get_data_encoding_count(&db),
            || "unexpected error: no bounding_box.data_encoding_id found".to_string()
        )
    }

    pub(crate) fn _get_spectrum(&self, spectrum_id: i64)-> Result<MzdbSpectrum> {
        let db = self._connection().location(here!())?;

        let spectrum = queries::get_spectrum(&db, spectrum_id, &self._entity_cache).location(here!())?;
        let mzdb_spectrum = MzdbSpectrum {
            header: MzdbSpectrumHeader::new(&spectrum.header),
            data: MzdbSpectrumData::new(&spectrum.data),
        };

        Ok(mzdb_spectrum)
    }

    pub(crate) fn _for_each_spectrum<F>(&self, ms_level: Option<u8>, mut on_each_spectrum: F) -> Result<()>
        where F: FnMut(MzdbSpectrum) -> Result<()> {

        let db = self._connection().location(here!())?;

        iterator::for_each_spectrum(&db, &self._entity_cache, ms_level, |s: &Spectrum| {

            let mzdb_spectrum = MzdbSpectrum {
                header: MzdbSpectrumHeader::new(&s.header),
                data: MzdbSpectrumData::new(&s.data),
            };

            on_each_spectrum(mzdb_spectrum).location(here!())?;

            Ok(())
        }).location(here!())?;

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
