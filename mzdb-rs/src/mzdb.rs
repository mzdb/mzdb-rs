use std::collections::HashMap;
use anyhow::*;
use crate::anyhow_ext::*;

use rusqlite::Connection; // OptionalExtension
use serde_rusqlite::from_rows;

use crate::model::{BBSizes, DataEncoding, DataEncodingsCache, EntityCache, SpectrumHeader};
use crate::queries::{get_param_tree_mzdb, list_data_encodings};

/*macro_rules! here {
    () => {
        &Location {
            file: file!(),
            line: line!(),
            column: column!(),
        }
    };
}*/


pub fn get_spectrum_headers(db: &Connection) -> Result<Vec<SpectrumHeader>> {

    // FIXME: load everything
    let mut statement = db.prepare("SELECT id, initial_id, title, cycle, time, ms_level, activation_type, tic, base_peak_mz, base_peak_intensity, main_precursor_mz, main_precursor_charge, data_points_count, precursor_list, shared_param_tree_id, instrument_configuration_id, source_file_id, run_id, data_processing_id, data_encoding_id, bb_first_spectrum_id FROM spectrum").unwrap();
    //let mut statement = db.prepare("SELECT * FROM spectrum").unwrap();
    let s_headers = from_rows::<SpectrumHeader>(statement.query([]).unwrap()).collect::<rusqlite::Result<Vec<SpectrumHeader>, _>>()?;

    /*let records = from_rows::<SpectrumHeaderRecord>(statement.query([]).unwrap());

    let mut s_headers = Vec::new();
    for record_res in records {
        let sh_record = record_res.location(here!())?;

        let sh = SpectrumHeader {
            id: sh_record.id,
            initial_id: sh_record.initial_id.unwrap(),
            title: sh_record.title.unwrap(),
            cycle: sh_record.cycle.unwrap(),
            time: sh_record.time.unwrap(),
            ms_level: sh_record.ms_level.unwrap(),
            activation_type: sh_record.activation_type,
            tic: sh_record.tic.unwrap(),
            base_peak_mz: sh_record.base_peak_mz.unwrap(),
            base_peak_intensity: sh_record.base_peak_intensity.unwrap(),
            precursor_mz: sh_record.main_precursor_mz,
            precursor_charge: sh_record.main_precursor_charge,
            peaks_count: sh_record.data_points_count.unwrap(),
            param_tree_str: sh_record.param_tree.unwrap(),
            scan_list_str: sh_record.scan_list,
            precursor_list_str: sh_record.precursor_list,
            product_list_str: sh_record.product_list,
            shared_param_tree_id: sh_record.shared_param_tree_id,
            instrument_configuration_id: sh_record.instrument_configuration_id.unwrap(),
            source_file_id: sh_record.source_file_id.unwrap(),
            run_id: sh_record.run_id.unwrap(),
            data_processing_id: sh_record.data_processing_id.unwrap(),
            data_encoding_id: sh_record.data_encoding_id.unwrap(),
            bb_first_spectrum_id: sh_record.data_encoding_id.unwrap(),
        };

        s_headers.push(sh);
    }*/

    Ok(s_headers)
}

pub fn create_entity_cache(db: &Connection) -> Result<EntityCache> {

    let param_tree = get_param_tree_mzdb(&db).location(here!())?.unwrap_or(String::new());
    let bb_sizes = BBSizes::from_xml(&param_tree)?;

    let data_encodings = list_data_encodings(&db)?;

    let mut data_encoding_by_id:  HashMap<i64, DataEncoding> = HashMap::with_capacity(data_encodings.len());
    for de in data_encodings {
        data_encoding_by_id.insert(de.id, de);
    }

    //let spectra_data_encoding_ids= list_get_spectra_data_encoding_ids(&db)?;
    let mut stmt = db.prepare("SELECT id, data_encoding_id FROM spectrum").location(here!())?;
    let mut rows = stmt.query([]).location(here!())?;

    let mut spectra_data_encoding_ids = HashMap::new();
    while let Some(row) = rows.next().location(here!())? {
        let id: i64 = row.get(0).location(here!())?;
        let data_encoding_id: i64 = row.get(1).location(here!())?;
        spectra_data_encoding_ids.insert(id, data_encoding_id);
    }

    let de_cache = DataEncodingsCache::new(
        data_encoding_by_id,
        spectra_data_encoding_ids
    );

    Ok(EntityCache {
        bb_sizes: bb_sizes,
        data_encodings_cache: de_cache,
        spectrum_headers: get_spectrum_headers(db).location(here!())?
    })
}