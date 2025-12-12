//! Core mzDB file operations
//!
//! This module provides the main entry point for working with mzDB files,
//! including creating the entity cache which holds pre-loaded metadata
//! required for efficient spectrum access.

use std::collections::HashMap;

use anyhow::*;
use anyhow_ext::Context;
use rusqlite::Connection;
use serde_rusqlite::from_rows;

use crate::model::{BBSizes, DataEncoding, DataEncodingsCache, EntityCache, SpectrumHeader};
use crate::queries::{get_param_tree_mzdb, list_data_encodings};

pub fn get_spectrum_headers(db: &Connection) -> Result<Vec<SpectrumHeader>> {
    let mut statement = db.prepare(
        "SELECT id, initial_id, title, cycle, time, ms_level, activation_type, tic, \
         base_peak_mz, base_peak_intensity, main_precursor_mz, main_precursor_charge, \
         data_points_count, precursor_list, shared_param_tree_id, instrument_configuration_id, \
         source_file_id, run_id, data_processing_id, data_encoding_id, bb_first_spectrum_id \
         FROM spectrum",
    )?;

    let s_headers = from_rows::<SpectrumHeader>(statement.query([])?)
        .collect::<rusqlite::Result<Vec<SpectrumHeader>, _>>()?;

    Ok(s_headers)
}

pub fn create_entity_cache(db: &Connection) -> Result<EntityCache> {
    let param_tree = get_param_tree_mzdb(db).dot()?.unwrap_or_default();
    let bb_sizes = BBSizes::from_xml(&param_tree)?;

    let data_encodings = list_data_encodings(db)?;

    let mut data_encoding_by_id: HashMap<i64, DataEncoding> =
        HashMap::with_capacity(data_encodings.len());
    for de in data_encodings {
        data_encoding_by_id.insert(de.id, de);
    }

    let mut stmt = db
        .prepare("SELECT id, data_encoding_id FROM spectrum")
        .dot()?;
    let mut rows = stmt.query([]).dot()?;

    let mut spectra_data_encoding_ids = HashMap::new();
    while let Some(row) = rows.next().dot()? {
        let id: i64 = row.get(0).dot()?;
        let data_encoding_id: i64 = row.get(1).dot()?;
        spectra_data_encoding_ids.insert(id, data_encoding_id);
    }

    let de_cache = DataEncodingsCache::new(data_encoding_by_id, spectra_data_encoding_ids);

    Ok(EntityCache {
        bb_sizes,
        data_encodings_cache: de_cache,
        spectrum_headers: get_spectrum_headers(db).dot()?,
    })
}
