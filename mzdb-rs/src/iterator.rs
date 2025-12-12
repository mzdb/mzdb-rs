//! Iterator utilities for streaming through mzDB data
//!
//! This module provides efficient iteration over bounding boxes and spectra,
//! allowing processing of large mzDB files without loading everything into memory.
//!
//! # Example
//! ```no_run
//! use mzdb::iterator::for_each_spectrum;
//! use mzdb::mzdb::create_entity_cache;
//! use rusqlite::Connection;
//!
//! let db = Connection::open("file.mzDB").unwrap();
//! let cache = create_entity_cache(&db).unwrap();
//!
//! for_each_spectrum(&db, &cache, Some(1), |spectrum| {
//!     println!("MS1 spectrum: {}", spectrum.header.id);
//!     Ok(())
//! }).unwrap();
//! ```

use anyhow::*;
use anyhow_ext::Context;
use itertools::Itertools;
use rusqlite::{Connection, Statement};

use crate::model::*;
use crate::queries::*;

const SQL_QUERY_ALL_MS_LEVELS: &str = 
    "SELECT bounding_box.* FROM bounding_box, spectrum \
     WHERE spectrum.id = bounding_box.first_spectrum_id";

pub fn create_bb_iter_stmt_for_all_ms_levels(db: &Connection) -> Result<Statement> {
    let stmt = db.prepare(SQL_QUERY_ALL_MS_LEVELS).dot()?;
    Ok(stmt)
}

pub fn create_bb_iter_stmt_for_single_ms_level(db: &Connection, ms_level: u8) -> Result<Statement> {
    let stmt = db
        .prepare(&format!(
            "SELECT bounding_box.* FROM bounding_box, spectrum \
             WHERE spectrum.id = bounding_box.first_spectrum_id AND spectrum.ms_level={}",
            ms_level
        ))
        .dot()?;

    Ok(stmt)
}

fn iterate_bb<'stmt>(
    stmt: &'stmt mut Statement,
) -> Result<impl Iterator<Item = rusqlite::Result<BoundingBox>> + 'stmt> {
    let rows = stmt
        .query_map([], |row| {
            rusqlite::Result::Ok(BoundingBox {
                id: row.get(0)?,
                first_spectrum_id: row.get(3)?,
                last_spectrum_id: row.get(4)?,
                run_slice_id: row.get(2)?,
                blob_data: row.get(1)?,
            })
        })
        .dot()?;

    Ok(rows)
}

pub fn for_each_bb<F>(db: &Connection, ms_level: Option<u8>, mut on_each_bb: F) -> Result<()>
where
    F: FnMut(BoundingBox) -> Result<()>,
{
    let mut bb_iter_stmt = match ms_level {
        None => create_bb_iter_stmt_for_all_ms_levels(db).dot()?,
        Some(level) => create_bb_iter_stmt_for_single_ms_level(db, level).dot()?,
    };

    let bb_iter = iterate_bb(&mut bb_iter_stmt).dot()?;

    for bb_res in bb_iter {
        on_each_bb(bb_res?)?;
    }

    Ok(())
}

pub fn for_each_spectrum<F>(
    db: &Connection,
    entity_cache: &EntityCache,
    ms_level: Option<u8>,
    mut on_each_spectrum: F,
) -> Result<()>
where
    F: FnMut(&Spectrum) -> Result<()>,
{
    let mut bb_row_buffer = Vec::with_capacity(100);
    let mut spectrum_buffer = Vec::with_capacity(100);

    let mut prev_first_spectrum_id: Option<i64> = None;

    for_each_bb(db, ms_level, |bb: BoundingBox| {
        let spec_idx = (bb.first_spectrum_id - 1) as usize;

        let bb_first_spectrum_header = entity_cache
            .spectrum_headers
            .get(spec_idx)
            .ok_or_else(|| anyhow!("spectrum header not found at index {}", spec_idx))?;

        let spec_ms_level = bb_first_spectrum_header.ms_level;

        // Process buffer when we encounter a new first_spectrum_id
        let is_new_spectrum = match prev_first_spectrum_id {
            None => false,
            Some(prev_id) => bb.first_spectrum_id != prev_id,
        };

        // the loop will stop if the next ms level is a ms level 1 and if a ms level 1 has already been processed
        // => will collect one ms level 1 and each ms level > 1 (before or after the ms level 1)
        // note: this is required to sort MS1 and MS2 spectra and thus iterate them in the right order
        if is_new_spectrum {
            bb_row_buffer_to_spectrum_buffer(&bb_row_buffer, &mut spectrum_buffer, entity_cache)
                .dot()?;
            bb_row_buffer.clear();

            // When encountering MS1, emit all buffered spectra in order
            if spec_ms_level == 1 {
                spectrum_buffer.sort_by(|s1, s2| s1.header.id.cmp(&s2.header.id));

                for s in spectrum_buffer.iter() {
                    on_each_spectrum(s).dot()?;
                }

                spectrum_buffer.clear();
            }
        }

        prev_first_spectrum_id = Some(bb.first_spectrum_id);
        bb_row_buffer.push(bb);

        Ok(())
    })?;

    // Process remaining bounding boxes
    bb_row_buffer_to_spectrum_buffer(&bb_row_buffer, &mut spectrum_buffer, entity_cache)
        .dot()?;

    // Emit remaining spectra
    spectrum_buffer.sort_by(|s1, s2| s1.header.id.cmp(&s2.header.id));

    for s in spectrum_buffer.iter() {
        on_each_spectrum(s)?;
    }

    Ok(())
}

fn bb_row_buffer_to_spectrum_buffer(
    bb_row_buffer: &[BoundingBox],
    spectrum_buffer: &mut Vec<Spectrum>,
    entity_cache: &EntityCache,
) -> Result<()> {
    if bb_row_buffer.is_empty() {
        return Ok(());
    }

    let de_cache = &entity_cache.data_encodings_cache;
    let bb_count = bb_row_buffer.len();

    let indexed_bbs = bb_row_buffer
        .iter()
        .map(|bb| index_bbox(bb, de_cache))
        .collect_vec();

    let first_bb_index = indexed_bbs[0].as_ref().map_err(|e| anyhow!("{}", e))?;
    let n_spectra = first_bb_index.spectra_ids.len();

    for spectrum_slice_idx in 0..n_spectra {
        let mut spectrum_peak_count = 0;
        let mut spectrum_slices = Vec::with_capacity(bb_count);

        let spectrum_id = first_bb_index.spectra_ids[spectrum_slice_idx];
        let spectrum_header = entity_cache
            .spectrum_headers
            .get((spectrum_id - 1) as usize)
            .ok_or_else(|| anyhow!("spectrum header not found for ID {}", spectrum_id))?;

        let data_encoding = de_cache
            .get_data_encoding_by_spectrum_id(&spectrum_id)
            .ok_or_else(|| anyhow!("can't retrieve data encoding for spectrum ID={}", spectrum_id))?;

        for bb_idx in 0..bb_count {
            let bb = &bb_row_buffer[bb_idx];
            let bb_index = indexed_bbs[bb_idx].as_ref().map_err(|e| anyhow!("{}", e))?;

            let spectrum_slice_data = read_spectrum_slice_data_at(
                bb,
                bb_index,
                data_encoding,
                spectrum_slice_idx,
                None,
                None,
            )
            .dot()?;

            spectrum_peak_count += spectrum_slice_data.peaks_count;
            spectrum_slices.push(spectrum_slice_data);
        }

        let spectrum_data =
            merge_spectrum_slices(&mut spectrum_slices, spectrum_peak_count).dot()?;

        let spectrum = Spectrum {
            header: spectrum_header.clone(),
            data: spectrum_data,
        };

        spectrum_buffer.push(spectrum);
    }

    Ok(())
}
