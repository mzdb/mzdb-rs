

use anyhow::*;
use itertools::Itertools;
use rusqlite::{Connection, Statement};
//use rusqlite::types::Type::Null;

use crate::anyhow_ext::*;
use crate::model::*;
use crate::queries::*;

const SQLQUERY_ALLMSLEVELS: &'static str = "SELECT bounding_box.* FROM bounding_box, spectrum WHERE spectrum.id = bounding_box.first_spectrum_id";
//const SQLQUERY_SINGLEMSLEVEL: &'static str = "SELECT bounding_box.* FROM bounding_box, spectrum WHERE spectrum.id = bounding_box.first_spectrum_id AND spectrum.ms_level=?";

pub fn create_bb_iter_stmt_for_all_ms_levels(db: &Connection) -> Result<Statement> {
    let stmt = db.prepare(SQLQUERY_ALLMSLEVELS).location(here!())?;
    Ok(stmt)
}

pub fn create_bb_iter_stmt_for_single_ms_level(db: &Connection, ms_level: u8) -> Result<Statement> {
    let stmt = db.prepare(
        &*format!(
            "SELECT bounding_box.* FROM bounding_box, spectrum WHERE spectrum.id = bounding_box.first_spectrum_id AND spectrum.ms_level={}",
            ms_level
        )
    ).location(here!())?;

    Ok(stmt)
}

fn iterate_bb<'stmt>(stmt: &'stmt mut Statement) -> Result<impl Iterator<Item = rusqlite::Result<BoundingBox>> + 'stmt> {

    let rows = stmt.query_map([], |row| {
        rusqlite::Result::Ok(BoundingBox {
            id: row.get(0)?,
            first_spectrum_id: row.get(3)?,
            last_spectrum_id: row.get(4)?,
            run_slice_id: row.get(2)?,
            blob_data: row.get(1)?,
        })
    }).location(here!())?;

    Ok( rows)
}


pub fn for_each_bb<F>(db: &Connection, ms_level: Option<u8>, mut on_each_bb: F) -> Result<()> where F: FnMut(BoundingBox) -> Result<()> {

    let mut bb_iter_stmt = if ms_level.is_none() {
        create_bb_iter_stmt_for_all_ms_levels(&db).location(here!())?
    } else{
        create_bb_iter_stmt_for_single_ms_level(&db,ms_level.unwrap()).location(here!())?
    };

    let bb_iter = iterate_bb(&mut bb_iter_stmt).location(here!())?;

    for bb_res in bb_iter {
        on_each_bb(bb_res?)?;
    }

    Ok(())
}

pub fn for_each_spectrum<F>(db: &Connection, entity_cache: &EntityCache, ms_level: Option<u8>, mut on_each_spectrum: F) -> Result<()>
    where F: FnMut(&Spectrum) -> Result<()> {

    let mut bb_row_buffer = Vec::with_capacity(100);
    let mut spectrum_buffer = Vec::with_capacity(100);

    let mut prev_first_spectrum_id: Option<i64> = None;

    for_each_bb(db, ms_level, |bb: BoundingBox| {
        //println!("Loaded bb {}", bb.id);

        if prev_first_spectrum_id.is_none() {
            prev_first_spectrum_id = Some(bb.first_spectrum_id);
        }

        let spec_idx = (bb.first_spectrum_id - 1) as usize;

        let bb_first_spectrum_header = entity_cache.spectrum_headers.get(spec_idx).unwrap(); // FIXME: unsafe  //test idx -1
        let spec_ms_level = bb_first_spectrum_header.ms_level;
        //println!("spec_ms_level={}",spec_ms_level);
        //println!("spectrum_buffer.is_empty={}",spectrum_buffer.is_empty() );

        // the loop will stop if the next ms level is a ms level 1 and if a ms level 1 has already been processed
        // => will collect one ms level 1 and each ms level > 1 (before or after the ms level 1)
        // note: this is required to sort MS1 and MS2 spectra and thus iterate them in the right order
        if bb.first_spectrum_id != prev_first_spectrum_id.unwrap() {
            prev_first_spectrum_id = Some(bb.first_spectrum_id);
            //prev_first_spectrum_id = None; //test

            _bb_row_buffer_to_spectrum_buffer(&bb_row_buffer, &mut spectrum_buffer, &entity_cache).location(here!())?;
            bb_row_buffer.clear();

            if spec_ms_level == 1 {
                spectrum_buffer.sort_by(|s1, s2| *&s1.header.id.partial_cmp(&s2.header.id).unwrap());

                for s in spectrum_buffer.iter() {
                    on_each_spectrum(s).location(here!())?;
                }

                spectrum_buffer.clear();
            }
        }

        bb_row_buffer.push(bb);

        Ok(())
    })?;

    _bb_row_buffer_to_spectrum_buffer(&bb_row_buffer, &mut spectrum_buffer, &entity_cache).location(here!())?;

    spectrum_buffer.sort_by(|s1, s2| *&s1.header.id.partial_cmp(&s2.header.id).unwrap());

    for s in spectrum_buffer.iter() {
        on_each_spectrum(s)?;
    }

    /*let mut bb_iter_stmt = if ms_level.is_none() {
         create_bb_iter_stmt_for_all_ms_levels(&db)?
    } else{
         create_bb_iter_stmt_for_single_ms_level(&db,ms_level.unwrap())?
    };

     let mut bb_buffer = Vec::with_capacity(100);

     //let mut bb_iter_stmt = create_bb_iter_stmt_for_all_ms_levels(&db)?;
     let mut bb_iter= iterate_bb(&mut bb_iter_stmt)?;
     let first_bb_opt = bb_iter.next();
     if first_bb_opt.is_none() {
         return Ok(());
     }

     let first_bb = first_bb_opt.unwrap()?;
     let mut cur_first_spectrum_id = first_bb.first_spectrum_id;

     bb_buffer.push(first_bb);

     let bb_index = index_bbox(&first_bb, de_cache)?;

     let new_ms1_met = false;

     //the loop will stop if the next ms level is a ms level 1 and if a ms level 1 has already been processed
     //=> will collect one ms level 1 and each ms level > 1 (before or after the ms level 1)
     // note: this loop is required to sort MS1 and MS2 spectra and thus iterate them in the right order
     let previous_first_spectrum_id = -1;
     while ms1_row_count <= 1 && previous_first_spectrum_id != cur_first_spectrum_id {

     }

     let mut is_id = false;
     while !is_id {
         let bb_opt = bb_iter.next();
         if bb_opt.is_none(){
             return Ok(());
         }

         let bb_res= bb_opt.unwrap();
         if spectrum.? ==  {
             is_id = true
         }
     }*/

    Ok(())
    /*for bb_res in bb_iter{
        on
    }

     */
}

fn _bb_row_buffer_to_spectrum_buffer(bb_row_buffer: &Vec<BoundingBox>, spectrum_buffer: &mut Vec<Spectrum>, entity_cache: &EntityCache) -> Result<()> {
    if bb_row_buffer.is_empty() {
        return Ok(())
    }

    let de_cache = &entity_cache.data_encodings_cache;
    let bb_count = bb_row_buffer.len();

    let indexed_bbs = bb_row_buffer.iter().map(|bb| {index_bbox(bb, de_cache)}).collect_vec();
    let first_bb_index = indexed_bbs[0].as_ref().unwrap();
    let n_spectra = first_bb_index.spectra_ids.len();

    for spectrum_slice_idx in 0..n_spectra {
        let mut spectrum_peak_count = 0;
        let mut spectrum_slices = Vec::with_capacity(bb_count);

        let spectrum_id = first_bb_index.spectra_ids[spectrum_slice_idx];
        let spectrum_header = entity_cache.spectrum_headers.get((spectrum_id - 1) as usize).unwrap(); // FIXME: unsafe

        let de_opt = de_cache.get_data_encoding_by_spectrum_id(&spectrum_id);
        if de_opt.is_none() {
            bail!("can't retrieve data encoding for spectrum ID={}", spectrum_id);
        }

        let data_encoding = de_opt.unwrap();

        for bb_idx in 0..bb_count {
            let bb = &bb_row_buffer[bb_idx];
            let bb_index = indexed_bbs[bb_idx].as_ref().unwrap();

            let spectrum_slice_data = read_spectrum_slice_data_at(
                bb,
                bb_index,
                data_encoding,
                spectrum_slice_idx,
                None,
                None
            ).location(here!())?;

            spectrum_peak_count += spectrum_slice_data.peaks_count;

            spectrum_slices.push(spectrum_slice_data);
        }

        let spectrum_data = merge_spectrum_slices(&mut spectrum_slices, spectrum_peak_count).location(here!())?;

        let spectrum = Spectrum {
            header: spectrum_header.clone(),
            data: spectrum_data
        };

        spectrum_buffer.push(spectrum);
    }

    Ok(())
}