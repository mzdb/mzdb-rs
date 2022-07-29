#![allow(
dead_code,
unused_imports
)]

//extern crate core;

mod anyhow_ext; // has to be first?
mod bb_iterator_v1;
mod model;
mod mzdb;
mod queries;
mod iterator;
mod test;

use crate::model::BoundingBox;

use anyhow::*;
use std::io::Write;
use std::ptr::null;
//use rusqlite::{Connection, Statement};
use rusqlite::{Connection, DatabaseName, MappedRows, OptionalExtension, Row, Statement};
use rusqlite::types::Type::Null;
use ::mzdb::queries::get_pwiz_mzdb_version;

use crate::anyhow_ext::*;
use crate::bb_iterator_v1::{BoundingBoxIterator, create_bb_iter};
use crate::iterator::*;
use crate::model::{EntityCache, PeakEncoding, Spectrum};
use crate::mzdb::create_entity_cache;
use crate::queries::{get_param_tree_mzdb, get_spectrum};


/*fn cmp_spectrum(spectrum: Spectrum) {
    print_spectrum(&spectrum);
}*/

fn main() -> anyhow::Result<()> {

    //print_file()?;

    let db = Connection::open("./data/OVEMB150205_12.mzDB")?;
    let entity_cache = create_entity_cache(&db).location(here!())?;

    /*for_each_bb(&db, None,|bb: BoundingBox| {
        println!("Loaded bb {}", bb.id);

        Ok(())
    });*/

    for_each_spectrum(&db, &entity_cache, None, |s: &Spectrum| {
        println!("Loaded spectrum {}", s.header.ms_level);

        Ok(())
    })?;
    //print_file();


    let test = get_pwiz_mzdb_version(&db);
    print!("{:?}",test);

    Ok(())
}

fn print_file() -> Result<()> {

    let db = Connection::open("./data/OVEMB150205_12.mzDB")?;

    let entity_cache = create_entity_cache(&db).location(here!())?;
    //let bb_iter = create_bb_iter(&db, &entity_cache)?;

    /*bb_iter.next();
    let bb_res_opt = bb_iter.next();
    if bb_res_opt.is_some() {
        println!("{:?}", bb_res_opt.unwrap().unwrap().id)
    }
     */

    let s = get_spectrum(&db, 1, &entity_cache)?;

    print_spectrum(&s)?;


    Ok(())
}


fn print_spectrum(spectrum: &Spectrum) -> Result<()> {
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

    std::io::stdout().flush()?;

    Ok(())
}

