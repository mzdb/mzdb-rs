mod cache;
mod iterator;
mod model;
mod mzdb;
mod queries;

use anyhow::*;
use anyhow_ext::Context;
use rusqlite::Connection;
use std::path::PathBuf;

use crate::iterator::for_each_spectrum;
use crate::model::Spectrum;
use crate::mzdb::create_entity_cache;
use crate::queries::get_pwiz_mzdb_version;

fn test_db_path() -> PathBuf {
    let mut path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    path.push("data");
    path.push("small.mzDB");
    path
}

fn main() -> Result<()> {
        
    // Using the low-level API (for demonstration)
    let db = Connection::open(test_db_path())?;
    let entity_cache = create_entity_cache(&db).dot()?;

    println!("=== Low-level API ===");
    let mut count = 0;
    for_each_spectrum(&db, &entity_cache, None, |s: &Spectrum| {
        count += 1;
        if count <= 5 {
            println!(
                "Spectrum {} (MS{}): {} peaks",
                s.header.id, s.header.ms_level, s.data.peaks_count
            );
        }
        Ok(())
    })?;
    println!("Total spectra: {}", count);

    let version = get_pwiz_mzdb_version(&db)?;
    println!("mzDB writer version: {:?}", version);

    Ok(())
}
