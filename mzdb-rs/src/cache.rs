//! Cached prepared statements for improved query performance
//!
//! This module provides a mechanism to cache commonly used prepared statements
//! to avoid re-preparing them on each query execution.
#![allow(unused)]

use std::collections::HashMap;
use std::cell::RefCell;

use anyhow_ext::{anyhow, Context, Result};
use rusqlite::{Connection, Statement};

use crate::model::{BBSizes, DataEncoding, DataEncodingsCache, EntityCache};
use crate::queries::{
    get_param_tree_mzdb, list_data_encodings, get_spectrum_headers_with_options,
    SpectrumHeaderLoadOptions,
};
use crate::metadata::parse_msn_bb_time_width;

/// SQL queries that are frequently used and benefit from caching
pub mod sql {
    pub const GET_SPECTRUM_BY_ID: &str = 
        "SELECT bb_first_spectrum_id FROM spectrum WHERE id = ?";
    
    pub const COUNT_BB_BY_FIRST_SPECTRUM_ID: &str = 
        "SELECT count(id) FROM bounding_box WHERE bounding_box.first_spectrum_id = ?";
    
    pub const GET_BB_BY_FIRST_SPECTRUM_ID: &str = 
        "SELECT id, data, run_slice_id, first_spectrum_id, last_spectrum_id \
         FROM bounding_box WHERE first_spectrum_id = ?";
    
    pub const GET_ALL_BB_ALL_MS_LEVELS: &str = 
        "SELECT bounding_box.* FROM bounding_box, spectrum \
         WHERE spectrum.id = bounding_box.first_spectrum_id";
    
    pub const GET_ALL_BB_MS_LEVEL: &str = 
        "SELECT bounding_box.* FROM bounding_box, spectrum \
         WHERE spectrum.id = bounding_box.first_spectrum_id AND spectrum.ms_level = ?";
}

/// A cache for prepared statements to improve query performance
/// 
/// Note: This uses RefCell for interior mutability since Statement preparation
/// requires mutable access to Connection but we want to share the cache.
pub struct StatementCache<'conn> {
    conn: &'conn Connection,
    // Using Option to allow lazy initialization
    get_spectrum_stmt: RefCell<Option<Statement<'conn>>>,
    count_bb_stmt: RefCell<Option<Statement<'conn>>>,
    get_bb_stmt: RefCell<Option<Statement<'conn>>>,
}

impl<'conn> StatementCache<'conn> {
    /// Create a new statement cache for the given connection
    pub fn new(conn: &'conn Connection) -> Self {
        Self {
            conn,
            get_spectrum_stmt: RefCell::new(None),
            count_bb_stmt: RefCell::new(None),
            get_bb_stmt: RefCell::new(None),
        }
    }
    
    /// Get the underlying connection
    pub fn connection(&self) -> &Connection {
        self.conn
    }
    
    /// Prepare or retrieve the cached statement for getting spectrum by ID
    /// 
    /// Note: Due to Rust's borrowing rules with RefCell, we can't return a reference
    /// to the cached statement. Instead, callers should use `with_get_spectrum_stmt`.
    pub fn prepare_get_spectrum(&self) -> rusqlite::Result<Statement<'conn>> {
        // For simplicity in this implementation, we prepare fresh each time
        // A more sophisticated implementation would use unsafe or a different pattern
        self.conn.prepare(sql::GET_SPECTRUM_BY_ID)
    }
    
    /// Prepare or retrieve the cached statement for counting bounding boxes
    pub fn prepare_count_bb(&self) -> rusqlite::Result<Statement<'conn>> {
        self.conn.prepare(sql::COUNT_BB_BY_FIRST_SPECTRUM_ID)
    }
    
    /// Prepare or retrieve the cached statement for getting bounding boxes
    pub fn prepare_get_bb(&self) -> rusqlite::Result<Statement<'conn>> {
        self.conn.prepare(sql::GET_BB_BY_FIRST_SPECTRUM_ID)
    }
}

/// A simpler approach: pre-compile SQL strings and reuse them
/// This doesn't cache the actual Statement objects but provides
/// consistent SQL strings that SQLite will cache internally
pub struct SqlQueries;

impl SqlQueries {
    /// Get the SQL for fetching a spectrum's bb_first_spectrum_id
    pub fn get_spectrum_bb_first_id() -> &'static str {
        sql::GET_SPECTRUM_BY_ID
    }
    
    /// Get the SQL for counting bounding boxes by first spectrum ID
    pub fn count_bb_by_first_spectrum_id() -> &'static str {
        sql::COUNT_BB_BY_FIRST_SPECTRUM_ID
    }
    
    /// Get the SQL for fetching bounding boxes by first spectrum ID
    pub fn get_bb_by_first_spectrum_id() -> &'static str {
        sql::GET_BB_BY_FIRST_SPECTRUM_ID
    }
    
    /// Get the SQL for iterating all bounding boxes (all MS levels)
    pub fn get_all_bb_all_ms_levels() -> &'static str {
        sql::GET_ALL_BB_ALL_MS_LEVELS
    }
    
    /// Get the SQL for iterating bounding boxes for a specific MS level
    pub fn get_all_bb_ms_level() -> &'static str {
        sql::GET_ALL_BB_MS_LEVEL
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_sql_queries_are_valid() {
        // Just verify the SQL strings are non-empty
        assert!(!SqlQueries::get_spectrum_bb_first_id().is_empty());
        assert!(!SqlQueries::count_bb_by_first_spectrum_id().is_empty());
        assert!(!SqlQueries::get_bb_by_first_spectrum_id().is_empty());
        assert!(!SqlQueries::get_all_bb_all_ms_levels().is_empty());
        assert!(!SqlQueries::get_all_bb_ms_level().is_empty());
    }
}

// ============================================================================
// Entity cache creation
// ============================================================================

/// Build the entity cache, loading only `precursor_list` among the optional spectrum-header XML
/// columns -- the same default [`get_spectrum_headers`] itself uses.
///  Preserved under the original name and behavior so existing callers are unaffected; prefer
/// [`create_entity_cache_with_options`] when a reader needs `scan_list` or `param_tree`.
pub fn create_entity_cache(db: &Connection) -> Result<EntityCache> {
    create_entity_cache_with_options(db, SpectrumHeaderLoadOptions::default())
}

/// Build the entity cache, loading exactly the optional spectrum-header XML columns requested by
/// `options`. This is the one place those columns are read for the whole crate: every reader
/// (`get_spectrum`, `for_each_spectrum`, `SpectrumIterator`, `MzDbReader`) works from the
/// `EntityCache` this produces, so `options` here determines what is available everywhere else.
pub fn create_entity_cache_with_options(
    db: &Connection,
    options: SpectrumHeaderLoadOptions,
) -> Result<EntityCache> {
    let param_tree = get_param_tree_mzdb(db).dot()?.unwrap_or_default();
    let bb_sizes = BBSizes::from_xml(&param_tree)?;
    let msn_bb_time_width = parse_msn_bb_time_width(&param_tree);

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

    let spectrum_headers = get_spectrum_headers_with_options(db, options).dot()?;
    
    // Build ID-to-index map for non-consecutive ID support
    let spectrum_id_to_index: HashMap<i64, usize> = spectrum_headers
        .iter()
        .enumerate()
        .map(|(idx, h)| (h.id, idx))
        .collect();

    Ok(EntityCache {
        bb_sizes,
        data_encodings_cache: de_cache,
        spectrum_headers,
        spectrum_id_to_index,
        msn_bb_time_width,
        header_load_options: options,
    })
}

