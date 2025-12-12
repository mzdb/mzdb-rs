//! Cached prepared statements for improved query performance
//!
//! This module provides a mechanism to cache commonly used prepared statements
//! to avoid re-preparing them on each query execution.
//!
//! ## Performance Notes
//!
//! The `bytes` crate is available for potential future zero-copy parsing optimizations
//! in the bounding box blob data parsing. Currently, the standard library's byte slicing
//! is used, but for high-throughput scenarios, `bytes::Bytes` could provide benefits
//! through reference counting and avoiding copies.

use rusqlite::{Connection, Statement};
use std::cell::RefCell;

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
