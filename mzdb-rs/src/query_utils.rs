//! Database query utility functions
//!
//! This module provides low-level helper functions for common database query patterns.
//! These utilities reduce boilerplate when querying single values from SQLite.
//!
//! # Usage
//!
//! ```no_run
//! use mzdb::query_utils::{query_single_i64, query_single_string};
//! use rusqlite::Connection;
//!
//! let db = Connection::open("file.mzDB").unwrap();
//! let count = query_single_i64(&db, "SELECT COUNT(*) FROM spectrum").unwrap();
//! let version = query_single_string(&db, "SELECT version FROM mzdb").unwrap();
//! ```
#![allow(unused)]

use anyhow_ext::{Context, Result};
use rusqlite::{Connection, OptionalExtension, ToSql};
use rusqlite::types::ValueRef;

// ============================================================================
// Legacy encoding support
// ============================================================================

/// Convert bytes that may be Latin-1/Windows-1252 encoded to a UTF-8 String.
/// 
/// Legacy mzDB files produced by pwiz-mzDB may contain text encoded in Latin-1
/// (ISO-8859-1) or Windows-1252 instead of UTF-8. This function handles the
/// conversion by first attempting UTF-8 decoding, and falling back to Latin-1
/// decoding if that fails.
/// 
/// Latin-1 has a simple 1:1 mapping where each byte 0x00-0xFF maps directly to 
/// Unicode code points U+0000-U+00FF, making the conversion lossless.
fn bytes_to_string_with_legacy_fallback(bytes: &[u8]) -> String {
    // First try UTF-8 (fast path for modern mzDB files)
    match std::str::from_utf8(bytes) {
        Ok(s) => s.to_string(),
        Err(_) => {
            // Fall back to Latin-1 decoding (for legacy pwiz-mzDB files)
            // Latin-1 bytes 0x00-0xFF map directly to Unicode U+0000-U+00FF
            bytes.iter().map(|&b| b as char).collect()
        }
    }
}

/// Extract a string from a TEXT column ValueRef, handling legacy encoding.
/// Returns None for NULL values.
fn text_value_to_string(value: ValueRef<'_>) -> rusqlite::Result<Option<String>> {
    match value {
        ValueRef::Text(bytes) => Ok(Some(bytes_to_string_with_legacy_fallback(bytes))),
        ValueRef::Null => Ok(None),
        other => Err(rusqlite::Error::InvalidColumnType(
            0,
            "column".to_string(),
            other.data_type(),
        )),
    }
}

// ============================================================================
// Single value query helpers (no parameters)
// ============================================================================

/// Query a single optional String value.
pub fn query_single_string(db: &Connection, sql: &str) -> Result<Option<String>> {
    db.prepare(sql)
        .dot()?
        .query_row([], |row| row.get(0))
        .optional()
        .dot()
}

/// Query a single optional String value, handling legacy Latin-1 encoding.
///
/// This variant should be used for fields that may contain Latin-1 encoded text
/// in legacy mzDB files (e.g., mzdb.param_tree from older pwiz-mzDB versions).
pub fn query_single_string_latin1_safe(db: &Connection, sql: &str) -> Result<Option<String>> {
    let result = db.prepare(sql)
        .dot()?
        .query_row([], |row| text_value_to_string(row.get_ref(0)?))
        .optional()
        .dot()?;

    Ok(result.flatten())
}

/// Query all String values from a single column.
pub fn query_all_strings(db: &Connection, sql: &str) -> Result<Vec<String>> {
    let mut stmt = db.prepare(sql).dot()?;
    let rows = stmt.query_map([], |row| row.get(0)).dot()?;

    let mut result = Vec::new();
    for value in rows {
        result.push(value.dot()?);
    }
    Ok(result)
}

/// Query a single optional i64 value
pub fn query_single_i64(db: &Connection, sql: &str) -> Result<Option<i64>> {
    db.prepare(sql)
        .dot()?
        .query_row([], |row| row.get(0))
        .optional()
        .dot()
}

/// Query a single required i64 value (returns error if not found)
pub fn query_single_i64_required(db: &Connection, sql: &str) -> Result<i64> {
    db.prepare(sql)
        .dot()?
        .query_row([], |row| row.get(0))
        .dot()
}

/// Query a single optional f32 value
pub fn query_single_f32(db: &Connection, sql: &str) -> Result<Option<f32>> {
    db.prepare(sql)
        .dot()?
        .query_row([], |row| row.get(0))
        .optional()
        .dot()
}

/// Query a single optional f64 value
pub fn query_single_f64(db: &Connection, sql: &str) -> Result<Option<f64>> {
    db.prepare(sql)
        .dot()?
        .query_row([], |row| row.get(0))
        .optional()
        .dot()
}

// ============================================================================
// Parameterized single value query helpers
// ============================================================================

/// Query a single optional String value with parameters.
pub fn query_single_string_with_params<P: rusqlite::Params>(
    db: &Connection,
    sql: &str,
    params: P,
) -> Result<Option<String>> {
    db.prepare(sql)
        .dot()?
        .query_row(params, |row| row.get(0))
        .optional()
        .dot()
}

/// Query a single optional i64 value with parameters
pub fn query_single_i64_with_params<P: rusqlite::Params>(
    db: &Connection,
    sql: &str,
    params: P,
) -> Result<Option<i64>> {
    db.prepare(sql)
        .dot()?
        .query_row(params, |row| row.get(0))
        .optional()
        .dot()
}

/// Query a single required i64 value with parameters
pub fn query_single_i64_required_with_params<P: rusqlite::Params>(
    db: &Connection,
    sql: &str,
    params: P,
) -> Result<i64> {
    db.prepare(sql)
        .dot()?
        .query_row(params, |row| row.get(0))
        .dot()
}

/// Query a single optional f32 value with parameters
pub fn query_single_f32_with_params<P: rusqlite::Params>(
    db: &Connection,
    sql: &str,
    params: P,
) -> Result<Option<f32>> {
    db.prepare(sql)
        .dot()?
        .query_row(params, |row| row.get(0))
        .optional()
        .dot()
}

/// Query a single optional f64 value with parameters
pub fn query_single_f64_with_params<P: rusqlite::Params>(
    db: &Connection,
    sql: &str,
    params: P,
) -> Result<Option<f64>> {
    db.prepare(sql)
        .dot()?
        .query_row(params, |row| row.get(0))
        .optional()
        .dot()
}

// ============================================================================
// Table utilities
// ============================================================================

/// Check if a table exists in the database
pub fn table_exists(db: &Connection, table_name: &str) -> Result<bool> {
    let count: i64 = db
        .prepare("SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name = ?1")?
        .query_row([table_name], |row| row.get(0))?;
    Ok(count > 0)
}

/// Get the number of records in a table.
///
/// This function first tries to get the count from sqlite_sequence (which is faster
/// for large tables with AUTOINCREMENT primary keys), and falls back to COUNT(*)
/// if the table is not present in sqlite_sequence.
///
/// Note: sqlite_sequence stores the last used ROWID, which may be higher than the
/// actual row count if rows have been deleted. For exact counts, use `get_table_count_exact`.
pub fn get_table_records_count(db: &Connection, table_name: &str) -> Result<Option<i64>> {
    // First try sqlite_sequence (fast path for AUTOINCREMENT tables)
    let seq_count: Option<i64> = db
        .prepare("SELECT seq FROM sqlite_sequence WHERE name = ?1")?
        .query_row([table_name], |row| row.get(0))
        .optional()?;

    if seq_count.is_some() {
        return Ok(seq_count);
    }

    // Fall back to COUNT(*) if not in sqlite_sequence
    get_table_count_exact(db, table_name)
}

/// Get the exact number of records in a table using COUNT(*).
///
/// This is slower than `get_table_records_count` for large tables but always
/// returns the accurate count.
pub fn get_table_count_exact(db: &Connection, table_name: &str) -> Result<Option<i64>> {
    // Validate table name to prevent SQL injection (only alphanumeric and underscore allowed)
    if !table_name.chars().all(|c| c.is_alphanumeric() || c == '_') {
        return Ok(None);
    }

    let sql = format!("SELECT COUNT(*) FROM {}", table_name);
    db.prepare(&sql)?
        .query_row([], |row| row.get(0))
        .optional()
        .map_err(Into::into)
}

// ============================================================================
// Data encoding utilities
// ============================================================================

use crate::model::{ByteOrder, DataEncoding, DataMode, PeakEncoding};

/// Parse a DataEncoding from a database row.
///
/// Expected row format: (id, mode, compression, byte_order, mz_precision, intensity_precision)
/// This is used by both queries.rs and chromatogram.rs for consistent encoding parsing.
pub fn parse_data_encoding_from_row(row: &rusqlite::Row) -> rusqlite::Result<DataEncoding> {
    let mode_str: String = row.get(1)?;
    let byte_order_str: String = row.get(3)?;
    let mz_precision: u32 = row.get(4)?;
    let intensity_precision: u32 = row.get(5)?;

    let mode = match mode_str.as_str() {
        "fitted" => DataMode::Fitted,
        "centroid" => DataMode::Centroid,
        _ => DataMode::Profile,
    };

    let byte_order = if byte_order_str == "little_endian" {
        ByteOrder::LittleEndian
    } else {
        ByteOrder::BigEndian
    };

    let peak_encoding = if mz_precision == 32 {
        PeakEncoding::LowRes
    } else if intensity_precision == 32 {
        PeakEncoding::HighRes
    } else {
        PeakEncoding::NoLoss
    };

    Ok(DataEncoding {
        id: row.get(0)?,
        mode,
        peak_encoding,
        compression: row.get(2)?,
        byte_order,
    })
}

/// Get a data encoding by ID
pub fn get_data_encoding_by_id(db: &Connection, id: i64) -> Result<Option<DataEncoding>> {
    db.prepare(
        "SELECT id, mode, compression, byte_order, mz_precision, intensity_precision \
         FROM data_encoding WHERE id = ?1"
    )?
    .query_row([id], parse_data_encoding_from_row)
    .optional()
    .map_err(Into::into)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_table_name_validation() {
        // Valid table names
        assert!("spectrum".chars().all(|c| c.is_alphanumeric() || c == '_'));
        assert!("run_slice".chars().all(|c| c.is_alphanumeric() || c == '_'));
        assert!("bounding_box_rtree".chars().all(|c| c.is_alphanumeric() || c == '_'));

        // Invalid table names (SQL injection attempts)
        assert!(!"spectrum; DROP TABLE".chars().all(|c| c.is_alphanumeric() || c == '_'));
        assert!(!"spectrum--".chars().all(|c| c.is_alphanumeric() || c == '_'));
    }
}