//! R-tree spatial index queries for mzDB files
//!
//! This module provides support for the R-tree indices used in mzDB files
//! for efficient spatial queries on bounding boxes. The mzDB format uses
//! SQLite's R-tree extension to enable fast lookups by m/z and retention time.
//!
//! # Tables
//!
//! - **bounding_box_rtree**: R-tree index for MS1 bounding boxes
//! - **bounding_box_msn_rtree**: R-tree index for MSn bounding boxes (DIA support)
//!
//! # Usage
//!
//! R-tree queries are useful for:
//! - Extracting ion chromatograms (XICs)
//! - Region-of-interest queries
//! - DIA/SWATH data access
//!
//! # Example
//!
//! ```no_run
//! use mzdb::rtree::{query_bounding_boxes_in_mz_range, query_bounding_boxes_in_region};
//! use rusqlite::Connection;
//!
//! let db = Connection::open("file.mzDB").unwrap();
//!
//! // Find all bounding boxes containing m/z 500.0
//! let boxes = query_bounding_boxes_in_mz_range(&db, 499.5, 500.5).unwrap();
//!
//! // Find bounding boxes in a specific region
//! let region = query_bounding_boxes_in_region(&db, 400.0, 600.0, 10.0, 20.0).unwrap();
//! ```
#![allow(unused)]

use anyhow_ext::Result;
use rusqlite::{params, Connection, OptionalExtension};
use serde::{Deserialize, Serialize};

use crate::query_utils::{query_single_f64_with_params, table_exists};

// SQLite R*Tree correction factors for floating point precision
// See: https://www.sqlite.org/rtree.html#roundoff_error
pub const SQLITE_RTREE_UB_CORR: f64 = 1.0 + 0.00000012;
pub const SQLITE_RTREE_LB_CORR: f64 = 1.0 - 0.00000012;

// ============================================================================
// R-tree entry structures
// ============================================================================

/// Entry from the bounding_box_rtree table (MS1 R-tree)
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct BoundingBoxRTreeEntry {
    /// Bounding box ID (references bounding_box.id)
    pub id: i64,
    /// Minimum m/z value
    pub min_mz: f64,
    /// Maximum m/z value
    pub max_mz: f64,
    /// Minimum retention time
    pub min_time: f64,
    /// Maximum retention time
    pub max_time: f64,
}

/// Entry from the bounding_box_msn_rtree table (MSn R-tree for DIA)
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct BoundingBoxMsnRTreeEntry {
    /// Bounding box ID (references bounding_box.id)
    pub id: i64,
    /// Minimum MS level
    pub min_ms_level: i64,
    /// Maximum MS level
    pub max_ms_level: i64,
    /// Minimum parent m/z (for DIA isolation window)
    pub min_parent_mz: f64,
    /// Maximum parent m/z (for DIA isolation window)
    pub max_parent_mz: f64,
    /// Minimum m/z value
    pub min_mz: f64,
    /// Maximum m/z value
    pub max_mz: f64,
    /// Minimum retention time
    pub min_time: f64,
    /// Maximum retention time
    pub max_time: f64,
}

impl BoundingBoxRTreeEntry {
    /// Check if this entry contains a given m/z value
    pub fn contains_mz(&self, mz: f64) -> bool {
        mz >= self.min_mz && mz <= self.max_mz
    }
    
    /// Check if this entry contains a given time value
    pub fn contains_time(&self, time: f64) -> bool {
        time >= self.min_time && time <= self.max_time
    }
    
    /// Check if this entry contains a given (mz, time) point
    pub fn contains_point(&self, mz: f64, time: f64) -> bool {
        self.contains_mz(mz) && self.contains_time(time)
    }
    
    /// Get the m/z range width
    pub fn mz_width(&self) -> f64 {
        self.max_mz - self.min_mz
    }
    
    /// Get the time range width
    pub fn time_width(&self) -> f64 {
        self.max_time - self.min_time
    }
    
    /// Get the center m/z value
    pub fn center_mz(&self) -> f64 {
        (self.min_mz + self.max_mz) / 2.0
    }
    
    /// Get the center time value
    pub fn center_time(&self) -> f64 {
        (self.min_time + self.max_time) / 2.0
    }
}

impl BoundingBoxMsnRTreeEntry {
    /// Check if this entry matches a given MS level
    pub fn matches_ms_level(&self, ms_level: i64) -> bool {
        ms_level >= self.min_ms_level && ms_level <= self.max_ms_level
    }
    
    /// Check if this entry contains a given parent m/z (for DIA)
    pub fn contains_parent_mz(&self, parent_mz: f64) -> bool {
        parent_mz >= self.min_parent_mz && parent_mz <= self.max_parent_mz
    }
    
    /// Check if this entry contains a given m/z value
    pub fn contains_mz(&self, mz: f64) -> bool {
        mz >= self.min_mz && mz <= self.max_mz
    }
    
    /// Check if this entry contains a given time value
    pub fn contains_time(&self, time: f64) -> bool {
        time >= self.min_time && time <= self.max_time
    }
}

// ============================================================================
// R-tree availability checks
// ============================================================================

/// Check if the bounding_box_rtree table exists and has data
pub fn has_rtree(db: &Connection) -> Result<bool> {
    if !table_exists(db, "bounding_box_rtree")? {
        return Ok(false);
    }
    
    let entry_count: i64 = db
        .prepare("SELECT COUNT(*) FROM bounding_box_rtree")?
        .query_row([], |row| row.get(0))?;
    
    Ok(entry_count > 0)
}

/// Check if the bounding_box_msn_rtree table exists and has data
pub fn has_msn_rtree(db: &Connection) -> Result<bool> {
    if !table_exists(db, "bounding_box_msn_rtree")? {
        return Ok(false);
    }
    
    let entry_count: i64 = db
        .prepare("SELECT COUNT(*) FROM bounding_box_msn_rtree")?
        .query_row([], |row| row.get(0))?;
    
    Ok(entry_count > 0)
}

/// Get statistics about the R-tree index
#[derive(Clone, Debug, PartialEq)]
pub struct RTreeStats {
    /// Total number of entries
    pub entry_count: i64,
    /// Minimum m/z across all entries
    pub global_min_mz: f64,
    /// Maximum m/z across all entries
    pub global_max_mz: f64,
    /// Minimum time across all entries
    pub global_min_time: f64,
    /// Maximum time across all entries
    pub global_max_time: f64,
}

/// Get statistics for the MS1 R-tree
pub fn get_rtree_stats(db: &Connection) -> Result<Option<RTreeStats>> {
    if !has_rtree(db)? {
        return Ok(None);
    }
    
    let stats = db
        .prepare(
            "SELECT COUNT(*), MIN(min_mz), MAX(max_mz), MIN(min_time), MAX(max_time) \
             FROM bounding_box_rtree"
        )?
        .query_row([], |row| {
            Ok(RTreeStats {
                entry_count: row.get(0)?,
                global_min_mz: row.get(1)?,
                global_max_mz: row.get(2)?,
                global_min_time: row.get(3)?,
                global_max_time: row.get(4)?,
            })
        })?;
    
    Ok(Some(stats))
}

// ============================================================================
// MS1 R-tree queries (bounding_box_rtree)
// ============================================================================

/// Query all bounding boxes from the R-tree
pub fn list_rtree_entries(db: &Connection) -> Result<Vec<BoundingBoxRTreeEntry>> {
    let mut stmt = db.prepare(
        "SELECT id, min_mz, max_mz, min_time, max_time FROM bounding_box_rtree"
    )?;
    
    let entries = stmt.query_map([], |row| {
        Ok(BoundingBoxRTreeEntry {
            id: row.get(0)?,
            min_mz: row.get(1)?,
            max_mz: row.get(2)?,
            min_time: row.get(3)?,
            max_time: row.get(4)?,
        })
    })?;
    
    entries.collect::<rusqlite::Result<Vec<_>>>().map_err(Into::into)
}

/// Query bounding boxes that overlap with an m/z range
pub fn query_bounding_boxes_in_mz_range(
    db: &Connection,
    min_mz: f64,
    max_mz: f64,
) -> Result<Vec<BoundingBoxRTreeEntry>> {
    // Apply SQLite R-tree correction factors
    let min_mz_corr = min_mz * SQLITE_RTREE_LB_CORR;
    let max_mz_corr = max_mz * SQLITE_RTREE_UB_CORR;
    
    let mut stmt = db.prepare(
        "SELECT id, min_mz, max_mz, min_time, max_time \
         FROM bounding_box_rtree \
         WHERE max_mz >= ?1 AND min_mz <= ?2"
    )?;
    
    let entries = stmt.query_map([min_mz_corr, max_mz_corr], |row| {
        Ok(BoundingBoxRTreeEntry {
            id: row.get(0)?,
            min_mz: row.get(1)?,
            max_mz: row.get(2)?,
            min_time: row.get(3)?,
            max_time: row.get(4)?,
        })
    })?;
    
    entries.collect::<rusqlite::Result<Vec<_>>>().map_err(Into::into)
}

/// Query bounding boxes that overlap with a time range
pub fn query_bounding_boxes_in_time_range(
    db: &Connection,
    min_time: f64,
    max_time: f64,
) -> Result<Vec<BoundingBoxRTreeEntry>> {
    // Apply SQLite R-tree correction factors
    let min_time_corr = min_time * SQLITE_RTREE_LB_CORR;
    let max_time_corr = max_time * SQLITE_RTREE_UB_CORR;
    
    let mut stmt = db.prepare(
        "SELECT id, min_mz, max_mz, min_time, max_time \
         FROM bounding_box_rtree \
         WHERE max_time >= ?1 AND min_time <= ?2"
    )?;
    
    let entries = stmt.query_map([min_time_corr, max_time_corr], |row| {
        Ok(BoundingBoxRTreeEntry {
            id: row.get(0)?,
            min_mz: row.get(1)?,
            max_mz: row.get(2)?,
            min_time: row.get(3)?,
            max_time: row.get(4)?,
        })
    })?;
    
    entries.collect::<rusqlite::Result<Vec<_>>>().map_err(Into::into)
}

/// Query bounding boxes that overlap with a 2D region (m/z x time)
pub fn query_bounding_boxes_in_region(
    db: &Connection,
    min_mz: f64,
    max_mz: f64,
    min_time: f64,
    max_time: f64,
) -> Result<Vec<BoundingBoxRTreeEntry>> {
    // Apply SQLite R-tree correction factors
    let min_mz_corr = min_mz * SQLITE_RTREE_LB_CORR;
    let max_mz_corr = max_mz * SQLITE_RTREE_UB_CORR;
    let min_time_corr = min_time * SQLITE_RTREE_LB_CORR;
    let max_time_corr = max_time * SQLITE_RTREE_UB_CORR;
    
    let mut stmt = db.prepare(
        "SELECT id, min_mz, max_mz, min_time, max_time \
         FROM bounding_box_rtree \
         WHERE max_mz >= ?1 AND min_mz <= ?2 \
         AND max_time >= ?3 AND min_time <= ?4"
    )?;
    
    let entries = stmt.query_map(
        [min_mz_corr, max_mz_corr, min_time_corr, max_time_corr],
        |row| {
            Ok(BoundingBoxRTreeEntry {
                id: row.get(0)?,
                min_mz: row.get(1)?,
                max_mz: row.get(2)?,
                min_time: row.get(3)?,
                max_time: row.get(4)?,
            })
        }
    )?;
    
    entries.collect::<rusqlite::Result<Vec<_>>>().map_err(Into::into)
}

/// Query bounding boxes that contain a specific point
pub fn query_bounding_boxes_containing_point(
    db: &Connection,
    mz: f64,
    time: f64,
) -> Result<Vec<BoundingBoxRTreeEntry>> {
    query_bounding_boxes_in_region(db, mz, mz, time, time)
}

/// Get bounding box IDs in an m/z range (optimized for XIC generation)
pub fn get_bounding_box_ids_in_mz_range(
    db: &Connection,
    min_mz: f64,
    max_mz: f64,
) -> Result<Vec<i64>> {
    let min_mz_corr = min_mz * SQLITE_RTREE_LB_CORR;
    let max_mz_corr = max_mz * SQLITE_RTREE_UB_CORR;
    
    let mut stmt = db.prepare(
        "SELECT id FROM bounding_box_rtree \
         WHERE max_mz >= ?1 AND min_mz <= ?2"
    )?;
    
    let ids = stmt.query_map([min_mz_corr, max_mz_corr], |row| row.get(0))?;
    
    ids.collect::<rusqlite::Result<Vec<i64>>>().map_err(Into::into)
}

// ============================================================================
// MSn R-tree queries (bounding_box_msn_rtree) for DIA support
// ============================================================================

/// Query all MSn R-tree entries
pub fn list_msn_rtree_entries(db: &Connection) -> Result<Vec<BoundingBoxMsnRTreeEntry>> {
    let mut stmt = db.prepare(
        "SELECT id, min_ms_level, max_ms_level, min_parent_mz, max_parent_mz, \
         min_mz, max_mz, min_time, max_time FROM bounding_box_msn_rtree"
    )?;
    
    let entries = stmt.query_map([], |row| {
        Ok(BoundingBoxMsnRTreeEntry {
            id: row.get(0)?,
            min_ms_level: row.get(1)?,
            max_ms_level: row.get(2)?,
            min_parent_mz: row.get(3)?,
            max_parent_mz: row.get(4)?,
            min_mz: row.get(5)?,
            max_mz: row.get(6)?,
            min_time: row.get(7)?,
            max_time: row.get(8)?,
        })
    })?;
    
    entries.collect::<rusqlite::Result<Vec<_>>>().map_err(Into::into)
}

/// Query MSn bounding boxes by MS level and parent m/z (for DIA/SWATH)
pub fn query_msn_bounding_boxes_for_dia(
    db: &Connection,
    ms_level: i64,
    parent_mz: f64,
    parent_mz_tolerance: f64,
) -> Result<Vec<BoundingBoxMsnRTreeEntry>> {
    let min_parent_mz = (parent_mz - parent_mz_tolerance) * SQLITE_RTREE_LB_CORR;
    let max_parent_mz = (parent_mz + parent_mz_tolerance) * SQLITE_RTREE_UB_CORR;
    
    let mut stmt = db.prepare(
        "SELECT id, min_ms_level, max_ms_level, min_parent_mz, max_parent_mz, \
         min_mz, max_mz, min_time, max_time FROM bounding_box_msn_rtree \
         WHERE min_ms_level <= ?1 AND max_ms_level >= ?1 \
         AND max_parent_mz >= ?2 AND min_parent_mz <= ?3"
    )?;
    
    let entries = stmt.query_map([ms_level, min_parent_mz as i64, max_parent_mz as i64], |row| {
        Ok(BoundingBoxMsnRTreeEntry {
            id: row.get(0)?,
            min_ms_level: row.get(1)?,
            max_ms_level: row.get(2)?,
            min_parent_mz: row.get(3)?,
            max_parent_mz: row.get(4)?,
            min_mz: row.get(5)?,
            max_mz: row.get(6)?,
            min_time: row.get(7)?,
            max_time: row.get(8)?,
        })
    })?;
    
    entries.collect::<rusqlite::Result<Vec<_>>>().map_err(Into::into)
}

/// Query MSn bounding boxes in a region (m/z x time) for a specific MS level
pub fn query_msn_bounding_boxes_in_region(
    db: &Connection,
    ms_level: i64,
    min_mz: f64,
    max_mz: f64,
    min_time: f64,
    max_time: f64,
) -> Result<Vec<BoundingBoxMsnRTreeEntry>> {
    let min_mz_corr = min_mz * SQLITE_RTREE_LB_CORR;
    let max_mz_corr = max_mz * SQLITE_RTREE_UB_CORR;
    let min_time_corr = min_time * SQLITE_RTREE_LB_CORR;
    let max_time_corr = max_time * SQLITE_RTREE_UB_CORR;
    
    let mut stmt = db.prepare(
        "SELECT id, min_ms_level, max_ms_level, min_parent_mz, max_parent_mz, \
         min_mz, max_mz, min_time, max_time FROM bounding_box_msn_rtree \
         WHERE min_ms_level <= ?1 AND max_ms_level >= ?1 \
         AND max_mz >= ?2 AND min_mz <= ?3 \
         AND max_time >= ?4 AND min_time <= ?5"
    )?;
    
    let entries = stmt.query_map(
        params![ms_level, min_mz_corr, max_mz_corr, min_time_corr, max_time_corr],
        |row| {
            Ok(BoundingBoxMsnRTreeEntry {
                id: row.get(0)?,
                min_ms_level: row.get(1)?,
                max_ms_level: row.get(2)?,
                min_parent_mz: row.get(3)?,
                max_parent_mz: row.get(4)?,
                min_mz: row.get(5)?,
                max_mz: row.get(6)?,
                min_time: row.get(7)?,
                max_time: row.get(8)?,
            })
        }
    )?;
    
    entries.collect::<rusqlite::Result<Vec<_>>>().map_err(Into::into)
}

/// Get unique parent m/z windows from the MSn R-tree (for DIA/SWATH data)
pub fn get_parent_mz_windows(db: &Connection) -> Result<Vec<(f64, f64)>> {
    let mut stmt = db.prepare(
        "SELECT DISTINCT min_parent_mz, max_parent_mz FROM bounding_box_msn_rtree \
         WHERE min_ms_level >= 2 \
         ORDER BY min_parent_mz"
    )?;
    
    let windows = stmt.query_map([], |row| {
        Ok((row.get(0)?, row.get(1)?))
    })?;
    
    windows.collect::<rusqlite::Result<Vec<_>>>().map_err(Into::into)
}

// ============================================================================
// Single bounding box R-tree queries (moved from queries.rs)
// ============================================================================

/// Get the R-tree entry for a specific bounding box by ID
pub fn get_bounding_box_rtree_entry(db: &Connection, bb_id: i64) -> Result<Option<BoundingBoxRTreeEntry>> {
    db.prepare(
        "SELECT id, min_mz, max_mz, min_time, max_time FROM bounding_box_rtree WHERE id = ?1"
    )?
    .query_row([bb_id], |row| {
        Ok(BoundingBoxRTreeEntry {
            id: row.get(0)?,
            min_mz: row.get(1)?,
            max_mz: row.get(2)?,
            min_time: row.get(3)?,
            max_time: row.get(4)?,
        })
    })
    .optional()
    .map_err(Into::into)
}

/// Get the minimum m/z of a bounding box from R-tree
pub fn get_bounding_box_min_mz(db: &Connection, bb_rtree_id: i64) -> Result<Option<f64>> {
    query_single_f64_with_params(
        db,
        "SELECT min_mz FROM bounding_box_rtree WHERE id = ?1",
        [bb_rtree_id],
    )
}

/// Get the maximum m/z of a bounding box from R-tree
pub fn get_bounding_box_max_mz(db: &Connection, bb_rtree_id: i64) -> Result<Option<f64>> {
    query_single_f64_with_params(
        db,
        "SELECT max_mz FROM bounding_box_rtree WHERE id = ?1",
        [bb_rtree_id],
    )
}

/// Get the minimum time of a bounding box from R-tree
pub fn get_bounding_box_min_time(db: &Connection, bb_rtree_id: i64) -> Result<Option<f64>> {
    query_single_f64_with_params(
        db,
        "SELECT min_time FROM bounding_box_rtree WHERE id = ?1",
        [bb_rtree_id],
    )
}

/// Get the maximum time of a bounding box from R-tree
pub fn get_bounding_box_max_time(db: &Connection, bb_rtree_id: i64) -> Result<Option<f64>> {
    query_single_f64_with_params(
        db,
        "SELECT max_time FROM bounding_box_rtree WHERE id = ?1",
        [bb_rtree_id],
    )
}

/// Get the MSn R-tree entry for a specific bounding box by ID
pub fn get_bounding_box_msn_rtree_entry(db: &Connection, bb_id: i64) -> Result<Option<BoundingBoxMsnRTreeEntry>> {
    db.prepare(
        "SELECT id, min_ms_level, max_ms_level, min_parent_mz, max_parent_mz, \
         min_mz, max_mz, min_time, max_time FROM bounding_box_msn_rtree WHERE id = ?1"
    )?
    .query_row([bb_id], |row| {
        Ok(BoundingBoxMsnRTreeEntry {
            id: row.get(0)?,
            min_ms_level: row.get(1)?,
            max_ms_level: row.get(2)?,
            min_parent_mz: row.get(3)?,
            max_parent_mz: row.get(4)?,
            min_mz: row.get(5)?,
            max_mz: row.get(6)?,
            min_time: row.get(7)?,
            max_time: row.get(8)?,
        })
    })
    .optional()
    .map_err(Into::into)
}

// ============================================================================
// Helper functions for ppm-based queries
// ============================================================================

/// Convert ppm tolerance to absolute m/z tolerance
pub fn ppm_to_mz_tolerance(mz: f64, ppm: f64) -> f64 {
    mz * ppm / 1_000_000.0
}

/// Query bounding boxes containing an m/z with ppm tolerance
pub fn query_bounding_boxes_at_mz_ppm(
    db: &Connection,
    mz: f64,
    ppm_tolerance: f64,
) -> Result<Vec<BoundingBoxRTreeEntry>> {
    let mz_tol = ppm_to_mz_tolerance(mz, ppm_tolerance);
    query_bounding_boxes_in_mz_range(db, mz - mz_tol, mz + mz_tol)
}

/// Query bounding boxes in a region with ppm tolerance for m/z
pub fn query_bounding_boxes_in_region_ppm(
    db: &Connection,
    mz: f64,
    ppm_tolerance: f64,
    min_time: f64,
    max_time: f64,
) -> Result<Vec<BoundingBoxRTreeEntry>> {
    let mz_tol = ppm_to_mz_tolerance(mz, ppm_tolerance);
    query_bounding_boxes_in_region(db, mz - mz_tol, mz + mz_tol, min_time, max_time)
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_rtree_entry_methods() {
        let entry = BoundingBoxRTreeEntry {
            id: 1,
            min_mz: 400.0,
            max_mz: 600.0,
            min_time: 10.0,
            max_time: 20.0,
        };
        
        assert!(entry.contains_mz(500.0));
        assert!(!entry.contains_mz(300.0));
        assert!(entry.contains_time(15.0));
        assert!(!entry.contains_time(25.0));
        assert!(entry.contains_point(500.0, 15.0));
        assert!(!entry.contains_point(300.0, 15.0));
        
        assert_eq!(entry.mz_width(), 200.0);
        assert_eq!(entry.time_width(), 10.0);
        assert_eq!(entry.center_mz(), 500.0);
        assert_eq!(entry.center_time(), 15.0);
    }
    
    #[test]
    fn test_msn_rtree_entry_methods() {
        let entry = BoundingBoxMsnRTreeEntry {
            id: 1,
            min_ms_level: 2,
            max_ms_level: 2,
            min_parent_mz: 490.0,
            max_parent_mz: 510.0,
            min_mz: 100.0,
            max_mz: 1000.0,
            min_time: 10.0,
            max_time: 20.0,
        };
        
        assert!(entry.matches_ms_level(2));
        assert!(!entry.matches_ms_level(1));
        assert!(entry.contains_parent_mz(500.0));
        assert!(!entry.contains_parent_mz(600.0));
    }
    
    #[test]
    fn test_ppm_conversion() {
        let mz = 500.0;
        let ppm = 10.0;
        let tolerance = ppm_to_mz_tolerance(mz, ppm);
        assert!((tolerance - 0.005).abs() < 1e-9);
    }
}
