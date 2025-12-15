//! Additional query functions for comprehensive mzDB support
//!
//! This module extends the base queries with additional functionality
//! for run slices, region queries, and DIA/SWATH support.
#![allow(unused)]

use anyhow_ext::Result;
use rusqlite::{Connection, OptionalExtension};

use crate::model::*;
use crate::query_utils::{get_table_records_count, table_exists};

// ============================================================================
// Run slice queries
// ============================================================================

/// Get all run slice headers
pub fn list_run_slices(db: &Connection) -> Result<Vec<RunSliceHeader>> {
    let mut stmt = db.prepare(
        "SELECT id, ms_level, number, begin_mz, end_mz, run_id FROM run_slice ORDER BY number"
    )?;
    
    let slices = stmt.query_map([], |row| {
        Ok(RunSliceHeader {
            id: row.get(0)?,
            ms_level: row.get(1)?,
            number: row.get(2)?,
            begin_mz: row.get(3)?,
            end_mz: row.get(4)?,
            run_id: row.get(5)?,
        })
    })?;
    
    slices.collect::<rusqlite::Result<Vec<_>>>().map_err(Into::into)
}

/// Get run slices for a specific MS level
pub fn list_run_slices_by_ms_level(db: &Connection, ms_level: i64) -> Result<Vec<RunSliceHeader>> {
    let mut stmt = db.prepare(
        "SELECT id, ms_level, number, begin_mz, end_mz, run_id FROM run_slice \
         WHERE ms_level = ?1 ORDER BY number"
    )?;
    
    let slices = stmt.query_map([ms_level], |row| {
        Ok(RunSliceHeader {
            id: row.get(0)?,
            ms_level: row.get(1)?,
            number: row.get(2)?,
            begin_mz: row.get(3)?,
            end_mz: row.get(4)?,
            run_id: row.get(5)?,
        })
    })?;
    
    slices.collect::<rusqlite::Result<Vec<_>>>().map_err(Into::into)
}

/// Get a specific run slice by ID
pub fn get_run_slice(db: &Connection, id: i64) -> Result<Option<RunSliceHeader>> {
    let result = db
        .prepare("SELECT id, ms_level, number, begin_mz, end_mz, run_id FROM run_slice WHERE id = ?1")?
        .query_row([id], |row| {
            Ok(RunSliceHeader {
                id: row.get(0)?,
                ms_level: row.get(1)?,
                number: row.get(2)?,
                begin_mz: row.get(3)?,
                end_mz: row.get(4)?,
                run_id: row.get(5)?,
            })
        })
        .optional()?;
    Ok(result)
}

/// Get run slice containing a specific m/z at a given MS level
pub fn get_run_slice_containing_mz(
    db: &Connection, 
    mz: f64, 
    ms_level: i64
) -> Result<Option<RunSliceHeader>> {
    let result = db
        .prepare(
            "SELECT id, ms_level, number, begin_mz, end_mz, run_id FROM run_slice \
             WHERE ms_level = ?1 AND begin_mz <= ?2 AND end_mz >= ?2 LIMIT 1"
        )?
        .query_row([ms_level as f64, mz], |row| {
            Ok(RunSliceHeader {
                id: row.get(0)?,
                ms_level: row.get(1)?,
                number: row.get(2)?,
                begin_mz: row.get(3)?,
                end_mz: row.get(4)?,
                run_id: row.get(5)?,
            })
        })
        .optional()?;
    Ok(result)
}

/// Get the number of run slices
pub fn get_run_slice_count(db: &Connection) -> Result<i64> {
    Ok(get_table_records_count(db, "run_slice")?.unwrap_or(0))
}

/// Get the number of run slices for a specific MS level
pub fn get_run_slice_count_by_ms_level(db: &Connection, ms_level: i64) -> Result<i64> {
    db.prepare("SELECT COUNT(*) FROM run_slice WHERE ms_level = ?1")?
        .query_row([ms_level], |row| row.get(0))
        .map_err(Into::into)
}

// ============================================================================
// Extended spectrum queries
// ============================================================================

/// Get spectrum IDs in a retention time range
pub fn get_spectrum_ids_in_rt_range(
    db: &Connection,
    min_rt: f32,
    max_rt: f32,
    ms_level: Option<i64>,
) -> Result<Vec<i64>> {
    let query = match ms_level {
        Some(level) => format!(
            "SELECT id FROM spectrum WHERE time >= ?1 AND time <= ?2 AND ms_level = {} ORDER BY id",
            level
        ),
        None => "SELECT id FROM spectrum WHERE time >= ?1 AND time <= ?2 ORDER BY id".to_string(),
    };
    
    let mut stmt = db.prepare(&query)?;
    let ids = stmt.query_map([min_rt, max_rt], |row| row.get(0))?;
    ids.collect::<rusqlite::Result<Vec<i64>>>().map_err(Into::into)
}

/// Get spectrum IDs in a cycle range
pub fn get_spectrum_ids_in_cycle_range(
    db: &Connection,
    min_cycle: i64,
    max_cycle: i64,
    ms_level: Option<i64>,
) -> Result<Vec<i64>> {
    let query = match ms_level {
        Some(level) => format!(
            "SELECT id FROM spectrum WHERE cycle >= ?1 AND cycle <= ?2 AND ms_level = {} ORDER BY id",
            level
        ),
        None => "SELECT id FROM spectrum WHERE cycle >= ?1 AND cycle <= ?2 ORDER BY id".to_string(),
    };
    
    let mut stmt = db.prepare(&query)?;
    let ids = stmt.query_map([min_cycle, max_cycle], |row| row.get(0))?;
    ids.collect::<rusqlite::Result<Vec<i64>>>().map_err(Into::into)
}

/// Get MS2 spectrum IDs for a given precursor m/z range
pub fn get_ms2_spectrum_ids_for_precursor_mz(
    db: &Connection,
    min_precursor_mz: f64,
    max_precursor_mz: f64,
) -> Result<Vec<i64>> {
    let mut stmt = db.prepare(
        "SELECT id FROM spectrum WHERE ms_level = 2 \
         AND main_precursor_mz >= ?1 AND main_precursor_mz <= ?2 ORDER BY id"
    )?;
    
    let ids = stmt.query_map([min_precursor_mz, max_precursor_mz], |row| row.get(0))?;
    ids.collect::<rusqlite::Result<Vec<i64>>>().map_err(Into::into)
}

// ============================================================================
// Bounding box queries
// ============================================================================

/// Get bounding box IDs for a run slice
pub fn get_bounding_box_ids_for_run_slice(db: &Connection, run_slice_id: i64) -> Result<Vec<i64>> {
    let mut stmt = db.prepare(
        "SELECT id FROM bounding_box WHERE run_slice_id = ?1 ORDER BY first_spectrum_id"
    )?;
    
    let ids = stmt.query_map([run_slice_id], |row| row.get(0))?;
    ids.collect::<rusqlite::Result<Vec<i64>>>().map_err(Into::into)
}

/// Get the total number of bounding boxes
pub fn get_bounding_box_count(db: &Connection) -> Result<i64> {
    Ok(get_table_records_count(db, "bounding_box")?.unwrap_or(0))
}

/// Get bounding boxes overlapping with a spectrum ID range
pub fn get_bounding_boxes_for_spectrum_range(
    db: &Connection,
    first_spectrum_id: i64,
    last_spectrum_id: i64,
) -> Result<Vec<BoundingBox>> {
    let mut stmt = db.prepare(
        "SELECT id, data, run_slice_id, first_spectrum_id, last_spectrum_id FROM bounding_box \
         WHERE first_spectrum_id <= ?2 AND last_spectrum_id >= ?1 ORDER BY first_spectrum_id"
    )?;
    
    let boxes = stmt.query_map([first_spectrum_id, last_spectrum_id], |row| {
        Ok(BoundingBox {
            id: row.get(0)?,
            blob_data: row.get(1)?,
            run_slice_id: row.get(2)?,
            first_spectrum_id: row.get(3)?,
            last_spectrum_id: row.get(4)?,
        })
    })?;
    
    boxes.collect::<rusqlite::Result<Vec<_>>>().map_err(Into::into)
}

// ============================================================================
// Statistics queries
// ============================================================================

/// Statistics about the mzDB file
#[derive(Clone, Debug, PartialEq)]
pub struct MzDbStats {
    pub spectrum_count: i64,
    pub ms1_count: i64,
    pub ms2_count: i64,
    pub chromatogram_count: i64,
    pub bounding_box_count: i64,
    pub run_slice_count: i64,
    pub min_mz: Option<f64>,
    pub max_mz: Option<f64>,
    pub min_rt: Option<f32>,
    pub max_rt: Option<f32>,
}

/// Get comprehensive statistics about the mzDB file
pub fn get_mzdb_stats(db: &Connection) -> Result<MzDbStats> {
    // Use sqlite_sequence where possible for better performance
    let spectrum_count = get_table_records_count(db, "spectrum")?.unwrap_or(0);
    
    // These need WHERE clauses so we use COUNT(*) directly
    let ms1_count: i64 = db
        .prepare("SELECT COUNT(*) FROM spectrum WHERE ms_level = 1")?
        .query_row([], |row| row.get(0))?;
    
    let ms2_count: i64 = db
        .prepare("SELECT COUNT(*) FROM spectrum WHERE ms_level = 2")?
        .query_row([], |row| row.get(0))?;
    
    let chromatogram_count = get_table_records_count(db, "chromatogram")?.unwrap_or(0);
    let bounding_box_count = get_table_records_count(db, "bounding_box")?.unwrap_or(0);
    let run_slice_count = get_table_records_count(db, "run_slice")?.unwrap_or(0);
    
    let min_mz: Option<f64> = db
        .prepare("SELECT MIN(begin_mz) FROM run_slice")?
        .query_row([], |row| row.get(0))
        .ok();
    
    let max_mz: Option<f64> = db
        .prepare("SELECT MAX(end_mz) FROM run_slice")?
        .query_row([], |row| row.get(0))
        .ok();
    
    let min_rt: Option<f32> = db
        .prepare("SELECT MIN(time) FROM spectrum")?
        .query_row([], |row| row.get(0))
        .ok();
    
    let max_rt: Option<f32> = db
        .prepare("SELECT MAX(time) FROM spectrum")?
        .query_row([], |row| row.get(0))
        .ok();
    
    Ok(MzDbStats {
        spectrum_count,
        ms1_count,
        ms2_count,
        chromatogram_count,
        bounding_box_count,
        run_slice_count,
        min_mz,
        max_mz,
        min_rt,
        max_rt,
    })
}

// ============================================================================
// XML parsing helpers
// ============================================================================

/// Parse a CV param float value from XML descendants
fn parse_cv_param_f32_value(children: &mut roxmltree::Descendants, cv_param_ac: &str) -> Option<f32> {
    children.find(|n| n.attribute("accession") == Some(cv_param_ac)).and_then(|n| {
        n.attributes()
            .find(|a| a.name().starts_with("value"))
            .and_then(|attr| attr.value().parse::<f32>().ok())
    })
}

/// Parse an isolation window from precursor_list XML
fn parse_isolation_window_from_xml(prec_list_xml: &str) -> Option<IsolationWindow> {
    let xml_doc = roxmltree::Document::parse(prec_list_xml).ok()?;
    let mut children = xml_doc.descendants();
    
    // MS:1000827 = isolation window target m/z
    let target_mz = parse_cv_param_f32_value(&mut children, "MS:1000827")?;
    
    // Reset iterator for next search
    let mut children = xml_doc.descendants();
    // MS:1000828 = isolation window lower offset
    let lower_offset = parse_cv_param_f32_value(&mut children, "MS:1000828").unwrap_or(0.0);
    
    let mut children = xml_doc.descendants();
    // MS:1000829 = isolation window upper offset
    let upper_offset = parse_cv_param_f32_value(&mut children, "MS:1000829").unwrap_or(0.0);
    
    Some(IsolationWindow {
        min_mz: (target_mz - lower_offset) as f64,
        max_mz: (target_mz + upper_offset) as f64,
    })
}

// ============================================================================
// DIA/SWATH specific queries
// ============================================================================

/// Get unique MS2 isolation windows (for DIA data) by parsing precursor_list XML
pub fn get_isolation_windows(db: &Connection) -> Result<Vec<IsolationWindow>> {
    use std::collections::HashSet;
    
    // Query all MS2 spectra with their precursor_list XML
    let mut stmt = db.prepare(
        "SELECT DISTINCT precursor_list FROM spectrum WHERE ms_level = 2 AND precursor_list IS NOT NULL"
    )?;
    
    let precursor_lists: Vec<String> = stmt.query_map([], |row| row.get(0))?
        .filter_map(|r| r.ok())
        .collect();
    
    // Parse isolation windows from XML and deduplicate
    // Use a HashSet with rounded values to handle floating point comparison
    let mut seen: HashSet<(i64, i64)> = HashSet::new();
    let mut windows = Vec::new();
    
    for prec_list in &precursor_lists {
        if let Some(window) = parse_isolation_window_from_xml(prec_list) {
            // Round to 0.01 m/z precision for deduplication
            let key = (
                (window.min_mz * 100.0).round() as i64,
                (window.max_mz * 100.0).round() as i64,
            );
            
            if seen.insert(key) {
                windows.push(window);
            }
        }
    }
    
    // Sort by min_mz
    windows.sort_by(|a, b| a.min_mz.partial_cmp(&b.min_mz).unwrap_or(std::cmp::Ordering::Equal));
    
    Ok(windows)
}

/// Check if the file appears to be DIA data
pub fn is_dia_data(db: &Connection) -> Result<bool> {
    // DIA data typically has many MS2 spectra with similar precursor m/z patterns
    let ms2_count: i64 = db
        .prepare("SELECT COUNT(*) FROM spectrum WHERE ms_level = 2")?
        .query_row([], |row| row.get(0))?;
    
    if ms2_count < 100 {
        return Ok(false);
    }
    
    // Check if MSn R-tree exists (typically present for DIA)
    table_exists(db, "bounding_box_msn_rtree")
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_mzdb_stats_struct() {
        let stats = MzDbStats {
            spectrum_count: 1000,
            ms1_count: 100,
            ms2_count: 900,
            chromatogram_count: 5,
            bounding_box_count: 500,
            run_slice_count: 50,
            min_mz: Some(100.0),
            max_mz: Some(2000.0),
            min_rt: Some(0.0),
            max_rt: Some(60.0),
        };
        
        assert_eq!(stats.spectrum_count, 1000);
        assert_eq!(stats.ms1_count + stats.ms2_count, stats.spectrum_count);
    }
}
