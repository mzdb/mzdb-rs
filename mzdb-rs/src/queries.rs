//! Database query functions for mzDB files
//!
//! This module provides functions for querying mzDB SQLite databases, including:
//! - Metadata queries (versions, param trees, counts)
//! - Data encoding retrieval
//! - Spectrum and bounding box parsing
//! - XIC (Extracted Ion Chromatogram) generation
//!
//! Most functions take a `&Connection` reference and return `Result<T>`.
#![allow(unused)]

use std::collections::HashMap;

use anyhow_ext::{anyhow, bail, Context, Result};
use rusqlite::{params, Connection, OptionalExtension, Row};
use rusqlite::Result as RusqliteResult;
use serde_rusqlite::from_rows;
use smallvec::SmallVec;

use crate::bounding_box::{create_bbox, index_bbox, read_spectrum_slice_data_at, merge_spectrum_slices};
use crate::model::*;
use crate::rtree::{SQLITE_RTREE_LB_CORR, SQLITE_RTREE_UB_CORR};
use crate::query_utils::{
    query_single_string, query_single_string_latin1_safe, query_all_strings,
    query_single_i64, query_single_i64_required,
    query_single_f32, query_single_f64,
    get_table_records_count as get_table_records_count_impl,
    table_exists,
    parse_data_encoding_from_row,
};
use crate::xml::parse_isolation_window_from_xml;

pub const BOUNDING_BOX_TABLE_NAME: &str = "bounding_box";
pub const DATA_ENCODING_TABLE_NAME: &str = "data_encoding";
pub const SPECTRUM_TABLE_NAME: &str = "spectrum";

// ============================================================================
// Public metadata query functions
// ============================================================================

/// Get the mzDB version
pub fn get_mzdb_version(db: &Connection) -> Result<Option<String>> {
    query_single_string(db, "SELECT version FROM mzdb LIMIT 1")
}

/// Get the mzDB writer version (pwiz-mzdb)
pub fn get_pwiz_mzdb_version(db: &Connection) -> Result<Option<String>> {
    query_single_string(db, "SELECT version FROM software WHERE name LIKE '%mzDB'")
}

/// Get all param trees of the chromatogram table
pub fn get_param_tree_chromatogram(db: &Connection) -> Result<Vec<String>> {
    query_all_strings(db, "SELECT param_tree FROM chromatogram")
}

/// Get the param tree of a spectrum by ID
pub fn get_param_tree_spectrum(db: &Connection, spectrum_id: i64) -> Result<Option<String>> {
    query_single_string(
        db,
        &format!("SELECT param_tree FROM spectrum WHERE id = {}", spectrum_id),
    )
}

/// Get param tree of the mzdb table
pub fn get_param_tree_mzdb(db: &Connection) -> Result<Option<String>> {
    // Use Latin-1 safe variant as legacy pwiz-mzDB files may contain Latin-1 encoded text
    query_single_string_latin1_safe(db, "SELECT param_tree FROM mzdb LIMIT 1")
}

/// Get the processing method param tree
pub fn get_processing_method_param_tree(db: &Connection) -> Result<Vec<String>> {
    query_all_strings(db, "SELECT param_tree FROM processing_method")
}

/// Get the last cycle number of spectrum
pub fn get_last_cycle_number(db: &Connection) -> Result<Option<i64>> {
    query_single_i64(db, "SELECT cycle FROM spectrum ORDER BY id DESC LIMIT 1")
}

/// Get the last retention time
pub fn get_last_time(db: &Connection) -> Result<Option<f32>> {
    query_single_f32(db, "SELECT time FROM spectrum ORDER BY id DESC LIMIT 1")
}

/// Get max MS level from run slices
pub fn get_max_ms_level(db: &Connection) -> Result<Option<i64>> {
    query_single_i64(db, "SELECT max(ms_level) FROM run_slice")
}

/// Get the number of bounding boxes for a run slice
pub fn get_run_slice_bounding_boxes_count(db: &Connection, run_slice_id: i64) -> Result<Option<i64>> {
    query_single_i64(
        db,
        &format!(
            "SELECT count(*) FROM bounding_box WHERE bounding_box.run_slice_id = {}",
            run_slice_id
        ),
    )
}

/// Get the number of spectra for a given MS level
pub fn get_spectra_count_by_ms_level(db: &Connection, ms_level: i64) -> Result<Option<i64>> {
    query_single_i64(
        db,
        &format!("SELECT count(id) FROM spectrum WHERE ms_level = {}", ms_level),
    )
}

/// Get the number of records in a table.
/// 
/// This function first tries to get the count from sqlite_sequence (which is faster
/// for large tables with AUTOINCREMENT primary keys), and falls back to COUNT(*) 
/// if the table is not present in sqlite_sequence.
pub fn get_table_records_count(db: &Connection, table_name: &str) -> Result<Option<i64>> {
    get_table_records_count_impl(db, table_name)
}

/// Get the bb_first_spectrum_id for a spectrum
pub fn get_bounding_box_first_spectrum_id(db: &Connection, spectrum_id: i64) -> Result<Option<i64>> {
    query_single_i64(
        db,
        &format!("SELECT bb_first_spectrum_id FROM spectrum WHERE id = {}", spectrum_id),
    )
}

/// Get the run slice ID of a bounding box
pub fn get_run_slice_id(db: &Connection, bb_id: i64) -> Result<Option<i64>> {
    query_single_i64(
        db,
        &format!("SELECT run_slice_id FROM bounding_box WHERE id = {}", bb_id),
    )
}

/// Get the MS level of a run slice
pub fn get_ms_level_from_run_slice_id(db: &Connection, run_slice_id: i64) -> Result<Option<i64>> {
    query_single_i64(
        db,
        &format!("SELECT ms_level FROM run_slice WHERE run_slice.id = {}", run_slice_id),
    )
}

/// Get the MS level of a bounding box (via its run slice)
pub fn get_bounding_box_ms_level(db: &Connection, bb_id: i64) -> Result<Option<i64>> {
    let run_slice_id = query_single_i64_required(
        db,
        &format!("SELECT run_slice_id FROM bounding_box WHERE id = {}", bb_id),
    )?;
    
    query_single_i64(
        db,
        &format!("SELECT ms_level FROM run_slice WHERE run_slice.id = {}", run_slice_id),
    )
}

/// Get the data encoding ID for a bounding box
pub fn get_data_encoding_id(db: &Connection, bb_id: i64) -> Result<Option<i64>> {
    query_single_i64(
        db,
        &format!(
            "SELECT s.data_encoding_id FROM spectrum s, bounding_box b \
             WHERE b.id = {} AND b.first_spectrum_id = s.id",
            bb_id
        ),
    )
}

/// Get the count of data encodings
pub fn get_data_encoding_count(db: &Connection) -> Result<Option<i64>> {
    query_single_i64(db, "SELECT count(id) FROM data_encoding")
}

// ============================================================================
// Data encoding functions
// ============================================================================

pub fn list_data_encodings(db: &Connection) -> Result<Vec<DataEncoding>> {
    let mut stmt = db
        .prepare("SELECT id, mode, compression, byte_order, mz_precision, intensity_precision FROM data_encoding")
        .dot()?;

    let values = stmt
        .query_map([], parse_data_encoding_from_row)
        .dot()?;

    let mut result = Vec::new();
    for value in values {
        result.push(value.dot()?);
    }

    Ok(result)
}

pub fn list_get_spectra_data_encoding_ids(db: &Connection) -> Result<HashMap<i64, i64>> {
    let mut stmt = db
        .prepare("SELECT id, data_encoding_id FROM spectrum")
        .dot()?;
    let mut rows = stmt.query([]).dot()?;

    let mut mapping = HashMap::new();
    while let Some(row) = rows.next().dot()? {
        let id: i64 = row.get(0).dot()?;
        let data_encoding_id: i64 = row.get(1).dot()?;
        mapping.insert(id, data_encoding_id);
    }

    Ok(mapping)
}

// ============================================================================
// Spectrum retrieval
// ============================================================================

/// SQL for parameterized spectrum queries (enables SQLite query caching)
mod spectrum_sql {
    pub const GET_BB_FIRST_SPECTRUM_ID: &str = 
        "SELECT bb_first_spectrum_id FROM spectrum WHERE id = ?1";
    pub const COUNT_BB_BY_FIRST_SPECTRUM_ID: &str = 
        "SELECT count(id) FROM bounding_box WHERE first_spectrum_id = ?1";
    pub const GET_BB_BY_FIRST_SPECTRUM_ID: &str = 
        "SELECT * FROM bounding_box WHERE first_spectrum_id = ?1";
}

/// Get a single spectrum by ID
pub fn get_spectrum(
    db: &Connection,
    spectrum_id: i64,
    entity_cache: &EntityCache,
) -> Result<Spectrum> {
    let spectrum_header = entity_cache
        .get_spectrum_header(spectrum_id)
        .context(format!("can't retrieve spectrum with ID={}", spectrum_id))
        .dot()?;

    // Use parameterized query for better performance (SQLite caches the prepared statement)
    let bb_first_spec_id: Option<i64> = db
        .prepare_cached(spectrum_sql::GET_BB_FIRST_SPECTRUM_ID)
        .dot()?
        .query_row([spectrum_id], |row| row.get(0))
        .optional()
        .dot()?
        .ok_or_else(|| anyhow!("can't get bb_first_spectrum_id for spectrum with ID = {}", spectrum_id))?;

    // If bb_first_spectrum_id is NULL, this is an empty spectrum with no data
    let bb_first_spec_id = match bb_first_spec_id {
        Some(id) => id,
        None => {
            // Return empty spectrum
            return Ok(Spectrum {
                header: spectrum_header.clone(),
                data: SpectrumData {
                    data_encoding: entity_cache.data_encodings_cache
                        .get_data_encoding_by_spectrum_id(&spectrum_id)
                        .ok_or_else(|| anyhow!("can't retrieve data encoding for spectrum ID={}", spectrum_id))?
                        .clone(),
                    peaks_count: 0,
                    mz_array: vec![],
                    intensity_array: vec![],
                    lwhm_array: vec![],
                    rwhm_array: vec![],
                },
            });
        }
    };

    let bb_count: i64 = db
        .prepare_cached(spectrum_sql::COUNT_BB_BY_FIRST_SPECTRUM_ID)
        .dot()?
        .query_row([bb_first_spec_id], |row| row.get(0))
        .optional()
        .dot()?
        .ok_or_else(|| anyhow!("can't determine the number of bounding boxes for first_spectrum_id = {}", bb_first_spec_id))?;

    let mut stmt = db
        .prepare_cached(spectrum_sql::GET_BB_BY_FIRST_SPECTRUM_ID)
        .dot()?;

    let de_cache = &entity_cache.data_encodings_cache;

    let data_encoding = de_cache
        .get_data_encoding_by_spectrum_id(&spectrum_id)
        .ok_or_else(|| anyhow!("can't retrieve data encoding for spectrum ID={}", spectrum_id))?;

    let mut target_slice_idx: Option<usize> = None;
    let mut sd_slices: Vec<SpectrumData> = Vec::with_capacity(bb_count as usize);

    let mut rows = stmt.query([bb_first_spec_id])?;
    while let Some(row) = rows.next().dot()? {
        let cur_bb = create_bbox(row).dot()?;
        let bb_index = index_bbox(&cur_bb, de_cache).dot()?;

        if target_slice_idx.is_none() {
            target_slice_idx = bb_index
                .spectra_ids
                .iter()
                .enumerate()
                .find(|&(ref _slice_idx, &cur_spec_id)| cur_spec_id == spectrum_id)
                .map(|(slice_idx, _)| slice_idx);
        }

        let slice_idx = target_slice_idx.ok_or_else(|| {
            anyhow!(
                "can't find slice index for spectrum with ID={} in bounding box with ID={} (BB range: {}-{})",
                spectrum_id,
                cur_bb.id,
                cur_bb.first_spectrum_id,
                cur_bb.last_spectrum_id
            )
        })?;

        let spectrum_slice_data = read_spectrum_slice_data_at(
            &cur_bb,
            &bb_index,
            data_encoding,
            slice_idx,
            None,
            None,
        )
        .context(format!("Failed to read slice {} from BB {} (range: {}-{}, spectrum_id: {})",
                        slice_idx, cur_bb.id, cur_bb.first_spectrum_id, cur_bb.last_spectrum_id, spectrum_id))?;

        sd_slices.push(spectrum_slice_data);
    }

    let peaks_count = sd_slices.iter().map(|slice| slice.peaks_count).sum();
    let spectrum_data = merge_spectrum_slices(&mut sd_slices, peaks_count)
        .context(format!("Failed to merge {} spectrum slices with {} total peaks for spectrum {}",
                        sd_slices.len(), peaks_count, spectrum_id))?;

    Ok(Spectrum {
        header: spectrum_header.clone(),
        data: spectrum_data,
    })
}

/// Get simplified spectrum data by ID (m/z and intensity arrays only)
///
/// This is a convenience function that retrieves spectrum data and converts it
/// to `SimpleSpectrumData`, discarding encoding info and fitted peak parameters.
/// Useful for processing pipelines that only need m/z and intensity values.
///
/// # Arguments
/// * `db` - Database connection
/// * `spectrum_id` - The spectrum ID to retrieve
/// * `entity_cache` - Entity cache with headers and data encodings
///
/// # Returns
/// `SimpleSpectrumData` containing only m/z and intensity arrays
pub fn get_simple_spectrum_data(
    db: &Connection,
    spectrum_id: i64,
    entity_cache: &EntityCache,
) -> Result<SimpleSpectrumData> {
    let spectrum = get_spectrum(db, spectrum_id, entity_cache)?;
    Ok(SimpleSpectrumData::from(spectrum.data))
}

// ============================================================================
// Spectrum slices retrieval (for XIC)
// ============================================================================

pub fn get_ms_spectrum_slices(
    connection: &Connection,
    min_mz: f64,
    max_mz: f64,
    min_rt: f32,
    max_rt: f32,
    entity_cache: &EntityCache,
) -> Result<Vec<SpectrumSlice>> {
    get_spectrum_slices_in_ranges(connection, min_mz, max_mz, min_rt, max_rt, 1, 0.0, entity_cache)
}

pub fn get_msn_spectrum_slices(
    connection: &Connection,
    parent_mz: f64,
    min_frag_mz: f64,
    max_frag_mz: f64,
    min_rt: f32,
    max_rt: f32,
    entity_cache: &EntityCache,
) -> Result<Vec<SpectrumSlice>> {
    get_spectrum_slices_in_ranges(
        connection,
        min_frag_mz,
        max_frag_mz,
        min_rt,
        max_rt,
        2,
        parent_mz,
        entity_cache,
    )
}

#[allow(clippy::too_many_arguments)]
fn get_spectrum_slices_in_ranges(
    connection: &Connection,
    min_mz: f64,
    max_mz: f64,
    min_rt: f32,
    max_rt: f32,
    ms_level: u8,
    parent_mz: f64,
    entity_cache: &EntityCache,
) -> Result<Vec<SpectrumSlice>> {
    let bb_sizes = entity_cache.bb_sizes;
    let (rt_width, mz_height) = if ms_level == 1 {
        (bb_sizes.bb_rt_width_ms1, bb_sizes.bb_mz_height_ms1)
    } else {
        (bb_sizes.bb_rt_width_msn, bb_sizes.bb_mz_height_msn)
    };

    let bb_min_mz = (min_mz - mz_height) * SQLITE_RTREE_LB_CORR;
    let bb_max_mz = (max_mz + mz_height) * SQLITE_RTREE_UB_CORR;
    let bb_min_rt = (min_rt - rt_width) * SQLITE_RTREE_LB_CORR as f32;
    let bb_max_rt = (max_rt + rt_width) * SQLITE_RTREE_UB_CORR as f32;

    let sql_query = if ms_level == 1 {
        "SELECT * FROM bounding_box WHERE id IN (
            SELECT id FROM bounding_box_rtree
            WHERE min_mz >= ? AND max_mz <= ?
            AND min_time >= ? AND max_time <= ?
        )
        ORDER BY first_spectrum_id"
    } else {
        "SELECT * FROM bounding_box WHERE id IN (
            SELECT id FROM bounding_box_msn_rtree
            WHERE min_ms_level = ? AND max_ms_level = ?
            AND min_parent_mz <= ? AND max_parent_mz >= ?
            AND min_mz >= ? AND max_mz <= ?
            AND min_time >= ? AND max_time <= ?
        )
        ORDER BY first_spectrum_id"
    };

    let mut stmt = connection.prepare(sql_query)?;

    let mut rows = if ms_level == 1 {
        stmt.query(params![bb_min_mz, bb_max_mz, bb_min_rt, bb_max_rt])?
    } else {
        stmt.query(params![
            ms_level,
            ms_level,
            parent_mz,
            parent_mz,
            bb_min_mz,
            bb_max_mz,
            bb_min_rt,
            bb_max_rt
        ])?
    };

    let spec_headers = &entity_cache.spectrum_headers;
    let spectrum_header_by_id: HashMap<_, _> = spec_headers
        .iter()
        .map(|header| (header.id, header))
        .collect();

    let de_cache = &entity_cache.data_encodings_cache;

    let mut spectrum_data_list_by_id: HashMap<i64, Vec<SpectrumData>> = HashMap::new();
    let mut peaks_count_by_spectrum_id: HashMap<i64, usize> = HashMap::new();

    while let Some(bb_record) = rows.next()? {
        let cur_bb = create_bbox(bb_record)?;

        let data_encoding = de_cache
            .get_data_encoding_by_spectrum_id(&cur_bb.first_spectrum_id)
            .ok_or_else(|| {
                anyhow!(
                    "can't retrieve data encoding for spectrum ID={}",
                    cur_bb.first_spectrum_id
                )
            })?;

        let bb_index = index_bbox(&cur_bb, de_cache).dot()?;
        let bb_spectrum_ids = &bb_index.spectra_ids;

        for (spectrum_idx, &spectrum_id) in bb_spectrum_ids.iter().enumerate() {
            if let Some(sh) = spectrum_header_by_id.get(&spectrum_id) {
                let current_rt = sh.time;
                if current_rt >= min_rt && current_rt <= max_rt {
                    let spectrum_slice_data = read_spectrum_slice_data_at(
                        &cur_bb,
                        &bb_index,
                        data_encoding,
                        spectrum_idx,
                        Some(min_mz as f32),
                        Some(max_mz as f32),
                    )?;

                    if spectrum_slice_data.peaks_count != 0 {
                        *peaks_count_by_spectrum_id.entry(spectrum_id).or_default() +=
                            spectrum_slice_data.peaks_count;
                        spectrum_data_list_by_id
                            .entry(spectrum_id)
                            .or_default()
                            .push(spectrum_slice_data);
                    }
                }
            }
        }
    }

    let mut final_spectrum_slices = Vec::with_capacity(spectrum_data_list_by_id.len());
    for (spectrum_id, spectrum_data_list) in spectrum_data_list_by_id {
        let peaks_count = peaks_count_by_spectrum_id.get(&spectrum_id).cloned().unwrap_or(0);
        let final_spectrum_data = merge_spectrum_data_list(spectrum_data_list, peaks_count)?;

        let header = spectrum_header_by_id
            .get(&spectrum_id)
            .ok_or_else(|| anyhow!("spectrum header not found for ID={}", spectrum_id))?;

        let spectrum_slice = SpectrumSlice {
            spectrum: Spectrum {
                header: (*header).clone(),
                data: final_spectrum_data,
            },
            run_slice_id: 0,
        };

        final_spectrum_slices.push(spectrum_slice);
    }

    Ok(final_spectrum_slices)
}

fn merge_spectrum_data_list(
    spectrum_data_list: Vec<SpectrumData>,
    peaks_count: usize,
) -> Result<SpectrumData> {
    if spectrum_data_list.is_empty() {
        bail!("Spectrum data list should not be empty")
    }

    let mut final_mz_array = Vec::with_capacity(peaks_count);
    let mut final_intensity_array = Vec::with_capacity(peaks_count);
    let mut final_lwhm_array: Option<Vec<f32>> = None;
    let mut final_rwhm_array: Option<Vec<f32>> = None;

    if let Some(first_spectrum_data) = spectrum_data_list.first()
        && !first_spectrum_data.lwhm_array.is_empty()
        && !first_spectrum_data.rwhm_array.is_empty()
    {
        final_lwhm_array = Some(Vec::with_capacity(peaks_count));
        final_rwhm_array = Some(Vec::with_capacity(peaks_count));
    }

    let data_encoding = spectrum_data_list[0].data_encoding.clone();
    for spectrum_data in spectrum_data_list {
        final_mz_array.extend(spectrum_data.mz_array);
        final_intensity_array.extend(spectrum_data.intensity_array);

        if let (Some(lwhm), Some(rwhm)) =
            (&mut final_lwhm_array, &mut final_rwhm_array)
        {
            lwhm.extend(spectrum_data.lwhm_array);
            rwhm.extend(spectrum_data.rwhm_array);
        }
    }

    Ok(SpectrumData {
        data_encoding,
        peaks_count,
        mz_array: final_mz_array,
        intensity_array: final_intensity_array,
        lwhm_array: final_lwhm_array.unwrap_or_default(),
        rwhm_array: final_rwhm_array.unwrap_or_default(),
    })
}

// ============================================================================
// XIC (Extracted Ion Chromatogram) functions
// ============================================================================

pub fn get_ms_xic(
    connection: &Connection,
    mz: f32,
    mz_tol_ppm: f32,
    min_rt: Option<f32>,
    max_rt: Option<f32>,
    method: XicMethod,
    entity_cache: &EntityCache,
) -> Result<Vec<XicPeak>> {
    let mz_tol_da = mz * mz_tol_ppm / 1e6;

    let min_rt_for_rtree = min_rt.unwrap_or(0.0);
    let max_rt_for_rtree = match max_rt {
        Some(rt) => rt,
        None => get_last_time(connection)?
            .context("can't retrieve the last spectrum retention time information")?,
    };

    let spectrum_slices = get_ms_spectrum_slices(
        connection,
        (mz - mz_tol_da) as f64,
        (mz + mz_tol_da) as f64,
        min_rt_for_rtree,
        max_rt_for_rtree,
        entity_cache,
    )?;

    Ok(spectrum_slices_to_xic(spectrum_slices, mz, mz_tol_ppm, method))
}

#[allow(clippy::too_many_arguments)]
fn get_msn_xic(
    connection: &Connection,
    parent_mz: f64,
    fragment_mz: f32,
    fragment_mz_tol_ppm: f32,
    min_rt: Option<f32>,
    max_rt: Option<f32>,
    method: XicMethod,
    entity_cache: &EntityCache,
) -> Result<Vec<XicPeak>> {
    let fragment_mz_tol_da = fragment_mz * fragment_mz_tol_ppm / 1e6;

    let min_rt_for_rtree = min_rt.unwrap_or(0.0);
    let max_rt_for_rtree = match max_rt {
        Some(rt) => rt,
        None => get_last_time(connection)?
            .context("can't retrieve the last spectrum retention time information")?,
    };

    let spectrum_slices = get_msn_spectrum_slices(
        connection,
        parent_mz,
        (fragment_mz - fragment_mz_tol_da) as f64,
        (fragment_mz + fragment_mz_tol_da) as f64,
        min_rt_for_rtree,
        max_rt_for_rtree,
        entity_cache,
    )?;

    Ok(spectrum_slices_to_xic(
        spectrum_slices,
        fragment_mz,
        fragment_mz_tol_ppm,
        method,
    ))
}

fn spectrum_slices_to_xic(
    spectrum_slices: Vec<SpectrumSlice>,
    searched_mz: f32,
    mz_tol_ppm: f32,
    method: XicMethod,
) -> Vec<XicPeak> {
    if spectrum_slices.is_empty() {
        return Vec::new();
    }

    let mut xic_peaks = Vec::with_capacity(spectrum_slices.len());

    match method {
        XicMethod::Max => {
            for sl in spectrum_slices {
                let spectrum_data = sl.spectrum.data;
                if spectrum_data.peaks_count > 0 {
                    let max_dp_opt = spectrum_data
                        .intensity_array
                        .iter()
                        .enumerate()
                        .max_by(|&a, &b| (*a.1).total_cmp(b.1));

                    if let Some((max_dp_idx, max_intensity)) = max_dp_opt {
                        xic_peaks.push(XicPeak {
                            mz: spectrum_data.mz_array[max_dp_idx],
                            intensity: *max_intensity,
                            rt: sl.spectrum.header.time,
                        });
                    }
                }
            }
        }
        XicMethod::Nearest => {
            for sl in spectrum_slices {
                let spectrum_data = sl.spectrum.data;
                if spectrum_data.peaks_count > 0 {
                    let rt = sl.spectrum.header.time;
                    if let Some(nearest_peak) =
                        spectrum_data.get_nearest_peak(searched_mz, mz_tol_ppm, rt)
                    {
                        xic_peaks.push(nearest_peak);
                    }
                }
            }
        }
    }

    xic_peaks
}

// ============================================================================
// Spectrum header queries
// ============================================================================

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

// ============================================================================
// Run slice queries
// ============================================================================

/// Helper function to map a row to RunSliceHeader
fn run_slice_from_row(row: &Row) -> rusqlite::Result<RunSliceHeader> {
    Ok(RunSliceHeader {
        id: row.get(0)?,
        ms_level: row.get(1)?,
        number: row.get(2)?,
        begin_mz: row.get(3)?,
        end_mz: row.get(4)?,
        run_id: row.get(5)?,
    })
}

/// Get all run slice headers
pub fn list_run_slices(db: &Connection) -> Result<Vec<RunSliceHeader>> {
    let mut stmt = db.prepare(
        "SELECT id, ms_level, number, begin_mz, end_mz, run_id FROM run_slice ORDER BY number"
    )?;

    let slices = stmt.query_map([], run_slice_from_row)?;
    slices.collect::<rusqlite::Result<Vec<_>>>().map_err(Into::into)
}

/// Get run slices for a specific MS level
pub fn list_run_slices_by_ms_level(db: &Connection, ms_level: i64) -> Result<Vec<RunSliceHeader>> {
    let mut stmt = db.prepare(
        "SELECT id, ms_level, number, begin_mz, end_mz, run_id FROM run_slice \
         WHERE ms_level = ?1 ORDER BY number"
    )?;

    let slices = stmt.query_map([ms_level], run_slice_from_row)?;
    slices.collect::<rusqlite::Result<Vec<_>>>().map_err(Into::into)
}

/// Get a specific run slice by ID
pub fn get_run_slice(db: &Connection, id: i64) -> Result<Option<RunSliceHeader>> {
    let result = db
        .prepare("SELECT id, ms_level, number, begin_mz, end_mz, run_id FROM run_slice WHERE id = ?1")?
        .query_row([id], run_slice_from_row)
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
        .query_row([ms_level as f64, mz], run_slice_from_row)
        .optional()?;
    Ok(result)
}

/// Get the number of run slices
pub fn get_run_slice_count(db: &Connection) -> Result<i64> {
    Ok(get_table_records_count_impl(db, "run_slice")?.unwrap_or(0))
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
// Extended bounding box queries
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
    Ok(get_table_records_count_impl(db, "bounding_box")?.unwrap_or(0))
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
    let spectrum_count = get_table_records_count_impl(db, "spectrum")?.unwrap_or(0);

    // These need WHERE clauses so we use COUNT(*) directly
    let ms1_count: i64 = db
        .prepare("SELECT COUNT(*) FROM spectrum WHERE ms_level = 1")?
        .query_row([], |row| row.get(0))?;

    let ms2_count: i64 = db
        .prepare("SELECT COUNT(*) FROM spectrum WHERE ms_level = 2")?
        .query_row([], |row| row.get(0))?;

    let chromatogram_count = get_table_records_count_impl(db, "chromatogram")?.unwrap_or(0);
    let bounding_box_count = get_table_records_count_impl(db, "bounding_box")?.unwrap_or(0);
    let run_slice_count = get_table_records_count_impl(db, "run_slice")?.unwrap_or(0);

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
// DIA/SWATH specific queries
// ============================================================================

/// Get unique MS2 isolation windows (for DIA data) by parsing precursor_list XML
pub fn get_isolation_windows(db: &Connection) -> Result<Vec<MzRange>> {
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