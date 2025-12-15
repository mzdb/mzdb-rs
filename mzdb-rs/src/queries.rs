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

use crate::model::*;
use crate::model::DataMode::Fitted;
use crate::rtree::{SQLITE_RTREE_LB_CORR, SQLITE_RTREE_UB_CORR};
use crate::query_utils::{
    query_single_string, query_all_strings,
    query_single_i64, query_single_i64_required,
    query_single_f32, query_single_f64,
    get_table_records_count as get_table_records_count_impl,
};

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
    query_single_string(db, "SELECT param_tree FROM mzdb LIMIT 1")
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
        .prepare("SELECT * FROM data_encoding")
        .dot()?;

    let values = stmt
        .query_map([], |row| {
            let mode_as_str: String = row.get(1)?;
            let byte_order_as_str: String = row.get(3)?;
            let mz_precision: u32 = row.get(4)?;
            let intensity_precision: u32 = row.get(5)?;

            let mode = if mode_as_str == "fitted" {
                DataMode::Fitted
            } else if mode_as_str == "centroid" {
                DataMode::Centroid
            } else {
                DataMode::Profile
            };

            let byte_order = if byte_order_as_str == "little_endian" {
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

            RusqliteResult::Ok(DataEncoding {
                id: row.get(0)?,
                mode,
                peak_encoding,
                compression: row.get(2)?,
                byte_order,
            })
        })
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
// Bounding box and spectrum slice parsing
// ============================================================================

fn bytes_to_int(bytes: &[u8; 4]) -> i32 {
    i32::from_le_bytes(*bytes)
}

pub fn create_bbox(row: &Row) -> Result<BoundingBox> {
    let bb_id: i64 = row.get(0).dot()?;
    let blob_data = row
        .get_ref(1)
        .dot()?
        .as_blob()
        .dot()?;
    let run_slice_id: i64 = row.get(2).dot()?;
    let first_spectrum_id: i64 = row.get(3).dot()?;
    let last_spectrum_id: i64 = row.get(4).dot()?;

    Ok(BoundingBox {
        id: bb_id,
        blob_data: blob_data.to_vec(),
        run_slice_id,
        first_spectrum_id,
        last_spectrum_id,
    })
}

/// Index a bounding box to extract spectrum slice metadata
pub fn index_bbox(bbox: &BoundingBox, cache: &DataEncodingsCache) -> Result<BoundingBoxIndex> {
    let estimated_slice_count = (1 + bbox.last_spectrum_id - bbox.first_spectrum_id) as usize;

    let mut slices_indexes = Vec::with_capacity(estimated_slice_count);
    let mut spectra_ids = Vec::with_capacity(estimated_slice_count);
    let mut peaks_counts = Vec::with_capacity(estimated_slice_count);

    let mut slices_count = 0;

    let blob_data = bbox.blob_data.as_slice();
    let n_bytes = blob_data.len();
    let mut int_as_bytes = [0u8; 4];

    let mut bytes_idx = 0;
    while bytes_idx < n_bytes {
        slices_indexes.push(bytes_idx);

        int_as_bytes.clone_from_slice(&blob_data[bytes_idx..=bytes_idx + 3]);
        let spectrum_id = bytes_to_int(&int_as_bytes) as i64;
        spectra_ids.push(spectrum_id);

        int_as_bytes.clone_from_slice(&blob_data[bytes_idx + 4..=bytes_idx + 7]);
        let peak_count = bytes_to_int(&int_as_bytes) as usize;
        peaks_counts.push(peak_count);

        let de = cache
            .get_data_encoding_by_spectrum_id(&spectrum_id)
            .ok_or(anyhow!("can't find data encoding"))
            .dot()?;

        let peak_size = de.get_peak_size();

        slices_count += 1;
        bytes_idx = bytes_idx + 8 + (peak_size * peak_count);
    }

    Ok(BoundingBoxIndex {
        bb_id: bbox.id,
        spectrum_slices_count: slices_count,
        spectra_ids,
        slices_indexes,
        peaks_counts,
    })
}

fn read_spectrum_slice_data(
    bb_bytes: &[u8],
    peaks_start_pos: usize,
    peaks_count: usize,
    de: &DataEncoding,
    min_mz: Option<f64>,
    max_mz: Option<f64>,
) -> Result<SpectrumData> {
    let data_mode = de.mode;
    let pe = de.peak_encoding;
    let byte_order = de.byte_order;

    let peak_size = de.get_peak_size();

    let mut float_bytes = [0u8; 4];
    let mut double_bytes = [0u8; 8];

    let mut bytes_to_double = |offset: usize, decode_float: bool| -> (f64, usize) {
        if decode_float {
            float_bytes.clone_from_slice(&bb_bytes[offset..offset + 4]);
            let value = if byte_order == ByteOrder::BigEndian {
                f32::from_be_bytes(float_bytes) as f64
            } else {
                f32::from_le_bytes(float_bytes) as f64
            };
            (value, 4)
        } else {
            double_bytes.clone_from_slice(&bb_bytes[offset..offset + 8]);
            let value = if byte_order == ByteOrder::BigEndian {
                f64::from_be_bytes(double_bytes)
            } else {
                f64::from_le_bytes(double_bytes)
            };
            (value, 8)
        }
    };

    let mut filtered_peaks_count = 0;
    let mut filtered_peaks_start_idx = 0;

    if min_mz.is_none() && max_mz.is_none() {
        filtered_peaks_count = peaks_count;
        filtered_peaks_start_idx = peaks_start_pos;
    } else {
        let max_mz_threshold = max_mz.unwrap_or(f64::MAX);

        let mut i = 0;
        while i < peaks_count {
            let peak_start_pos: usize = peaks_start_pos + i * peak_size;
            let (mz, _offset) = bytes_to_double(peak_start_pos, pe == PeakEncoding::LowRes);

            if let Some(min) = min_mz {
                if mz >= min && mz <= max_mz_threshold {
                    filtered_peaks_count += 1;
                    if filtered_peaks_start_idx == 0 {
                        filtered_peaks_start_idx = peak_start_pos;
                    }
                }
            }
            i += 1;
        }
    }

    let mut mz_array: Vec<f64> = Vec::with_capacity(filtered_peaks_count);
    let mut intensity_array: Vec<f32> = Vec::with_capacity(filtered_peaks_count);
    let mut lwhm_array: Vec<f32> = if data_mode == Fitted {
        Vec::with_capacity(filtered_peaks_count)
    } else {
        Vec::new()
    };
    let mut rwhm_array: Vec<f32> = if data_mode == Fitted {
        Vec::with_capacity(filtered_peaks_count)
    } else {
        Vec::new()
    };

    let mut float_bytes2 = [0u8; 4];
    let mut double_bytes2 = [0u8; 8];

    let mut bytes_to_float = |offset: usize, decode_float: bool| -> (f32, usize) {
        if decode_float {
            float_bytes2.clone_from_slice(&bb_bytes[offset..offset + 4]);
            let value = if byte_order == ByteOrder::BigEndian {
                f32::from_be_bytes(float_bytes2)
            } else {
                f32::from_le_bytes(float_bytes2)
            };
            (value, 4)
        } else {
            double_bytes2.clone_from_slice(&bb_bytes[offset..offset + 8]);
            let value = if byte_order == ByteOrder::BigEndian {
                f64::from_be_bytes(double_bytes2) as f32
            } else {
                f64::from_le_bytes(double_bytes2) as f32
            };
            (value, 8)
        }
    };

    let mut peak_idx = 0;
    while peak_idx < filtered_peaks_count {
        let peak_bytes_index = filtered_peaks_start_idx + peak_idx * peak_size;
        let (mz, offset) = bytes_to_double(peak_bytes_index, pe == PeakEncoding::LowRes);
        mz_array.push(mz);

        let (intensity, _offset) =
            bytes_to_float(peak_bytes_index + offset, pe != PeakEncoding::NoLoss);
        intensity_array.push(intensity);

        if data_mode == Fitted {
            let mz_int_size = pe as usize;
            lwhm_array.push(bytes_to_float(peak_bytes_index + mz_int_size, true).0);
            rwhm_array.push(bytes_to_float(peak_bytes_index + mz_int_size + 4, true).0);
        }

        peak_idx += 1;
    }

    Ok(SpectrumData {
        data_encoding: de.clone(),
        peaks_count,
        mz_array,
        intensity_array,
        lwhm_array,
        rwhm_array,
    })
}

pub fn read_spectrum_slice_data_at(
    bounding_box: &BoundingBox,
    bbox_index: &BoundingBoxIndex,
    data_encoding: &DataEncoding,
    spectrum_slice_idx: usize,
    min_mz: Option<f64>,
    max_mz: Option<f64>,
) -> Result<SpectrumData> {
    let peaks_count = bbox_index.peaks_counts[spectrum_slice_idx];
    let peaks_start_pos = bbox_index.slices_indexes[spectrum_slice_idx] + 8;

    read_spectrum_slice_data(
        &bounding_box.blob_data,
        peaks_start_pos,
        peaks_count,
        data_encoding,
        min_mz,
        max_mz,
    )
}

pub fn merge_spectrum_slices(
    sd_slices: &mut Vec<SpectrumData>,
    peaks_count: usize,
) -> Result<SpectrumData> {
    let data_encoding = sd_slices
        .first()
        .map(|sd| sd.data_encoding.clone())
        .context("sd_slices is empty")
        .dot()?;

    let data_mode = data_encoding.mode;

    let mut mz_array: Vec<f64> = Vec::with_capacity(peaks_count);
    let mut intensity_array: Vec<f32> = Vec::with_capacity(peaks_count);
    let mut lwhm_array: Vec<f32> = if data_mode == Fitted {
        Vec::with_capacity(peaks_count)
    } else {
        Vec::new()
    };
    let mut rwhm_array: Vec<f32> = if data_mode == Fitted {
        Vec::with_capacity(peaks_count)
    } else {
        Vec::new()
    };

    for sd_slice in sd_slices {
        mz_array.append(&mut sd_slice.mz_array);
        intensity_array.append(&mut sd_slice.intensity_array);

        if data_mode == Fitted {
            lwhm_array.append(&mut sd_slice.lwhm_array);
            rwhm_array.append(&mut sd_slice.rwhm_array);
        }
    }

    Ok(SpectrumData {
        data_encoding,
        peaks_count,
        mz_array,
        intensity_array,
        lwhm_array,
        rwhm_array,
    })
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
        .spectrum_headers
        .get((spectrum_id - 1) as usize)
        .context(format!("can't retrieve spectrum with ID={}", spectrum_id))
        .dot()?;

    // Use parameterized query for better performance (SQLite caches the prepared statement)
    let bb_first_spec_id: i64 = db
        .prepare_cached(spectrum_sql::GET_BB_FIRST_SPECTRUM_ID)
        .dot()?
        .query_row([spectrum_id], |row| row.get(0))
        .optional()
        .dot()?
        .ok_or_else(|| anyhow!("can't get bb_first_spectrum_id for spectrum with ID = {}", spectrum_id))?;

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
                "can't find slice index for spectrum with ID={} in bounding box with ID={}",
                spectrum_id,
                cur_bb.id
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
        .dot()?;

        sd_slices.push(spectrum_slice_data);
    }

    let peaks_count = sd_slices.iter().map(|slice| slice.peaks_count).sum();
    let spectrum_data = merge_spectrum_slices(&mut sd_slices, peaks_count).dot()?;

    Ok(Spectrum {
        header: spectrum_header.clone(),
        data: spectrum_data,
    })
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
                        Some(min_mz),
                        Some(max_mz),
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

    if let Some(first_spectrum_data) = spectrum_data_list.first() {
        if !first_spectrum_data.lwhm_array.is_empty() && !first_spectrum_data.rwhm_array.is_empty()
        {
            final_lwhm_array = Some(Vec::with_capacity(peaks_count));
            final_rwhm_array = Some(Vec::with_capacity(peaks_count));
        }
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
    mz: f64,
    mz_tol_ppm: f64,
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
        mz - mz_tol_da,
        mz + mz_tol_da,
        min_rt_for_rtree,
        max_rt_for_rtree,
        entity_cache,
    )?;

    Ok(spectrum_slices_to_xic(spectrum_slices, mz, mz_tol_ppm, method))
}

fn get_msn_xic(
    connection: &Connection,
    parent_mz: f64,
    fragment_mz: f64,
    fragment_mz_tol_ppm: f64,
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
        fragment_mz - fragment_mz_tol_da,
        fragment_mz + fragment_mz_tol_da,
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
    searched_mz: f64,
    mz_tol_ppm: f64,
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
