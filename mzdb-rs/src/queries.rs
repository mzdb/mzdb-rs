#![allow(
dead_code
)]

use std::collections::{BTreeMap, HashMap};

use anyhow::*;
use crate::anyhow_ext::*;
//use itertools::Itertools;

use rusqlite::{params, Connection, OptionalExtension, Row, Statement};
use rusqlite::{Result as RusqliteResult};
use crate::model::*;
use crate::model::DataMode::FITTED;

pub const BOUNDING_BOX_TABLE_NAME: &'static str = "bounding_box";
pub const DATA_ENCODING_TABLE_NAME: &'static str = "data_encoding";
pub const SPECTRUM_TABLE_NAME: &'static str = "spectrum";

//const SQLQUERY_SINGLEMSLEVEL: &'static str = "SELECT bounding_box.* FROM bounding_box, spectrum WHERE spectrum.id = bounding_box.first_spectrum_id AND spectrum.ms_level=?";


/*macro_rules! here {
    () => {
        &Location {
            file: file!(),
            line: line!(),
            column: column!(),
        }
    };
}*/

fn _get_first_string_using_stmt2(stmt: &mut Statement) -> Result<Option<String>> {
    stmt.query_row([], |row|  row.get(0)).optional().map_err(anyhow::Error::msg)
}

fn _get_first_string_from_query(db: &Connection, query_str: &str) -> anyhow::Result<Option<String>> {
    let mut stmt = db.prepare(query_str).location(here!())?;
    _get_first_string_using_stmt2(&mut stmt)
}

fn list_strings_using_stmt2(stmt: &mut Statement) -> anyhow::Result<Vec<String>> {

    let values= stmt.query_map(
        [],
        |row| {
            row.get(0)
        }
    ).location(here!())?;

    let mut strings: Vec<String> = Vec::new();
    for value in values {
        strings.push(value.location(here!())?);
    }

    Ok(strings)
}

fn get_strings(db: &Connection, query_str: &str) -> anyhow::Result<Vec<String>> {
    let mut stmt: Statement = db.prepare(query_str).location(here!())?;
    list_strings_using_stmt2(&mut stmt)
}

fn get_first_int_using_stmt(stmt: &mut Statement) -> Result<Option<i64>> {
    stmt.query_row([], |row| row.get(0)).optional().map_err(anyhow::Error::msg)
}

fn get_first_int(db: &Connection, query_str: &str) -> anyhow::Result<Option<i64>> {
    let mut stmt = db.prepare(query_str).location(here!())?;

    get_first_int_using_stmt(&mut stmt).location(here!())
}

fn get_first_int_using_stmt_no_option(stmt: &mut Statement) -> rusqlite::Result<i64> {
    stmt.query_row([], |row| row.get(0))
}

fn get_first_int_no_option(db: &Connection, query_str: &str) -> rusqlite::Result<i64> {
    let mut stmt = db.prepare(query_str)?;
    get_first_int_using_stmt_no_option(&mut stmt)
}

pub fn get_processing_method_param_tree(db: &Connection) -> anyhow::Result<Vec<String>> {
    let mut stmt: Statement = db.prepare("SELECT param_tree FROM processing_method").location(here!())?;
    list_strings_using_stmt2(&mut stmt)
}

fn get_first_string(db: &Connection, query_str: &str) -> anyhow::Result<Option<String>> {
    //let mut stmt: Statement = db.prepare(query_str).location(here!())?;
    _get_first_string_from_query(db, query_str)
}

fn get_first_real_using_stmt(stmt: &mut Statement) -> Result<Option<f32>> {
    stmt.query_row([], |row| row.get(0)).optional().map_err(anyhow::Error::msg)
}

fn get_first_real_from_query(db: &Connection, query_str: &str) -> anyhow::Result<Option<f32>> {
    let mut stmt = db.prepare(query_str).location(here!())?;

    get_first_real_using_stmt(&mut stmt)
}

fn get_first_f64_stmt(stmt: &mut Statement) -> Result<Option<f64>> {
    stmt.query_row([], |row| row.get(0)).optional().map_err(anyhow::Error::msg)
}

fn get_first_f64(db: &Connection, query_str: &str) -> anyhow::Result<Option<f64>> {
    let mut stmt = db.prepare(query_str).location(here!())?;

    get_first_f64_stmt(&mut stmt)
}

fn list_int_using_statement(stmt: &mut Statement) -> anyhow::Result<Vec<i64>> {
    let values = stmt.query_map(
        [],
        |row| {
            row.get(0)
        },
    )?;

    let mut ints: Vec<i64> = Vec::new();
    for value in values {
        ints.push(value.location(here!())?);
    }
    Ok(ints)
}

fn get_ints(db: &Connection, query_str: &str) -> anyhow::Result<Vec<i64>> {
    let mut stmt: Statement = db.prepare(query_str).location(here!())?;
    list_int_using_statement(&mut stmt)
}

//----------------------------------------------------------------------//

/// Get the mzDB version
///
/// # Arguments
///
/// * `db` - A reference to a `Connection` struct
///
/// # Returns
///
/// A `Result` containing an `Option` of a `String` representing the mzDB version.
/// Returns an `Err` variant if there is an error executing the SQL query.
pub fn get_mzdb_version(db: &Connection) -> Result<Option<String>> {
    _get_first_string_from_query(
        &db,
        "SELECT version FROM mzdb LIMIT 1",
    )
}

/// Get the mzDB writer version
///
/// # Arguments
///
/// * `db` - A reference to a `Connection` struct
///
/// # Returns
///
/// A `Result` containing an `Option` of a `String` representing the mzDB writer version.
/// Returns an `Err` variant if there is an error executing the SQL query.
pub fn get_pwiz_mzdb_version(db: &Connection) -> Result<Option<String>> {
    get_first_string(
        &db,
        "SELECT version FROM software WHERE name LIKE '%mzDB'",
    )
}

/// Get all param trees of the chromatogram table
///
/// # Arguments
///
/// * `db` - A reference to a `Connection` struct
///
/// # Returns
///
/// A `Result` containing a `Vec` of `String` representing the param tree of the chromatogram table.
/// Returns an `Err` variant if there is an error executing the SQL query.
pub fn get_param_tree_chromatogram_res(db: &Connection) -> Result<Vec<String>> {
    get_strings(
        &db,
        "SELECT param_tree FROM chromatogram",
    )
}

/// Get the param tree of the spectrum table from one spectrum id
///
/// # Arguments
///
/// * `db` - A reference to a `Connection` struct
/// * `spectrum_id` - An `i64` representing the spectrum id
///
/// # Returns
///
/// A `Result` containing an `Option` of a `String` representing the param tree of the spectrum table.
/// Returns an `Err` variant if there is an error executing the SQL query.
pub fn get_param_tree_spectrum(db: &Connection, spectrum_id: i64) -> Result<Option<String>> {
    get_first_string(
        &db,
        format!("SELECT param_tree FROM spectrum WHERE id = {}", spectrum_id).as_str(),
    )
}

/// Get param tree of the mzdb table
///
/// # Arguments
///
/// * `db` - A reference to a `Connection` struct
///
/// # Returns
///
/// A `Result` containing an `Option` of a `String` representing the param tree of the mzdb table.
/// Returns an `Err` variant if there is an error executing the SQL query.
pub fn get_param_tree_mzdb(db: &Connection) -> Result<Option<String>> {
    _get_first_string_from_query(
        &db,
        "SELECT param_tree FROM mzdb LIMIT 1",
    )
}

/// Get the last cycle of spectrum
pub fn get_last_cycle_number(db: &Connection) -> Result<Option<i64>> {
    get_first_int(
        &db,
        "SELECT cycle FROM spectrum ORDER BY id DESC LIMIT 1",
    )
}

/// Get the last cycle of spectrum
pub fn get_last_time(db: &Connection) -> Result<Option<f32>> {
    get_first_real_from_query(
        &db,
        "SELECT time FROM spectrum ORDER BY id DESC LIMIT 1",
    )
}

/// Get max ms level of run slice
pub fn get_max_ms_level(db: &Connection) -> Result<Option<i64>> {
    get_first_int(
        &db,
        "SELECT max(ms_level) FROM run_slice",
    )
}

/// The number of bounding box from one run slice id
pub fn get_run_slice_bounding_boxes_count(db: &Connection, run_slice_id: i64) -> Result<Option<i64>> {
    get_first_int(
        &db,
        format!("SELECT count(*) FROM bounding_box WHERE bounding_box.run_slice_id = {} ", run_slice_id).as_str(),
    )
}

/// The number of spectrum from one ms_level
pub fn get_spectra_count_single_ms_level(db: &Connection, ms_level: i64) -> Result<Option<i64>> {
    get_first_int(
        &db,
        format!("SELECT count(id) FROM spectrum WHERE ms_level = {}", ms_level).as_str(),
    )
}

/// Get the number of records stored in a given table
pub fn get_table_records_count(db: &Connection, name: &str) -> Result<Option<i64>> {
    get_first_int(
        &db,
        format!("SELECT seq FROM sqlite_sequence WHERE name =  {:?}", name).as_str(),
    )
}

/// Get the id of the first bounding box for one spectrum
pub fn get_bounding_box_first_spectrum_id(db: &Connection, first_id: i64) -> Result<Option<i64>> {
    get_first_int(
        &db,
        format!("SELECT bb_first_spectrum_id FROM spectrum WHERE id = {}", first_id).as_str(),
    )
}

/// Get the minimum m/z of a given bounding box
pub fn get_bounding_box_min_mz(db: &Connection, bb_r_tree_id: i64) -> Result<Option<f32>> {
    get_first_real_from_query(
        &db,
        format!("SELECT min_mz FROM bounding_box_rtree WHERE bounding_box_rtree.id = {}", bb_r_tree_id).as_str(),
    )
}

/// Get the minimum time of a given bounding box
pub fn get_bounding_box_min_time(db: &Connection, bb_r_tree_id: i64) -> Result<Option<f64>> {
    get_first_f64(
        &db,
        format!("SELECT min_time FROM bounding_box_rtree WHERE bounding_box_rtree.id = {}", bb_r_tree_id).as_str(),
    )
}

/// Get the run slice id of one bounding box
pub fn get_run_slice_id(db: &Connection, bb_id: i64) -> Result<Option<i64>> {
    get_first_int(
        &db,
        format!("SELECT run_slice_id FROM bounding_box WHERE id = {}", bb_id).as_str(),
    )
}

/// Get the ms level of one run slice
pub fn get_ms_level_from_run_slice_id(db: &Connection, run_slice_id: i64) -> Result<Option<i64>> {
    get_first_int(
        &db,
        format!("SELECT ms_level FROM run_slice WHERE run_slice.id = {}", run_slice_id).as_str(),
    )
}

/// Get the run_slice id of one bounding box for get the ms level of this run slice
pub fn get_bounding_box_ms_level(db: &Connection, bb_id: i64) -> Result<Option<i64>> {
    let run_slice_id = get_first_int_no_option(
        &db,
        format!("SELECT run_slice_id FROM bounding_box WHERE id = {}", bb_id).as_str(),
    ).location(here!())?;
    get_first_int(
        &db,
        format!("SELECT ms_level FROM run_slice WHERE run_slice.id = {:?}", run_slice_id).as_str(),
    )
}

/// Get the data encoding for one bounding and one spectrum
pub fn get_data_encoding_id(db: &Connection, bb_id: i64) -> Result<Option<i64>> {
    get_first_int(
        &db,
        format!("SELECT s.data_encoding_id FROM spectrum s, bounding_box b WHERE b.id = {} AND b.first_spectrum_id = s.id", bb_id).as_str(),
    )
}

/// Get the count of id for all the data encoding
pub fn get_data_encoding_count(db: &Connection) -> Result<Option<i64>> {
    get_first_int(
        &db,
        "SELECT count(id) FROM data_encoding"
    )
}


pub fn list_data_encodings(db: &Connection) -> Result<Vec<DataEncoding>> {
    // Get all the data from data_encoding row by row
    // Use se data from the table data encoding for complete the struct of data encoding
    // Push the value in a vector
    let mut stmt = db.prepare("SELECT * FROM data_encoding").location(here!())?;

    let values = stmt.query_map(
        [],
        |row| {
            let mode_as_str: String = row.get(1)?;
            let byte_order_as_str: String = row.get(3)?;
            let mz_precision: u32 = row.get(4)?;
            let intensity_precision: u32 = row.get(5)?;

            let mode = if mode_as_str == "fitted" {
                DataMode::FITTED
            } else if mode_as_str == "centroid" {
                DataMode::CENTROID
            } else {
                DataMode::PROFILE
            };

            let byte_order = if byte_order_as_str == "little_endian" {
                ByteOrder::LITTLE_ENDIAN
            } else {
                ByteOrder::BIG_ENDIAN
            };

            let peak_encoding = if mz_precision == 32 {
                PeakEncoding::LOW_RES_PEAK
            } else if intensity_precision == 32 {
                PeakEncoding::HIGH_RES_PEAK
            } else {
                PeakEncoding::NO_LOSS_PEAK
            };

            RusqliteResult::Ok(DataEncoding {
                id: row.get(0)?,
                mode: mode,
                peak_encoding: peak_encoding,
                compression: row.get(2)?,
                byte_order: byte_order,
            })
        }).location(here!())?;

    let mut result = Vec::new();
    for value in values {
        result.push(value.location(here!())?);
    }

    Ok(result)
}

pub fn list_get_spectra_data_encoding_ids(db: &Connection) -> Result<HashMap<i64, i64>> {
    let mut stmt = db.prepare("SELECT id, data_encoding_id FROM spectrum").location(here!())?;
    let mut rows = stmt.query([]).location(here!())?;

    let mut mapping = HashMap::new();
    while let Some(row) = rows.next().location(here!())? {
        let id: i64 = row.get(0).location(here!())?;
        let data_encoding_id: i64 = row.get(1).location(here!())?;
        mapping.insert(id, data_encoding_id);
    }

    Ok(mapping)
}

/*
use std::io::{Read, Seek, SeekFrom};

pub fn get_bounding_box_data(db: &Connection, bb_id: i64) -> Result<Vec<u8>> {
    let mut blob_handle = db.blob_open(
        DatabaseName::Main,
        "bounding_box",
        "data",
        bb_id,
        true,
    ).location(here!())?;

    blob_handle.seek(SeekFrom::Start(0)).location(here!())?;

    let n_bytes = blob_handle.len();

    let mut bytes_vec: Vec<u8> = vec![0; n_bytes];
    let bytes_as_slice = &mut bytes_vec[..];
    let bytes_read = blob_handle.read(bytes_as_slice).location(here!())?;

    assert_eq!(bytes_read, n_bytes); // check n_bytes were read

    blob_handle.close().location(here!())?;

    Ok(bytes_vec)
}*/

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
    //let peaks_bytes_length= peaks_count * peak_size;

    let mut float_bytes = [0u8; 4];
    let mut double_bytes = [0u8; 8];

    // Closure for bytes to double conversion
    let mut _bytes_to_double = |offset: usize, decode_float: bool| -> (f64, usize) {
        if decode_float {
            float_bytes.clone_from_slice(&bb_bytes[offset..offset + 4]);

            let value = if byte_order == ByteOrder::BIG_ENDIAN {
                f32::from_be_bytes(float_bytes) as f64
            } else {
                f32::from_le_bytes(float_bytes) as f64
            };
            (value, 4)
        } else {
            double_bytes.clone_from_slice(&bb_bytes[offset..offset + 8]);

            let value = if byte_order == ByteOrder::BIG_ENDIAN {
                f64::from_be_bytes(double_bytes)
            } else {
                f64::from_le_bytes(double_bytes)
            };
            (value, 8)
        }
    };

    let mut filtered_peaks_count = 0;
    let mut filtered_peaks_start_idx = 0;

    // If no m/z range is provided
    if min_mz.is_none() && max_mz.is_none() {
        // Compute the peaks count for the whole spectrum slice
        filtered_peaks_count = peaks_count;
        // Set peaks_start_idx value to spectrum_slice_start_pos
        filtered_peaks_start_idx = peaks_start_pos;
        // Else determine the peaks_start_idx and peaks_count corresponding to provided m/z filters
    } else {
        // Determine the max m/z threshold to use
        let max_mz_threshold = if max_mz.is_none() { f64::MAX } else { max_mz.unwrap() };

        let mut i = 0;
        while i < peaks_count {
            let peak_start_pos: usize = peaks_start_pos + 1;

            // TODO: compare with memcpy C implementation (see https://doc.rust-lang.org/std/ptr/fn.copy_nonoverlapping.html)
            let (mz, _offset) = _bytes_to_double(peak_start_pos, pe == PeakEncoding::LOW_RES_PEAK);

            // Check if we are in the desired m/z range
            if mz >= min_mz.unwrap() && mz <= max_mz_threshold {
                // Increment the number of peaks to read
                filtered_peaks_count += 1;
                // Determine the peaks start idx
                if mz >= min_mz.unwrap() && filtered_peaks_start_idx == 0 {
                    filtered_peaks_start_idx = peak_start_pos;
                }
            }
            i += 1;
        }
    }

    // Create new arrays of primitives
    let mut mz_array: Vec<f64> = Vec::with_capacity(filtered_peaks_count);
    let mut intensity_array: Vec<f32> = Vec::with_capacity(filtered_peaks_count);

    let mut lwhm_array: Vec<f32> = if data_mode == FITTED {
        Vec::with_capacity(filtered_peaks_count)
    } else {
        Vec::new()
    };

    let mut rwhm_array: Vec<f32> = if data_mode == FITTED {
        Vec::with_capacity(filtered_peaks_count)
    } else {
        Vec::new()
    };

    // Closure for bytes to float conversion
    let mut float_bytes2 = [0u8; 4];
    let mut double_bytes2 = [0u8; 8];

    let mut _bytes_to_float = |offset: usize, decode_float: bool| -> (f32, usize) {
        if decode_float {
            float_bytes2.clone_from_slice(&bb_bytes[offset..offset + 4]);

            let value = if byte_order == ByteOrder::BIG_ENDIAN {
                f32::from_be_bytes(float_bytes2)
            } else {
                f32::from_le_bytes(float_bytes2)
            };
            (value, 4)
        } else {
            double_bytes2.clone_from_slice(&bb_bytes[offset..offset + 8]);

            let value = if byte_order == ByteOrder::BIG_ENDIAN {
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
        let (mz, offset) = _bytes_to_double(peak_bytes_index, pe == PeakEncoding::LOW_RES_PEAK);
        mz_array.push(mz);

        let (intensity, _offset) = _bytes_to_float(peak_bytes_index + offset, pe != PeakEncoding::NO_LOSS_PEAK);
        intensity_array.push(intensity);

        // Read left and right HWHMs if needed
        if data_mode == FITTED {
            let mz_int_size = pe as usize;

            lwhm_array.push(_bytes_to_float(peak_bytes_index + mz_int_size, true).0);
            rwhm_array.push(_bytes_to_float(peak_bytes_index + mz_int_size + 4, true).0);
        }

        peak_idx += 1;
    }

    let sd = SpectrumData {
        data_encoding: de.clone(),
        peaks_count: peaks_count,
        mz_array: mz_array,
        intensity_array: intensity_array,
        lwhm_array: lwhm_array,
        rwhm_array: rwhm_array,
    };

    Ok(sd)
}

pub fn read_spectrum_slice_data_at(
    bounding_box: &BoundingBox,
    bbox_index: &BoundingBoxIndex,
    data_encoding: &DataEncoding,
    spectrum_slice_idx: usize,
    min_mz: Option<f64>,
    max_mz: Option<f64>,
) -> Result<SpectrumData> {

    // Retrieve the number of peaks
    let peaks_count = bbox_index.peaks_counts[spectrum_slice_idx];

    // Skip spectrum id and peaks count (two integers)
    let peaks_start_pos = bbox_index.slices_indexes[spectrum_slice_idx] + 8;

    // Instantiate a new SpectrumData for the corresponding spectrum slice
    read_spectrum_slice_data(&bounding_box.blob_data, peaks_start_pos, peaks_count, data_encoding, min_mz, max_mz)
}

// TODO: should be only public for the iterator mod
pub fn create_bbox(row: &Row) -> Result<BoundingBox> {
    let bb_id: i64 = row.get(0).location(here!())?;
    let blob_data = row.get_ref(1).location(here!())?.as_blob().location(here!())?;
    //let blob =  row.v(1).location(here!())?;
    let run_slice_id: i64 = row.get(2).location(here!())?;
    let first_spectrum_id: i64 = row.get(3).location(here!())?;
    let last_spectrum_id: i64 = row.get(4).location(here!())?;

    let cur_bb = BoundingBox {
        id: bb_id,
        blob_data: blob_data.to_vec(),
        run_slice_id: run_slice_id,
        first_spectrum_id: first_spectrum_id,
        last_spectrum_id: last_spectrum_id,
    };

    Ok(cur_bb)
}

/// Index a bounding box
/// For have the number of spectrum slices, the list of spectra ids, the list of spectrum slice starting positions,
/// The number of peaks in each spectrum slice, from the blob in one bbox
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
    while bytes_idx < n_bytes { // for each spectrum slice store in the blob

        slices_indexes.push(bytes_idx); // store the last byte index

        int_as_bytes.clone_from_slice(&blob_data[bytes_idx..=bytes_idx + 3]);
        let spectrum_id = _bytes_to_int(&int_as_bytes) as i64;
        spectra_ids.push(spectrum_id);

        int_as_bytes.clone_from_slice(&blob_data[bytes_idx + 4..=bytes_idx + 7]);
        let peak_count = _bytes_to_int(&int_as_bytes) as usize;
        peaks_counts.push(peak_count);

        let de = cache.get_data_encoding_by_spectrum_id(&spectrum_id).ok_or(anyhow!("can't find data encoding")).location(here!())?;

        let peak_size = de.get_peak_size();

        slices_count += 1;
        bytes_idx = bytes_idx + 8 + (peak_size * peak_count);
    }

    let indexed_bbox = BoundingBoxIndex {
        bb_id: bbox.id,
        spectrum_slices_count: slices_count,
        spectra_ids: spectra_ids,
        slices_indexes,
        peaks_counts: peaks_counts,
    };

    Ok(indexed_bbox)
}

pub fn merge_spectrum_slices(sd_slices: &mut Vec<SpectrumData>, peaks_count: usize) -> Result<SpectrumData> {
    let data_encoding = sd_slices.first()
        .map(|sd| sd.data_encoding.clone())
        .context("sd_slices is empty").location(here!())?;

    let data_mode = data_encoding.mode;

    // Create new vectors of primitives
    let mut mz_array: Vec<f64> = Vec::with_capacity(peaks_count);
    let mut intensity_array: Vec<f32> = Vec::with_capacity(peaks_count);

    let mut lwhm_array: Vec<f32> = if data_mode == FITTED {
        Vec::with_capacity(peaks_count)
    } else {
        Vec::new()
    };

    let mut rwhm_array: Vec<f32> = if data_mode == FITTED {
        Vec::with_capacity(peaks_count)
    } else {
        Vec::new()
    };

    // Merge vectors
    for sd_slice in sd_slices {
        mz_array.append(&mut sd_slice.mz_array);
        intensity_array.append(&mut sd_slice.intensity_array);

        if data_mode == FITTED {
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

fn _bytes_to_int(bytes: &[u8; 4]) -> i32 {
    unsafe {
        std::mem::transmute::<[u8; 4], i32>(*bytes)
    }
}

pub fn get_spectrum(db: &Connection, spectrum_id: i64, entity_cache: &EntityCache) -> Result<Spectrum> {
    let spectrum_header = entity_cache.spectrum_headers.get((spectrum_id - 1) as usize)
        .context(format!("can't retrieve spectrum with ID={}", spectrum_id)).location(here!())?;

    let bb_first_spec_id_opt = get_first_int(
        &db,
        format!("SELECT bb_first_spectrum_id FROM spectrum WHERE id = {}", spectrum_id).as_str(),
    ).location(here!())?;

    if bb_first_spec_id_opt.is_none() {
        bail!("can't get bb_first_spectrum_id for spectrum with ID = {}", spectrum_id);
    }

    let bb_first_spec_id = bb_first_spec_id_opt.unwrap();

    // Count the number of BBs to be loaded
    let bb_count_opt = get_first_int(
        db,
        format!("SELECT count(id) FROM bounding_box WHERE bounding_box.first_spectrum_id = {}", bb_first_spec_id).as_str(),
    ).location(here!())?;

    if bb_count_opt.is_none() {
        bail!("can't determine the number of bounding boxes for first_spectrum_id = {}", bb_first_spec_id);
    }

    let bb_count = bb_count_opt.unwrap();

    // Load BBs from the DB
    let mut stmt = db.prepare(
        format!("SELECT * FROM bounding_box WHERE bounding_box.first_spectrum_id = {}", bb_first_spec_id).as_str()
    ).location(here!())?;

    let de_cache = &entity_cache.data_encodings_cache;

    // Determine peak size in bytes
    let de_opt = de_cache.get_data_encoding_by_spectrum_id(&spectrum_id);
    if de_opt.is_none() {
        bail!("can't retrieve data encoding for spectrum ID={}", spectrum_id);
    }

    let data_encoding = de_opt.unwrap();

    // for each bounding box, will collect the data of the spectrum
    let mut target_slice_idx: Option<usize> = None;
    let mut sd_slices: Vec<SpectrumData> = Vec::with_capacity(bb_count as usize);

    //let mut cur_bb: Vec<BoundingBox> = Vec::new();
    // Select the information in bouding box for one spectrum id
    let mut rows = stmt.query([])?;
    while let Some(row) = rows.next().location(here!())? {
        // put the information of each bounding box in the struct of bounding box
        let cur_bb = create_bbox(row).location(here!())?;

        let bb_index = index_bbox(&cur_bb, de_cache).location(here!())?;

        if target_slice_idx == None {
            target_slice_idx = bb_index.spectra_ids
                .iter().enumerate()
                .find(|(_slice_idx, &cur_spec_id)| cur_spec_id == spectrum_id)
                .map(|(slice_idx, &_cur_spec_id)| slice_idx);
        }

        if target_slice_idx.is_none() {
            bail!("can't find slice index for spectrum with ID={} in bounding box with ID={}",spectrum_id, cur_bb.id);
        }

        let spectrum_slice_data = read_spectrum_slice_data_at(
            &cur_bb,
            &bb_index,
            data_encoding,
            target_slice_idx.unwrap(),
            None,
            None,
        ).location(here!())?;

        sd_slices.push(spectrum_slice_data);
    }

    let peaks_count = sd_slices.iter().map(|slice| slice.peaks_count).sum(); // .copied()
    let spectrum_data = merge_spectrum_slices(&mut sd_slices, peaks_count).location(here!())?;

    Ok(Spectrum {
        header: spectrum_header.clone(),
        data: spectrum_data,
    })
}

// Function to get MS spectrum slices (msLevel 1)
pub fn get_ms_spectrum_slices(
    connection: &Connection, // SQLite connection
    min_mz: f64,
    max_mz: f64,
    min_rt: f32,
    max_rt: f32,
    entity_cache: &EntityCache
) -> Result<Vec<SpectrumSlice>> {
    _get_spectrum_slices_in_ranges(
        connection,
        min_mz,
        max_mz,
        min_rt,
        max_rt,
        1, // msLevel 1 for MS spectra
        0.0, // parentMz is not used for MS level 1
        entity_cache
    )
}

// Function to get MS/MS spectrum slices (msLevel 2)
pub fn get_msn_spectrum_slices(
    connection: &Connection,
    parent_mz: f64,
    min_frag_mz: f64,
    max_frag_mz: f64,
    min_rt: f32,
    max_rt: f32,
    entity_cache: &EntityCache
) -> Result<Vec<SpectrumSlice>> {
    _get_spectrum_slices_in_ranges(
        connection,
        min_frag_mz,
        max_frag_mz,
        min_rt,
        max_rt,
        2, // msLevel 2 for MS/MS spectra
        parent_mz, // parentMz is used for MS level 2
        entity_cache
    )
}

/*
fn get_spectrum_slices_in_ranges(
    connection: &Connection,
    min_mz: f64,
    max_mz: f64,
    min_rt: f32,
    max_rt: f32,
    ms_level: i32,
    parent_mz: f64,
    entity_cache: &EntityCache
) -> Result<Vec<SpectrumSlice>> {
    let bb_sizes = entity_cache.bb_sizes;
    let rt_width = if ms_level == 1 {
        bb_sizes.bb_rt_width_ms1
    } else {
        bb_sizes.bb_rt_width_msn
    };
    let mz_height = if ms_level == 1 {
        bb_sizes.bb_mz_height_ms1
    } else {
        bb_sizes.bb_mz_height_msn
    };

    let min_mz = min_mz - mz_height;
    let max_mz = max_mz + mz_height;
    let min_rt = min_rt - rt_width;
    let max_rt = max_rt + rt_width;

    let sql_query = if ms_level == 1 {
        "SELECT * FROM bounding_box WHERE id IN \
         (SELECT id FROM bounding_box_rtree WHERE min_mz >= ?1 AND max_mz <= ?2 AND min_time >= ?3 AND max_time <= ?4) \
         ORDER BY first_spectrum_id"
    } else {
        "SELECT * FROM bounding_box WHERE id IN \
         (SELECT id FROM bounding_box_msn_rtree WHERE min_ms_level = ?5 AND max_ms_level = ?5 \
         AND min_parent_mz <= ?6 AND max_parent_mz >= ?6 \
         AND min_mz >= ?1 AND max_mz <= ?2 AND min_time >= ?3 AND max_time <= ?4) \
         ORDER BY first_spectrum_id"
    };

    let mut stmt = connection.prepare(sql_query)?;

    // Assuming SpectrumHeader is obtained through another function
    let spec_headers = &entity_cache.spectrum_headers;
   let spectrum_header_by_id: HashMap<_, _> = spec_headers
        .into_iter()
        .map(|header| (header.id, header))
        .collect();

    //let de_cache = &entity_cache.data_encodings_cache;

    let mut spectrum_data_list_by_id: BTreeMap<i64, Vec<SpectrumData>> = BTreeMap::new();
    let mut peaks_count_by_spectrum_id: HashMap<i64, i32> = HashMap::new();

    let mut rows = stmt.query(rusqlite::params![min_mz, max_mz, min_rt, max_rt, ms_level, parent_mz])?;

    while let Some(row) = rows.next()? {
        let bb = create_bbox(row)?;
        /*let bb_id: i32 = row.get(0)?;
        let data: Vec<u8> = row.get(1)?;
        let first_spectrum_id: i64 = row.get(2)?;
        let last_spectrum_id: i64 = row.get(3)?;

        let bb = BoundingBox {
            id: bb_id,
            data,
            first_spectrum_id,
            last_spectrum_id,
        };*/

        // TODO: bb_reader and actual data processing
        // Add logic to process each spectrum and store them in `spectrum_data_list_by_id`

        // Example:
        let spectrum_data = SpectrumData {
            /* populate */ data_encoding: DataEncoding {},
            peaks_count: 0,
            mz_array: vec![],
            intensity_array: vec![],
            lwhm_array: vec![],
            rwhm_array: vec![],
        };
        spectrum_data_list_by_id
            .entry(first_spectrum_id)
            .or_insert_with(Vec::new)
            .push(spectrum_data);
    }

    // Finalize SpectrumSlice array creation
    let mut final_spectrum_slices = Vec::new();

    for (spectrum_id, spectrum_data_list) in spectrum_data_list_by_id {
        let peaks_count = peaks_count_by_spectrum_id.get(&spectrum_id).cloned().unwrap_or(0);
        let final_spectrum_data = _merge_spectrum_data_list(&spectrum_data_list, peaks_count)?;

        let spectrum_slice = SpectrumSlice {
            spectrum: Spectrum {
                header: (*spectrum_header_by_id.get(&spectrum_id).unwrap()).clone(),
                data: final_spectrum_data,
            },
            run_slice_id: 0,
        };

        final_spectrum_slices.push(spectrum_slice);
    }

    Ok(final_spectrum_slices)
}*/

  // Important: SQlite R*Tree floating values are 32bits floats, thus we need to expand the search slightly
// Advice from SQLite developers (https://www.sqlite.org/rtree.html#roundoff_error):
// Applications should expand their contained-within query boxes slightly (by 0.000012%)
// by rounding down the lower coordinates and rounding up the top coordinates, in each dimension.
const SQLITE_RTREE_UB_CORR: f64 = 1.0 + 0.00000012;
const SQLITE_RTREE_LB_CORR: f64 = 1.0 - 0.00000012;


// TODO: implement BTree instead of HashMap
fn _get_spectrum_slices_in_ranges(
    connection: &Connection,
    min_mz: f64,
    max_mz: f64,
    min_rt: f32,
    max_rt: f32,
    ms_level: u8,
    parent_mz: f64,
    entity_cache: &EntityCache
) -> Result<Vec<SpectrumSlice>> {
    let bb_sizes = entity_cache.bb_sizes;
    let (rt_width, mz_height) = if ms_level == 1 {
        (bb_sizes.bb_rt_width_ms1, bb_sizes.bb_mz_height_ms1)
    } else {
        (bb_sizes.bb_rt_width_msn, bb_sizes.bb_mz_height_msn)
    };

    // Update min/max BB coordinates according to actual BB sizes
    let bb_min_mz = (min_mz - mz_height) * SQLITE_RTREE_LB_CORR;
    let bb_max_mz = (max_mz + mz_height) * SQLITE_RTREE_UB_CORR;
    let bb_min_rt = (min_rt - rt_width) * SQLITE_RTREE_LB_CORR as f32;
    let bb_max_rt = (max_rt + rt_width) * SQLITE_RTREE_UB_CORR as f32;

    let sql_query = if ms_level == 1 {
        "
            SELECT * FROM bounding_box WHERE id IN (
                SELECT id FROM bounding_box_rtree
                WHERE min_mz >= ? AND max_mz <= ?
                AND min_time >= ? AND max_time <= ?
            )
            ORDER BY first_spectrum_id;
        "
    } else {
        "
            SELECT * FROM bounding_box WHERE id IN (
                SELECT id FROM bounding_box_msn_rtree
                WHERE min_ms_level = ? AND max_ms_level = ?
                AND min_parent_mz <= ? AND max_parent_mz >= ?
                AND min_mz >= ? AND max_mz <= ?
                AND min_time >= ? AND max_time <= ?
            )
            ORDER BY first_spectrum_id;
        "
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
        .into_iter()
        .map(|header| (header.id, header))
        .collect();

    let de_cache = &entity_cache.data_encodings_cache;

    let mut spectrum_data_list_by_id: HashMap<i64, Vec<SpectrumData>> = HashMap::new();
    let mut peaks_count_by_spectrum_id: HashMap<i64, usize> = HashMap::new();

    // Process bounding box records
    while let Some(bb_record) = rows.next()? {
        /*let bb_id: i64 = record.get("id")?;
        let data: Vec<u8> = record.get("data")?;
        let first_spectrum_id: i64 = record.get("first_spectrum_id")?;
        let last_spectrum_id: i64 = record.get("last_spectrum_id")?;*/

        /*let bb = BoundingBox {
            id:  record.get("id")?,
            first_spectrum_id: record.get("first_spectrum_id")?,
            last_spectrum_id: record.get("last_spectrum_id")?,
            run_slice_id: record.get("run_slice_id")?,
            blob_data: record.get("data")?,
        };*/

        let cur_bb = create_bbox(bb_record)?;

        let de_opt = de_cache.get_data_encoding_by_spectrum_id(&cur_bb.first_spectrum_id);
        if de_opt.is_none() {
            bail!("can't retrieve data encoding for spectrum ID={}", cur_bb.first_spectrum_id);
        }
        let data_encoding = de_opt.unwrap();

        let bb_index = index_bbox(&cur_bb, de_cache).location(here!())?;

        //let bb_spectra_count = bb_index.spectrum_slices_count;
        let bb_spectrum_ids = &bb_index.spectra_ids;

        for (spectrum_idx, &spectrum_id) in bb_spectrum_ids.iter().enumerate() {
            if let Some(sh) = spectrum_header_by_id.get(&spectrum_id) {
                let current_rt = sh.time;
                if current_rt >= min_rt && current_rt <= max_rt {
                    let spectrum_slice_data = read_spectrum_slice_data_at(
                        &cur_bb, &bb_index, data_encoding, spectrum_idx, Some(min_mz), Some(max_mz)
                    )?;

                    if spectrum_slice_data.peaks_count != 0 {
                        *peaks_count_by_spectrum_id.entry(spectrum_id).or_default() += spectrum_slice_data.peaks_count;
                        spectrum_data_list_by_id.entry(spectrum_id).or_default().push(spectrum_slice_data);
                    }
                }
            }
        }
    }

    // Finalize spectrum slices
    /*let mut final_spectrum_slices = Vec::with_capacity(spectrum_data_list_by_id.len());
    for (spectrum_id, spectrum_data_list) in spectrum_data_list_by_id {
        let peaks_count = peaks_count_by_spectrum_id[&spectrum_id];
        let final_spectrum_data = SpectrumData::merge(&spectrum_data_list, peaks_count)?;
        if let Some(sh) = spectrum_header_by_id.get(&spectrum_id) {
            final_spectrum_slices.push(SpectrumSlice::new(sh.clone(), final_spectrum_data));
        }
    }*/

    let mut final_spectrum_slices = Vec::with_capacity(spectrum_data_list_by_id.len());
    for (spectrum_id, spectrum_data_list) in spectrum_data_list_by_id {
        let peaks_count = peaks_count_by_spectrum_id.get(&spectrum_id).cloned().unwrap_or(0);
        let final_spectrum_data = _merge_spectrum_data_list(spectrum_data_list, peaks_count)?;

        let spectrum_slice = SpectrumSlice {
            spectrum: Spectrum {
                header: (*spectrum_header_by_id.get(&spectrum_id).unwrap()).clone(),
                data: final_spectrum_data,
            },
            run_slice_id: 0,
        };

        final_spectrum_slices.push(spectrum_slice);
    }

    Ok(final_spectrum_slices)
}

fn _merge_spectrum_data_list(spectrum_data_list: Vec<SpectrumData>, peaks_count: usize) -> Result<SpectrumData> {
    if spectrum_data_list.is_empty() {
        bail!("Spectrum data list should not be empty")
    }

    let mut final_mz_array = Vec::with_capacity(peaks_count);
    let mut final_intensity_array = Vec::with_capacity(peaks_count);
    let mut final_lwhm_array: Option<Vec<f32>> = None;
    let mut final_rwhm_array: Option<Vec<f32>> = None;

    if let Some(first_spectrum_data) = spectrum_data_list.first() {
        if !first_spectrum_data.lwhm_array.is_empty() && !first_spectrum_data.rwhm_array.is_empty() {
            final_lwhm_array = Some(Vec::with_capacity(peaks_count));
            final_rwhm_array = Some(Vec::with_capacity(peaks_count));
        }
    }

    let data_encoding = spectrum_data_list[0].data_encoding.clone();
    for spectrum_data in spectrum_data_list {
        // Extend the primary arrays
        final_mz_array.extend(spectrum_data.mz_array);
        final_intensity_array.extend(spectrum_data.intensity_array);

        // Extend the optional arrays if they exist
        if let (Some(ref mut lwhm), Some(ref mut rwhm)) = (&mut final_lwhm_array, &mut final_rwhm_array) {
            lwhm.extend(spectrum_data.lwhm_array);
            rwhm.extend(spectrum_data.rwhm_array);
        }
    }

    Ok(SpectrumData {
        data_encoding: data_encoding,
        peaks_count,
        mz_array: final_mz_array,
        intensity_array: final_intensity_array,
        lwhm_array: final_lwhm_array.unwrap_or_default(),
        rwhm_array: final_rwhm_array.unwrap_or_default(),
    })
}

// Get MS XIC (ms level 1)
pub fn get_ms_xic(
    connection: &Connection,  // SQLite connection
    mz: f64,
    mz_tol_ppm: f64,
    min_rt: Option<f32>,
    max_rt: Option<f32>,
    method: XicMethod,
    entity_cache: &EntityCache
) -> Result<Vec<XicPeak>> {

    let mz_tol_da = mz * mz_tol_ppm / 1e6;

    let min_rt_for_rtree = min_rt.unwrap_or(0.0);
    let max_rt_for_rtree = if max_rt.is_some() { max_rt.unwrap() } else {
        get_last_time(connection)?.context("can't retrieve the last spectrum retention time information")?
    };

    // Assume `get_ms_spectrum_slices` returns spectrum slices
    let spectrum_slices = get_ms_spectrum_slices(
        connection,
        mz - mz_tol_da,
        mz + mz_tol_da,
        min_rt_for_rtree,
        max_rt_for_rtree,
        entity_cache
    )?;

    Ok(_spectrum_slices_to_xic(spectrum_slices, mz, mz_tol_ppm, method))
}

// Gt MS/MS XIC (ms level 2)
fn get_msn_xic(
    connection: &Connection,  // SQLite connection
    parent_mz: f64,
    fragment_mz: f64,
    fragment_mz_tol_ppm: f64,
    min_rt: Option<f32>,
    max_rt: Option<f32>,
    method: XicMethod,
    entity_cache: &EntityCache
) -> Result<Vec<XicPeak>> {

    let fragment_mz_tol_da = fragment_mz * fragment_mz_tol_ppm / 1e6;

    let min_rt_for_rtree = min_rt.unwrap_or(0.0);
    let max_rt_for_rtree = if max_rt.is_some() { max_rt.unwrap() } else {
        get_last_time(connection)?.context("can't retrieve the last spectrum retention time information")?
    };

    // Use the existing helper function `get_msn_spectrum_slices`
    let spectrum_slices = get_msn_spectrum_slices(
        connection,
        parent_mz,
        fragment_mz - fragment_mz_tol_da,
        fragment_mz + fragment_mz_tol_da,
        min_rt_for_rtree,
        max_rt_for_rtree,
        entity_cache
    )?;

    Ok(_spectrum_slices_to_xic(spectrum_slices, fragment_mz, fragment_mz_tol_ppm, method))
}

// Internal function to convert spectrum slices to XIC
fn _spectrum_slices_to_xic(
    spectrum_slices: Vec<SpectrumSlice>,  // Assuming SpectrumSlice type
    searched_mz: f64,
    mz_tol_ppm: f64,
    method: XicMethod,
) -> Vec<XicPeak> {
    if spectrum_slices.is_empty() {
        return Vec::new();  // Return empty vector if no spectrum slices
    }

    let mut xic_peaks = Vec::with_capacity(spectrum_slices.len());

    match method {
        XicMethod::MAX => {
            for sl in spectrum_slices {
                let spectrum_data = sl.spectrum.data;
                if spectrum_data.peaks_count > 0 {
                    let max_dp_opt = spectrum_data.intensity_array.iter().enumerate().max_by(|&a, &b| {
                        (*a.1).total_cmp(b.1)
                    });

                    if let Some((max_dp_idx, max_intensity)) = max_dp_opt {
                        xic_peaks.push(XicPeak {
                            mz: spectrum_data.mz_array[max_dp_idx],
                            intensity: *max_intensity,
                            rt: sl.spectrum.header.time
                        });
                    }
                }
            }
        },
        XicMethod::NEAREST => {
            for sl in spectrum_slices {
                let spectrum_data = sl.spectrum.data;
                if spectrum_data.peaks_count > 0 {
                    let rt = sl.spectrum.header.time;
                    if let Some(nearest_peak) = spectrum_data.get_nearest_peak(searched_mz, mz_tol_ppm, rt) {
                        xic_peaks.push(nearest_peak);  // Assuming `get_nearest_peak` returns an Option<Peak>
                    }
                }
            }
        },
    }

    xic_peaks
}