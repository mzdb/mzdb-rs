//! Common utilities for DIA file writing operations
//!
//! This module provides shared functionality used by both:
//! - `conversion::diafication` (DDA → DIA conversion)
//! - `processing::dia_simplifier` (DIA simplification)
//!
//! The utilities handle database setup, spectrum insertion, and R-tree management
//! for writing DIA spectra to mzDB files.

use anyhow_ext::{Context, Result};
use rusqlite::{params, Connection};

use super::get_or_create_centroid_data_encoding;

// ============================================================================
// DIA Write Context
// ============================================================================

/// Context for writing DIA spectra to an mzDB file
///
/// This struct holds the database IDs and configuration needed for inserting
/// new MS2 spectra into an existing mzDB file. It's initialized from an open
/// database connection and provides a consistent setup for DIA writing operations.
#[derive(Debug, Clone)]
pub struct DiaWriteContext {
    /// Next available spectrum ID (starts after max existing ID)
    pub next_spectrum_id: i64,
    /// Data encoding ID for centroid MS2 data
    pub data_encoding_id: i64,
    /// Run ID from the mzDB file
    pub run_id: i64,
    /// Instrument configuration ID (from MS1 spectra)
    pub instr_config_id: i64,
    /// Source file ID (from MS1 spectra)
    pub source_file_id: i64,
    /// Data processing ID (from MS1 spectra)
    pub data_proc_id: i64,
    /// Whether the MSn R-tree exists
    pub has_msn_rtree: bool,
}

impl DiaWriteContext {
    /// Initialize write context from an existing mzDB connection
    ///
    /// This queries the database to determine:
    /// - The next available spectrum ID
    /// - The data encoding ID for centroid data
    /// - The run ID
    /// - Reference metadata from MS1 spectra (instrument config, source file, data processing)
    /// - Whether the MSn R-tree table exists
    ///
    /// # Arguments
    /// * `conn` - Open database connection to the mzDB file
    ///
    /// # Returns
    /// A configured `DiaWriteContext` ready for spectrum insertion
    pub fn from_connection(conn: &Connection) -> Result<Self> {
        // Get the next spectrum ID (start after all existing spectra)
        let max_id: i64 = conn.query_row(
            "SELECT COALESCE(MAX(id), 0) FROM spectrum",
            [],
            |row| row.get(0),
        )?;
        let next_spectrum_id = max_id + 1;

        // Get or create data encoding for centroid MS2
        let data_encoding_id = get_or_create_centroid_data_encoding(conn)?;

        // Get run_id
        let run_id: i64 = conn.query_row(
            "SELECT id FROM run LIMIT 1",
            [],
            |row| row.get(0),
        )?;

        // Get reference metadata from MS1 spectra
        let (instr_config_id, source_file_id, data_proc_id): (i64, i64, i64) = conn
            .query_row(
                "SELECT instrument_configuration_id, source_file_id, data_processing_id 
                 FROM spectrum WHERE ms_level = 1 LIMIT 1",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .unwrap_or((1, 1, 1));

        // Check if MSn R-tree exists
        let has_msn_rtree: bool = conn
            .query_row(
                "SELECT COUNT(*) > 0 FROM sqlite_master 
                 WHERE type = 'table' AND name = 'bounding_box_msn_rtree'",
                [],
                |row| row.get(0),
            )
            .unwrap_or(false);

        Ok(Self {
            next_spectrum_id,
            data_encoding_id,
            run_id,
            instr_config_id,
            source_file_id,
            data_proc_id,
            has_msn_rtree,
        })
    }

    /// Advance the spectrum ID counter by the given count
    pub fn advance_spectrum_id(&mut self, count: i64) {
        self.next_spectrum_id += count;
    }
}

// ============================================================================
// Spectrum Statistics Helpers
// ============================================================================

/// Calculate time bounds (min, max) from a collection of items
///
/// # Arguments
/// * `items` - Slice of items to calculate bounds from
/// * `get_time` - Function to extract the time value from each item
///
/// # Returns
/// Tuple of (min_time, max_time) as f32
pub fn calculate_time_bounds<T, F>(items: &[T], get_time: F) -> (f32, f32)
where
    F: Fn(&T) -> f32,
{
    let min_time = items
        .iter()
        .map(&get_time)
        .fold(f32::INFINITY, f32::min);
    let max_time = items
        .iter()
        .map(&get_time)
        .fold(f32::NEG_INFINITY, f32::max);
    (min_time, max_time)
}

/// Calculate m/z bounds (min, max) from a collection of items
///
/// # Arguments
/// * `items` - Slice of items to calculate bounds from
/// * `get_mz` - Function to extract the m/z value from each item
///
/// # Returns
/// Tuple of (min_mz, max_mz) as f64
pub fn calculate_mz_bounds<T, F>(items: &[T], get_mz: F) -> (f64, f64)
where
    F: Fn(&T) -> f64,
{
    let min_mz = items
        .iter()
        .map(&get_mz)
        .fold(f64::INFINITY, f64::min);
    let max_mz = items
        .iter()
        .map(&get_mz)
        .fold(f64::NEG_INFINITY, f64::max);
    (min_mz, max_mz)
}

/// Calculate m/z bounds from nested arrays (e.g., spectra with mz_array fields)
///
/// # Arguments
/// * `items` - Slice of items containing m/z arrays
/// * `get_mz_array` - Function to extract the m/z array from each item
///
/// # Returns
/// Tuple of (min_mz, max_mz) as f64
pub fn calculate_mz_bounds_from_arrays<'a, T, F>(items: &'a [T], get_mz_array: F) -> (f64, f64)
where
    F: Fn(&'a T) -> &'a [f64],
{
    let min_mz = items
        .iter()
        .flat_map(|s| get_mz_array(s).iter())
        .fold(f64::INFINITY, |a, &b| a.min(b));
    let max_mz = items
        .iter()
        .flat_map(|s| get_mz_array(s).iter())
        .fold(f64::NEG_INFINITY, |a, &b| a.max(b));
    (min_mz, max_mz)
}

/// Find base peak (highest intensity) from parallel m/z and intensity arrays
///
/// # Arguments
/// * `mz_array` - Slice of m/z values
/// * `intensity_array` - Slice of intensity values (same length as mz_array)
///
/// # Returns
/// Tuple of (base_peak_mz, base_peak_intensity), or (0.0, 0.0) if empty
pub fn find_base_peak(mz_array: &[f64], intensity_array: &[f32]) -> (f64, f32) {
    mz_array
        .iter()
        .zip(intensity_array.iter())
        .max_by(|a, b| a.1.partial_cmp(b.1).unwrap())
        .map(|(&mz, &int)| (mz, int))
        .unwrap_or((0.0, 0.0))
}

// ============================================================================
// SQL Constants
// ============================================================================

/// SQL columns for DIA spectrum insertion
pub const DIA_SPECTRUM_INSERT_SQL: &str = 
    "INSERT INTO spectrum (
        id, initial_id, title, cycle, time, ms_level, activation_type,
        tic, base_peak_mz, base_peak_intensity, main_precursor_mz,
        main_precursor_charge, data_points_count, param_tree,
        scan_list, precursor_list, product_list, 
        shared_param_tree_id, instrument_configuration_id, source_file_id,
        run_id, data_processing_id, data_encoding_id, bb_first_spectrum_id
    ) VALUES (
        ?1, ?2, ?3, ?4, ?5, 2, 'HCD',
        ?6, ?7, ?8, ?9,
        NULL, ?10, ?11,
        ?12, ?13, NULL,
        NULL, ?14, ?15,
        ?16, ?17, ?18, ?19
    )";

/// Parameters for DIA spectrum insertion
///
/// This struct holds all the values needed to insert a DIA spectrum.
/// Use with `DIA_SPECTRUM_INSERT_SQL`.
#[derive(Debug, Clone)]
pub struct DiaSpectrumParams {
    pub spectrum_id: i64,
    pub title: String,
    pub cycle: i32,
    pub time: f32,
    pub tic: f64,
    pub base_peak_mz: f64,
    pub base_peak_intensity: f32,
    pub precursor_mz: f64,
    pub data_points_count: i32,
    pub param_tree: String,
    pub scan_list: String,
    pub precursor_list: String,
    pub instr_config_id: i64,
    pub source_file_id: i64,
    pub run_id: i64,
    pub data_proc_id: i64,
    pub data_encoding_id: i64,
    pub bb_first_spectrum_id: i64,
}

impl DiaSpectrumParams {
    /// Insert this spectrum into the database
    pub fn insert(&self, conn: &Connection) -> Result<()> {
        conn.execute(
            DIA_SPECTRUM_INSERT_SQL,
            params![
                self.spectrum_id,
                self.spectrum_id, // initial_id = id for new spectra
                self.title,
                self.cycle,
                self.time,
                self.tic,
                self.base_peak_mz,
                self.base_peak_intensity,
                self.precursor_mz,
                self.data_points_count,
                self.param_tree,
                self.scan_list,
                self.precursor_list,
                self.instr_config_id,
                self.source_file_id,
                self.run_id,
                self.data_proc_id,
                self.data_encoding_id,
                self.bb_first_spectrum_id,
            ],
        )?;
        Ok(())
    }
}
