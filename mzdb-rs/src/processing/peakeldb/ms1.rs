//! MS1 PeakelDB - Legacy format for MS1 peakels
//!
//! This module provides utilities for creating and reading MS1 peakelDB files.
//! The MS1 format includes lcms_map and peakeldb_file tables for compatibility
//! with legacy peakelDB tools.
//!
//! # Schema Overview
//!
//! - `peakeldb_file`: File-level metadata
//! - `lcms_map`: Map metadata (ms_level, peakel_count)
//! - `peakel`: Peakel data with all summary fields and peaks blob
//! - `peakel_rtree`: R-tree spatial index for fast queries

use std::path::Path;

use anyhow_ext::{Context, Result};
use rusqlite::{params, Connection};

use crate::processing::{Peakel, HasPeakelData};
use super::common::{chrono_lite_timestamp, PeakelWriterStats, PeakelDbWriter};

// ============================================================================
// MS1 PeakelSerializer - Legacy 4-array MessagePack format
// ============================================================================

/// MS1 peakel serialization using the legacy 4-array MessagePack format.
///
/// # Format
/// MessagePack tuple of 4 arrays: `[spectrum_ids (i64), elution_times (f32), mz_values (f64), intensities (f32)]`
/// Compatible with Scala mzdb-processing MessagePack format.
struct Ms1PeakelSerializer;

impl Ms1PeakelSerializer {
    /// Serialize peakel data to MessagePack bytes (legacy 4-array format).
    fn to_msgpack<T: HasPeakelData>(peakel: &T) -> anyhow_ext::Result<Vec<u8>> {
        let data = (
            peakel.spectrum_ids(),
            peakel.elution_times(),
            peakel.mz_values(),
            peakel.intensity_values(),
        );
        rmp_serde::to_vec(&data)
            .map_err(|e| anyhow_ext::anyhow!("msgpack serialization error: {}", e))
    }

    /// Deserialize MessagePack bytes to a new Peakel (legacy 4-array format).
    fn from_msgpack(bytes: &[u8]) -> anyhow_ext::Result<Peakel> {
        let (spectrum_ids, elution_times, mz_values_f64, intensity_values):
            (Vec<i64>, Vec<f32>, Vec<f64>, Vec<f32>) =
            rmp_serde::from_slice(bytes)
                .map_err(|e| anyhow_ext::anyhow!("msgpack deserialization error: {}", e))?;

        // mz_values: f64 in peakelDB, f32 in memory
        let mz_values: Vec<f32> = mz_values_f64.into_iter().map(|mz| mz as f32).collect();

        Peakel::from_vectors(
            spectrum_ids,
            elution_times,
            mz_values,
            intensity_values,
            None,
            None,
            0,
        )
    }
}

// ============================================================================
// Schema
// ============================================================================

/// MS1 PeakelDB schema (legacy format)
pub struct Ms1PeakelDbSchema;

impl Ms1PeakelDbSchema {
    /// SQL schema for MS1 peakelDB
    ///
    /// This schema uses legacy field names for backward compatibility:
    /// - `moz` (not `mz`) for m/z values
    /// - `peak_count` (not `peaks_count`) for number of peaks
    /// - Includes `intensity_cv` and `is_interfering` fields
    pub const SCHEMA: &'static str = r#"
CREATE TABLE peakeldb_file (
    id INTEGER NOT NULL PRIMARY KEY,
    name TEXT NOT NULL,
    description TEXT,
    raw_file_name TEXT NOT NULL,
    is_dia_experiment BOOLEAN NOT NULL,
    creation_timestamp TEXT NOT NULL,
    modification_timestamp TEXT NOT NULL,
    serialized_properties TEXT
);

CREATE TABLE lcms_map (
    id INTEGER NOT NULL PRIMARY KEY,
    ms_level INTEGER NOT NULL,
    peakel_count INTEGER NOT NULL,
    serialized_properties TEXT,
    peakeldb_file_id INTEGER NOT NULL,
    FOREIGN KEY (peakeldb_file_id) REFERENCES peakeldb_file (id)
);

CREATE TABLE peakel (
    id INTEGER NOT NULL PRIMARY KEY,
    moz REAL NOT NULL,
    elution_time REAL NOT NULL,
    duration REAL NOT NULL,
    gap_count INTEGER NOT NULL,
    apex_intensity REAL NOT NULL,
    area REAL NOT NULL,
    amplitude REAL NOT NULL,
    intensity_cv REAL NOT NULL,
    left_hwhm_mean REAL,
    left_hwhm_cv REAL,
    right_hwhm_mean REAL,
    right_hwhm_cv REAL,
    is_interfering BOOLEAN NOT NULL,
    peak_count INTEGER NOT NULL,
    peaks BLOB NOT NULL,
    serialized_properties TEXT,
    first_spectrum_id INTEGER NOT NULL,
    apex_spectrum_id INTEGER NOT NULL,
    last_spectrum_id INTEGER NOT NULL,
    map_id INTEGER NOT NULL,
    FOREIGN KEY (map_id) REFERENCES lcms_map (id)
);

CREATE VIRTUAL TABLE peakel_rtree USING rtree(
    id,
    min_mz, max_mz,
    min_time, max_time,
    min_intensity, max_intensity
);

CREATE INDEX peakel_moz_idx ON peakel (moz);
CREATE INDEX peakel_elution_time_idx ON peakel (elution_time);
CREATE INDEX peakel_map_id_idx ON peakel (map_id);
"#;
}

// ============================================================================
// Data Structures
// ============================================================================

/// MS1 peakel record for database operations
#[derive(Debug, Clone)]
pub struct Ms1PeakelRecord {
    pub id: i64,
    pub moz: f64,
    pub elution_time: f32,
    pub duration: f32,
    pub gap_count: i32,
    pub apex_intensity: f32,
    pub area: f32,
    pub amplitude: f32,
    pub intensity_cv: f32,
    pub left_hwhm_mean: Option<f32>,
    pub left_hwhm_cv: Option<f32>,
    pub right_hwhm_mean: Option<f32>,
    pub right_hwhm_cv: Option<f32>,
    pub is_interfering: bool,
    pub peak_count: i32,
    pub first_spectrum_id: i64,
    pub apex_spectrum_id: i64,
    pub last_spectrum_id: i64,
}

// ============================================================================
// Writer
// ============================================================================

/// Writer for MS1 peakelDB files
///
/// Creates a peakelDB file and writes peakels either all at once via
/// [`write_all_peakels`](Self::write_all_peakels) or incrementally via
/// [`write_peakels_batch`](Self::write_peakels_batch).
///
/// The writer must be finalized by calling [`close`](Self::close) which
/// commits the transaction and updates the peakel count. If not called
/// explicitly, [`Drop`] will attempt to commit as a safety net to ensure
/// SQLite file consistency, but callers should prefer calling `close()`
/// to handle errors properly.
///
/// # Example (batch mode)
/// ```no_run
/// use mzdb::processing::peakeldb::{Ms1PeakelDbWriter, PeakelDbWriter};
///
/// let mut writer = Ms1PeakelDbWriter::create("output.peakeldb", "input.mzDB", false).unwrap();
/// // ... write batches via write_peakels_batch() ...
/// writer.close().unwrap();
/// ```
pub struct Ms1PeakelDbWriter {
    conn: Connection,
    next_peakel_id: i64,
    stats: PeakelWriterStats,
    closed: bool,
}

impl Ms1PeakelDbWriter {
    /// SQL for inserting a peakel row
    const PEAKEL_INSERT_SQL: &'static str =
        "INSERT INTO peakel (id, moz, elution_time, duration, gap_count, apex_intensity, area,
         amplitude, intensity_cv, left_hwhm_mean, left_hwhm_cv, right_hwhm_mean, right_hwhm_cv,
         is_interfering, peak_count, peaks, serialized_properties,
         first_spectrum_id, apex_spectrum_id, last_spectrum_id, map_id)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

    /// SQL for inserting an R-tree entry
    const RTREE_INSERT_SQL: &'static str =
        "INSERT INTO peakel_rtree (id, min_mz, max_mz, min_time, max_time, min_intensity, max_intensity)
         VALUES (?, ?, ?, ?, ?, ?, ?)";
    
    /// Create a new MS1 peakelDB file
    ///
    /// Creates the database, inserts metadata, and begins a transaction
    /// for peakel insertion. The transaction is committed when [`close`](Self::close)
    /// is called or on [`Drop`].
    ///
    /// # Arguments
    /// * `path` - Path to the output file (will be overwritten if exists)
    /// * `mzdb_filename` - Name of the source mzDB file
    /// * `is_dia` - Whether the source is a DIA experiment
    pub fn create<P: AsRef<Path>>(path: P, mzdb_filename: &str, is_dia: bool) -> Result<Self> {
        // Remove existing file if present
        if path.as_ref().exists() {
            std::fs::remove_file(path.as_ref())?;
        }

        let conn = Connection::open(path.as_ref())
            .context("Failed to create peakelDB file")?;

        // SQLite optimizations
        conn.execute_batch(
            "PRAGMA synchronous=OFF;
             PRAGMA journal_mode=OFF;
             PRAGMA temp_store=2;
             PRAGMA cache_size=100000;"
        )?;

        // Create schema
        conn.execute_batch(Ms1PeakelDbSchema::SCHEMA)?;

        // Insert peakeldb_file record
        let now = chrono_lite_timestamp();
        conn.execute(
            "INSERT INTO peakeldb_file (id, name, description, raw_file_name, is_dia_experiment, 
             creation_timestamp, modification_timestamp) VALUES (?, ?, ?, ?, ?, ?, ?)",
            params![1, mzdb_filename, "Generated by mzdb2peakeldb", mzdb_filename, is_dia, &now, &now],
        )?;

        // Insert lcms_map record with placeholder peakel_count (updated on close)
        conn.execute(
            "INSERT INTO lcms_map (id, ms_level, peakel_count, peakeldb_file_id) VALUES (?, ?, ?, ?)",
            params![1, 1, 0, 1],
        )?;

        // Begin transaction for peakel insertion
        conn.execute("BEGIN TRANSACTION", [])?;

        Ok(Self {
            conn,
            next_peakel_id: 1,
            stats: PeakelWriterStats::new(),
            closed: false,
        })
    }

    /// Write a batch of peakels to the database (internal implementation)
    ///
    /// Can be called multiple times to write peakels incrementally.
    /// Peakel IDs are assigned sequentially across batches.
    fn write_peakels_batch_impl(&mut self, peakels: &[Peakel]) -> Result<()> {
        if peakels.is_empty() {
            return Ok(());
        }

        let mut peakel_stmt = self.conn.prepare_cached(Self::PEAKEL_INSERT_SQL)?;
        let mut rtree_stmt = self.conn.prepare_cached(Self::RTREE_INSERT_SQL)?;

        for peakel in peakels {
            let peakel_id = self.next_peakel_id;
            self.next_peakel_id += 1;
            self.stats.add_peakel(peakel);

            let mz = peakel.apex_mz().unwrap_or(f32::NAN);
            let elution_time = peakel.apex_elution_time().unwrap_or(0.0);
            let duration = peakel.calc_duration();
            let apex_intensity = peakel.apex_intensity().unwrap_or(0.0);
            let area = peakel.calc_area();
            let peak_count = peakel.peaks_count() as i32;
            
            let min_mz = peakel.min_mz();
            let max_mz = peakel.max_mz();
            let min_time = peakel.min_time();
            let max_time = peakel.max_time();
            let min_intensity = peakel.calc_min_intensity();

            let peaks_blob = Ms1PeakelSerializer::to_msgpack(peakel)?;
            let left_hwhm_mean = peakel.left_hwhm_mean();
            let right_hwhm_mean = peakel.right_hwhm_mean();
            let first_spectrum_id = peakel.first_spectrum_id().unwrap_or(0);
            let apex_spectrum_id = peakel.apex_spectrum_id().unwrap_or(0);
            let last_spectrum_id = peakel.last_spectrum_id().unwrap_or(0);
            let amplitude = if min_intensity == 0.0 {0.0} else {apex_intensity / min_intensity};

            // Use Option for nullable HWHM values (null if not computed)
            let left_hwhm_opt: Option<f32> = if left_hwhm_mean > 0.0 { Some(left_hwhm_mean) } else { None };
            let right_hwhm_opt: Option<f32> = if right_hwhm_mean > 0.0 { Some(right_hwhm_mean) } else { None };
            
            // Calculate intensity coefficient of variation
            let intensity_cv = peakel.calc_intensity_cv();

            peakel_stmt.execute(params![
                peakel_id,
                mz,
                elution_time,
                duration,
                peakel.gap_count,
                apex_intensity,
                area,
                amplitude,
                intensity_cv,
                left_hwhm_opt,
                Option::<f32>::None, // FIXME: calculate left_hwhm_cv
                right_hwhm_opt,
                Option::<f32>::None, // FIXME: calculate right_hwhm_cv
                false, // is_interfering
                peak_count,
                peaks_blob,
                Option::<String>::None, // serialized_properties
                first_spectrum_id,
                apex_spectrum_id,
                last_spectrum_id,
                1, // map_id
            ])?;

            rtree_stmt.execute(params![
                peakel_id,
                min_mz,
                max_mz,
                min_time,
                max_time,
                min_intensity as f64,
                apex_intensity as f64,
            ])?;
        }

        Ok(())
    }

    /// Finalize the database: commit the transaction and update peakel count.
    ///
    /// This should be called after all peakels have been written. If not called
    /// explicitly, [`Drop`] will attempt to commit, but errors will only be logged.
    fn close_impl(&mut self) -> Result<()> {
        if self.closed {
            return Ok(());
        }

        self.closed = true;

        let peakel_count = self.finalize_peakeldb()?;

        log::info!("MS1 peakelDB closed: {} peakels written", peakel_count);

        Ok(())
    }
}

impl PeakelDbWriter for Ms1PeakelDbWriter {
    type Record = Peakel;

    fn connection(&mut self) -> &Connection {
        &self.conn
    }

    fn write_peakels_batch(&mut self, peakels: &[Peakel]) -> Result<()> {
        self.write_peakels_batch_impl(peakels)
    }

    fn close(&mut self) -> Result<()> {
        self.close_impl()
    }

    fn stats(&self) -> &PeakelWriterStats {
        &self.stats
    }
}

impl Drop for Ms1PeakelDbWriter {
    fn drop(&mut self) {
        if !self.closed {
            if let Err(e) = self.close_impl() {
                log::error!("Error closing MS1 peakelDB writer on drop: {:?}", e);
            }
        }
    }
}

// ============================================================================
// Reader
// ============================================================================

/// Reader for MS1 peakeldb SQLite files
pub struct Ms1PeakelDbReader {
    conn: Connection,
}

impl Ms1PeakelDbReader {
    /// Open a peakeldb file
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let conn = Connection::open(path.as_ref())
            .context("Failed to open peakeldb file")?;
        Ok(Self { conn })
    }

    /// Get a reference to the underlying connection
    pub fn connection(&self) -> &Connection {
        &self.conn
    }

    /// Get the number of peakels in the database
    pub fn get_peakel_count(&self) -> Result<i64> {
        self.conn
            .query_row("SELECT COUNT(*) FROM peakel", [], |row| row.get(0))
            .context("Failed to count peakels")
    }

    /// Read all peakels from the database as ExtendedPeakel
    pub fn read_all_peakels(&self) -> Result<Vec<super::ExtendedPeakel>> {
        let mut stmt = self.conn.prepare(
            "SELECT id, moz, elution_time, duration, gap_count, apex_intensity,
                    area, amplitude, peak_count, peaks,
                    first_spectrum_id, apex_spectrum_id, last_spectrum_id
             FROM peakel
             ORDER BY id",
        )?;

        let peakel_iter = stmt.query_map([], |row| {
            let peaks_blob: Vec<u8> = row.get(9)?;
            Ok((
                row.get::<_, i64>(0)?,    // id
                row.get::<_, f64>(1)? as f32,    // moz -> mz (f64 in DB, f32 in memory)
                row.get::<_, f32>(2)?,    // elution_time
                row.get::<_, f32>(3)?,    // duration
                row.get::<_, i32>(4)?,    // gap_count
                row.get::<_, f32>(5)?,    // apex_intensity
                row.get::<_, f32>(6)?,    // area
                row.get::<_, f32>(7)?,    // amplitude
                row.get::<_, i32>(8)?,    // peak_count
                peaks_blob,               // peaks (MessagePack blob)
                row.get::<_, i64>(10)?,   // first_spectrum_id
                row.get::<_, i64>(11)?,   // apex_spectrum_id
                row.get::<_, i64>(12)?,   // last_spectrum_id
            ))
        })?;

        let mut peakels = Vec::new();

        for result in peakel_iter {
            let (
                id,
                mz,
                elution_time,
                duration,
                gap_count,
                apex_intensity,
                area,
                amplitude,
                peaks_count,
                peaks_blob,
                first_spectrum_id,
                apex_spectrum_id,
                last_spectrum_id,
            ) = result?;

            // Parse the MessagePack peaks blob
            let data = Ms1PeakelSerializer::from_msgpack(&peaks_blob)?;

            peakels.push(super::ExtendedPeakel::new(
                id,
                mz,
                elution_time,
                duration,
                gap_count,
                apex_intensity,
                area,
                amplitude,
                peaks_count,
                first_spectrum_id,
                apex_spectrum_id,
                last_spectrum_id,
                data,
            ));
        }

        log::info!("Loaded {} peakels from MS1 peakeldb", peakels.len());
        Ok(peakels)
    }
}