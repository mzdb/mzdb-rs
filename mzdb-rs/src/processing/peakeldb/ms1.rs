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
use super::common::chrono_lite_timestamp;

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
pub struct Ms1PeakelDbWriter {
    conn: Connection,
}

impl Ms1PeakelDbWriter {
    /// Create a new MS1 peakelDB file
    ///
    /// # Arguments
    /// * `path` - Path to the output file (will be overwritten if exists)
    pub fn create<P: AsRef<Path>>(path: P) -> Result<Self> {
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

        Ok(Self { conn })
    }

    /// Write peakels to the database
    ///
    /// # Arguments
    /// * `mzdb_filename` - Name of the source mzDB file
    /// * `is_dia` - Whether the source is a DIA experiment
    /// * `peakels` - Slice of peakels to write
    pub fn write_peakels(
        &self,
        mzdb_filename: &str,
        is_dia: bool,
        peakels: &[Peakel],
    ) -> Result<()> {
        // Insert peakeldb_file record
        let now = chrono_lite_timestamp();
        self.conn.execute(
            "INSERT INTO peakeldb_file (id, name, description, raw_file_name, is_dia_experiment, 
             creation_timestamp, modification_timestamp) VALUES (?, ?, ?, ?, ?, ?, ?)",
            params![1, mzdb_filename, "Generated by mzdb2peakeldb", mzdb_filename, is_dia, &now, &now],
        )?;

        // Insert lcms_map record
        self.conn.execute(
            "INSERT INTO lcms_map (id, ms_level, peakel_count, peakeldb_file_id) VALUES (?, ?, ?, ?)",
            params![1, 1, peakels.len() as i32, 1],
        )?;

        // Insert peakels
        self.conn.execute("BEGIN TRANSACTION", [])?;
        
        let mut peakel_stmt = self.conn.prepare(
            "INSERT INTO peakel (id, moz, elution_time, duration, gap_count, apex_intensity, area,
             amplitude, intensity_cv, left_hwhm_mean, left_hwhm_cv, right_hwhm_mean, right_hwhm_cv,
             is_interfering, peak_count, peaks, serialized_properties,
             first_spectrum_id, apex_spectrum_id, last_spectrum_id, map_id)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
        )?;

        let mut rtree_stmt = self.conn.prepare(
            "INSERT INTO peakel_rtree (id, min_mz, max_mz, min_time, max_time, min_intensity, max_intensity)
             VALUES (?, ?, ?, ?, ?, ?, ?)"
        )?;

        for (idx, peakel) in peakels.iter().enumerate() {
            let peakel_id = (idx + 1) as i64;
            
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

            let peaks_blob = super::PeakelSerializer::to_msgpack(peakel)?;
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

        self.conn.execute("COMMIT", [])?;
        
        Ok(())
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
            let data = super::PeakelSerializer::from_msgpack(&peaks_blob)?;

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