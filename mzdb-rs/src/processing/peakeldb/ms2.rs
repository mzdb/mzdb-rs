//! MS2 DIA PeakelDB - Format for DIA MS2 peakels with isolation windows
//!
//! This module provides utilities for creating and reading MS2 DIA peakelDB files.
//! The schema is aligned with the MS1 peakelDB format for consistency, with additional
//! tables for DIA-specific data (isolation windows).
//!
//! # Schema Overview
//!
//! - `peakeldb_file`: File-level metadata (same as MS1)
//! - `lcms_map`: Map metadata with ms_level=2 (same as MS1)
//! - `isolation_window`: DIA-specific isolation window definitions
//! - `peakel`: Peakel data with isolation_window_id and precursor_mz
//! - `peakel_rtree`: R-tree spatial index for fast queries

use std::path::Path;

use anyhow_ext::{Context, Result};
use rusqlite::{params, Connection};

use crate::processing::dia::{IsolationWindow, DiaMs2PeakelRecord};
use super::common::{chrono_lite_timestamp, ExtendedPeakel, PeakelData};

// ============================================================================
// Schema
// ============================================================================

/// MS2 DIA PeakelDB schema (aligned with MS1 schema)
pub struct Ms2PeakelDbSchema;

impl Ms2PeakelDbSchema {
    /// SQL schema for MS2 DIA peakelDB
    ///
    /// This schema is aligned with the MS1 peakelDB schema for consistency:
    /// - Same table names: peakeldb_file, lcms_map, peakel, peakel_rtree
    /// - Same base field names: moz (not mz), peak_count (not peaks_count)
    /// - Additional: isolation_window table and related foreign keys
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

CREATE TABLE isolation_window (
    id INTEGER PRIMARY KEY,
    target_mz REAL NOT NULL,
    lower_mz REAL NOT NULL,
    upper_mz REAL NOT NULL,
    spectrum_count INTEGER NOT NULL,
    map_id INTEGER NOT NULL,
    FOREIGN KEY (map_id) REFERENCES lcms_map (id)
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
    peak_count INTEGER NOT NULL,
    peaks BLOB NOT NULL,
    serialized_properties TEXT,
    first_spectrum_id INTEGER NOT NULL,
    apex_spectrum_id INTEGER NOT NULL,
    last_spectrum_id INTEGER NOT NULL,
    isolation_window_id INTEGER NOT NULL,
    precursor_mz REAL NOT NULL,
    map_id INTEGER NOT NULL,
    FOREIGN KEY (isolation_window_id) REFERENCES isolation_window (id),
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
CREATE INDEX peakel_isolation_window_idx ON peakel (isolation_window_id);
CREATE INDEX peakel_precursor_mz_idx ON peakel (precursor_mz);
CREATE INDEX peakel_map_id_idx ON peakel (map_id);
"#;
}

// ============================================================================
// Reader
// ============================================================================

/// Reader for MS2 DIA peakeldb SQLite files
pub struct Ms2PeakelDbReader {
    conn: Connection,
}

impl Ms2PeakelDbReader {
    /// Open a peakeldb file
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let conn = Connection::open(path.as_ref())
            .context("Failed to open peakeldb file")?;
        Ok(Self { conn })
    }

    /// Read all isolation windows from the database
    pub fn read_isolation_windows(&self) -> Result<Vec<IsolationWindow>> {
        let mut stmt = self.conn.prepare(
            "SELECT id, target_mz, lower_mz, upper_mz, spectrum_count
             FROM isolation_window
             ORDER BY id",
        )?;

        let windows = stmt.query_map([], |row| {
            Ok(IsolationWindow {
                id: row.get(0)?,
                target_mz: row.get(1)?,
                lower_mz: row.get(2)?,
                upper_mz: row.get(3)?,
                spectrum_count: row.get::<_, i64>(4)? as usize,
            })
        })?;

        windows
            .collect::<Result<Vec<_>, _>>()
            .context("Failed to read isolation windows")
    }

    /// Read all peakels from the database
    pub fn read_all_peakels(&self) -> Result<Vec<ExtendedPeakel>> {
        let mut stmt = self.conn.prepare(
            "SELECT id, moz, elution_time, duration, gap_count, apex_intensity,
                    area, amplitude, peak_count, peaks, serialized_properties,
                    first_spectrum_id, apex_spectrum_id, last_spectrum_id,
                    isolation_window_id, precursor_mz
             FROM peakel
             ORDER BY id",
        )?;

        let peakel_iter = stmt.query_map([], |row| {
            let peaks_blob: Vec<u8> = row.get(9)?;
            Ok((
                row.get::<_, i64>(0)?,    // id
                row.get::<_, f64>(1)?,    // moz -> mz
                row.get::<_, f32>(2)?,    // elution_time
                row.get::<_, f32>(3)?,    // duration
                row.get::<_, i32>(4)?,    // gap_count
                row.get::<_, f32>(5)?,    // apex_intensity
                row.get::<_, f32>(6)?,    // area
                row.get::<_, f32>(7)?,    // amplitude
                row.get::<_, i32>(8)?,    // peak_count -> peaks_count
                peaks_blob,               // peaks (index 9)
                row.get::<_, i64>(11)?,   // first_spectrum_id
                row.get::<_, i64>(12)?,   // apex_spectrum_id
                row.get::<_, i64>(13)?,   // last_spectrum_id
                row.get::<_, i64>(14)?,   // isolation_window_id
                row.get::<_, f64>(15)?,   // precursor_mz
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
                isolation_window_id,
                precursor_mz,
            ) = result?;

            // Parse the MessagePack peaks blob
            let data = PeakelData::from_msgpack(&peaks_blob)?;

            peakels.push(ExtendedPeakel::new_ms2_dia(
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
                isolation_window_id,
                precursor_mz,
                data,
            ));
        }

        log::info!("Loaded {} peakels from peakeldb", peakels.len());
        Ok(peakels)
    }
}

// ============================================================================
// Writer
// ============================================================================

/// Writer for MS2 DIA peakelDB files
pub struct Ms2PeakelDbWriter {
    conn: Connection,
}

impl Ms2PeakelDbWriter {
    /// Create a new MS2 DIA peakelDB file
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
        conn.execute_batch(Ms2PeakelDbSchema::SCHEMA)?;

        Ok(Self { conn })
    }

    /// Write DIA MS2 peakels to the database
    ///
    /// # Arguments
    /// * `mzdb_filename` - Name of the source mzDB file
    /// * `windows` - Isolation windows to write
    /// * `peakels` - Peakels to write
    pub fn write_peakels(
        &self,
        mzdb_filename: &str,
        windows: &[IsolationWindow],
        peakels: &[DiaMs2PeakelRecord],
    ) -> Result<()> {
        // Insert peakeldb_file record (aligned with MS1 schema)
        let timestamp = chrono_lite_timestamp();
        self.conn.execute(
            "INSERT INTO peakeldb_file (id, name, description, raw_file_name, is_dia_experiment, 
             creation_timestamp, modification_timestamp) VALUES (?, ?, ?, ?, ?, ?, ?)",
            params![1, mzdb_filename, "Generated by mzdb2peakeldb", mzdb_filename, true, &timestamp, &timestamp],
        )?;

        // Insert lcms_map record (aligned with MS1 schema)
        self.conn.execute(
            "INSERT INTO lcms_map (id, ms_level, peakel_count, peakeldb_file_id) VALUES (?, ?, ?, ?)",
            params![1, 2, peakels.len() as i32, 1],
        )?;

        // Insert isolation windows (with map_id foreign key)
        self.conn.execute("BEGIN TRANSACTION", [])?;
        {
            let mut stmt = self.conn.prepare(
                "INSERT INTO isolation_window (id, target_mz, lower_mz, upper_mz, spectrum_count, map_id) 
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6)"
            )?;

            for window in windows {
                stmt.execute(params![
                    window.id,
                    window.target_mz,
                    window.lower_mz,
                    window.upper_mz,
                    window.spectrum_count,
                    1, // map_id
                ])?;
            }
        }
        self.conn.execute("COMMIT", [])?;

        // Insert peakels
        self.conn.execute("BEGIN TRANSACTION", [])?;
        {
            let mut stmt = self.conn.prepare(
                "INSERT INTO peakel (id, moz, elution_time, duration, gap_count, apex_intensity, area, 
                 amplitude, peak_count, peaks, serialized_properties,
                 first_spectrum_id, apex_spectrum_id, last_spectrum_id, 
                 isolation_window_id, precursor_mz, map_id) 
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17)"
            )?;

            let mut rtree_stmt = self.conn.prepare(
                "INSERT INTO peakel_rtree (id, min_mz, max_mz, min_time, max_time, min_intensity, max_intensity) 
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)"
            )?;

            for peakel in peakels {
                // Serialize peaks data to MessagePack using PeakelData's method
                let peaks_blob = peakel.peaks.to_msgpack()?;

                // Calculate min/max for R-tree using HasPeakelData trait methods
                use crate::processing::model::HasPeakelData;
                let min_mz = peakel.peaks.min_mz();
                let max_mz = peakel.peaks.max_mz();
                let min_time = peakel.peaks.min_time();
                let max_time = peakel.peaks.max_time();
                let min_intensity = peakel.peaks.intensities().iter().cloned().fold(f32::INFINITY, f32::min);

                stmt.execute(params![
                    peakel.id,
                    peakel.mz,
                    peakel.elution_time,
                    peakel.duration,
                    peakel.gap_count,
                    peakel.apex_intensity,
                    peakel.area,
                    peakel.amplitude,
                    peakel.peaks_count,
                    peaks_blob,
                    Option::<String>::None, // serialized_properties
                    peakel.first_spectrum_id,
                    peakel.apex_spectrum_id,
                    peakel.last_spectrum_id,
                    peakel.isolation_window_id,
                    peakel.precursor_mz,
                    1, // map_id
                ])?;

                rtree_stmt.execute(params![
                    peakel.id,
                    min_mz,
                    max_mz,
                    min_time as f64,
                    max_time as f64,
                    min_intensity as f64,
                    peakel.apex_intensity as f64,
                ])?;
            }
        }
        self.conn.execute("COMMIT", [])?;

        Ok(())
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extended_peakel_apex_index() {
        let data = PeakelData::from_vectors(
            vec![100, 101, 102, 103, 104],
            vec![98.0, 99.0, 100.0, 101.0, 102.0],
            vec![500.0, 500.1, 500.0, 500.1, 500.0],
            vec![1000.0, 5000.0, 10000.0, 5000.0, 1000.0],
        );

        let peakel = ExtendedPeakel::new_ms2_dia(
            1,          // id
            500.0,      // mz
            100.0,      // elution_time
            10.0,       // duration
            0,          // gap_count
            10000.0,    // apex_intensity
            50000.0,    // area
            100.0,      // amplitude
            5,          // peaks_count
            100,        // first_spectrum_id
            102,        // apex_spectrum_id
            104,        // last_spectrum_id
            1,          // isolation_window_id
            500.0,      // precursor_mz
            data,
        );

        assert_eq!(peakel.apex_data_index(), Some(2));
    }
}
