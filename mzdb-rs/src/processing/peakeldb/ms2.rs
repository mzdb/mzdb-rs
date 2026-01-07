//! MS2 DIA PeakelDB - Format for DIA MS2 peakels with isolation windows
//!
//! This module provides utilities for creating and reading MS2 DIA peakelDB files.
//! The MS2 format includes isolation_window table for DIA data and uses
//! MessagePack-encoded peaks blob.

use std::path::Path;

use anyhow_ext::{Context, Result};
use rusqlite::{params, Connection};

use crate::processing::dia::{IsolationWindow, DiaMs2PeakelRecord, PeaksData};
use super::common::{chrono_lite_timestamp, parse_peaks_blob};

// ============================================================================
// Schema
// ============================================================================

/// MS2 DIA PeakelDB schema
pub struct Ms2PeakelDbSchema;

impl Ms2PeakelDbSchema {
    /// SQL schema for MS2 DIA peakelDB
    pub const SCHEMA: &'static str = r#"
CREATE TABLE peakeldb_info (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    description TEXT,
    creation_timestamp TEXT NOT NULL,
    peakel_count INTEGER NOT NULL,
    ms_level INTEGER NOT NULL DEFAULT 2
);

CREATE TABLE isolation_window (
    id INTEGER PRIMARY KEY,
    target_mz REAL NOT NULL,
    lower_mz REAL NOT NULL,
    upper_mz REAL NOT NULL,
    spectrum_count INTEGER NOT NULL
);

CREATE TABLE peakel (
    id INTEGER PRIMARY KEY,
    mz REAL NOT NULL,
    elution_time REAL NOT NULL,
    duration REAL NOT NULL,
    gap_count INTEGER NOT NULL,
    apex_intensity REAL NOT NULL,
    area REAL NOT NULL,
    amplitude REAL NOT NULL,
    peaks_count INTEGER NOT NULL,
    first_spectrum_id INTEGER NOT NULL,
    apex_spectrum_id INTEGER NOT NULL,
    last_spectrum_id INTEGER NOT NULL,
    isolation_window_id INTEGER NOT NULL,
    precursor_mz REAL NOT NULL,
    peaks BLOB NOT NULL,
    FOREIGN KEY (isolation_window_id) REFERENCES isolation_window(id)
);

CREATE INDEX peakel_mz_idx ON peakel (mz);
CREATE INDEX peakel_rt_idx ON peakel (elution_time);
CREATE INDEX peakel_isolation_window_idx ON peakel (isolation_window_id);
CREATE INDEX peakel_precursor_mz_idx ON peakel (precursor_mz);

CREATE VIRTUAL TABLE peakel_rtree USING rtree(
    id,
    min_mz, max_mz,
    min_time, max_time
);
"#;
}

// ============================================================================
// Data Structures
// ============================================================================

/// A peakel read from the peakeldb with full peaks data
#[derive(Debug, Clone)]
pub struct SimplifierPeakel {
    pub id: i64,
    pub mz: f64,
    pub elution_time: f32,
    pub duration: f32,
    pub gap_count: i32,
    pub apex_intensity: f32,
    pub area: f32,
    pub amplitude: f32,
    pub peaks_count: i32,
    pub first_spectrum_id: i64,
    pub apex_spectrum_id: i64,
    pub last_spectrum_id: i64,
    pub isolation_window_id: i64,
    pub precursor_mz: f64,
    /// Spectrum IDs at each data point
    pub spectrum_ids: Vec<i64>,
    /// Retention times at each data point (seconds)
    pub elution_times: Vec<f32>,
    /// m/z values at each data point
    pub mz_values: Vec<f64>,
    /// Intensities at each data point
    pub intensities: Vec<f32>,
}

impl SimplifierPeakel {
    /// Get the index of the apex spectrum in this peakel's data arrays
    pub fn apex_index(&self) -> Option<usize> {
        self.spectrum_ids
            .iter()
            .position(|&id| id == self.apex_spectrum_id)
    }
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
    pub fn read_all_peakels(&self) -> Result<Vec<SimplifierPeakel>> {
        let mut stmt = self.conn.prepare(
            "SELECT id, mz, elution_time, duration, gap_count, apex_intensity,
                    area, amplitude, peaks_count, first_spectrum_id, 
                    apex_spectrum_id, last_spectrum_id, isolation_window_id,
                    precursor_mz, peaks
             FROM peakel
             ORDER BY id",
        )?;

        let peakel_iter = stmt.query_map([], |row| {
            let peaks_blob: Vec<u8> = row.get(14)?;
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, f64>(1)?,
                row.get::<_, f32>(2)?,
                row.get::<_, f32>(3)?,
                row.get::<_, i32>(4)?,
                row.get::<_, f32>(5)?,
                row.get::<_, f32>(6)?,
                row.get::<_, f32>(7)?,
                row.get::<_, i32>(8)?,
                row.get::<_, i64>(9)?,
                row.get::<_, i64>(10)?,
                row.get::<_, i64>(11)?,
                row.get::<_, i64>(12)?,
                row.get::<_, f64>(13)?,
                peaks_blob,
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
                first_spectrum_id,
                apex_spectrum_id,
                last_spectrum_id,
                isolation_window_id,
                precursor_mz,
                peaks_blob,
            ) = result?;

            // Parse the MessagePack peaks blob
            let (spectrum_ids, elution_times, mz_values, intensities) =
                parse_peaks_blob(&peaks_blob)?;

            peakels.push(SimplifierPeakel {
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
                spectrum_ids,
                elution_times,
                mz_values,
                intensities,
            });
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
    /// * `windows` - Isolation windows to write
    /// * `peakels` - Peakels to write
    pub fn write_peakels(
        &self,
        windows: &[IsolationWindow],
        peakels: &[DiaMs2PeakelRecord],
    ) -> Result<()> {
        // Insert peakeldb_info
        let timestamp = chrono_lite_timestamp();
        self.conn.execute(
            "INSERT INTO peakeldb_info (id, name, description, creation_timestamp, peakel_count, ms_level) 
             VALUES (1, 'DIA MS2 peakelDB', 'Generated by mzdb-rs', ?1, ?2, 2)",
            params![timestamp, peakels.len()],
        )?;

        // Insert isolation windows
        self.conn.execute("BEGIN TRANSACTION", [])?;
        {
            let mut stmt = self.conn.prepare(
                "INSERT INTO isolation_window (id, target_mz, lower_mz, upper_mz, spectrum_count) 
                 VALUES (?1, ?2, ?3, ?4, ?5)"
            )?;

            for window in windows {
                stmt.execute(params![
                    window.id,
                    window.target_mz,
                    window.lower_mz,
                    window.upper_mz,
                    window.spectrum_count,
                ])?;
            }
        }
        self.conn.execute("COMMIT", [])?;

        // Insert peakels
        self.conn.execute("BEGIN TRANSACTION", [])?;
        {
            let mut stmt = self.conn.prepare(
                "INSERT INTO peakel (id, mz, elution_time, duration, gap_count, apex_intensity, area, 
                 amplitude, peaks_count, first_spectrum_id, apex_spectrum_id, last_spectrum_id, 
                 isolation_window_id, precursor_mz, peaks) 
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15)"
            )?;

            let mut rtree_stmt = self.conn.prepare(
                "INSERT INTO peakel_rtree (id, min_mz, max_mz, min_time, max_time) 
                 VALUES (?1, ?2, ?3, ?4, ?5)"
            )?;

            for peakel in peakels {
                // Serialize peaks data to MessagePack
                let peaks_blob = serialize_peaks_data(&peakel.peaks)?;

                // Calculate min/max for R-tree
                let min_mz = peakel.peaks.mz_values.iter().cloned().fold(f64::INFINITY, f64::min);
                let max_mz = peakel.peaks.mz_values.iter().cloned().fold(f64::NEG_INFINITY, f64::max);
                let min_time = peakel.peaks.elution_times.iter().cloned().fold(f32::INFINITY, f32::min);
                let max_time = peakel.peaks.elution_times.iter().cloned().fold(f32::NEG_INFINITY, f32::max);

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
                    peakel.first_spectrum_id,
                    peakel.apex_spectrum_id,
                    peakel.last_spectrum_id,
                    peakel.isolation_window_id,
                    peakel.precursor_mz,
                    peaks_blob,
                ])?;

                rtree_stmt.execute(params![
                    peakel.id,
                    min_mz,
                    max_mz,
                    min_time as f64,
                    max_time as f64,
                ])?;
            }
        }
        self.conn.execute("COMMIT", [])?;

        Ok(())
    }
}

// ============================================================================
// Serialization
// ============================================================================

/// Serialize peaks data to MessagePack format
fn serialize_peaks_data(peaks: &PeaksData) -> Result<Vec<u8>> {
    use rmpv::Value;

    let spectrum_ids: Vec<Value> = peaks.spectrum_ids.iter()
        .map(|&id| Value::Integer(id.into()))
        .collect();
    let elution_times: Vec<Value> = peaks.elution_times.iter()
        .map(|&t| Value::F32(t))
        .collect();
    let mz_values: Vec<Value> = peaks.mz_values.iter()
        .map(|&mz| Value::F64(mz))
        .collect();
    let intensities: Vec<Value> = peaks.intensity_values.iter()
        .map(|&i| Value::F32(i))
        .collect();

    let value = Value::Array(vec![
        Value::Array(spectrum_ids),
        Value::Array(elution_times),
        Value::Array(mz_values),
        Value::Array(intensities),
    ]);

    let mut buf = Vec::new();
    rmpv::encode::write_value(&mut buf, &value)?;
    Ok(buf)
}

// ============================================================================
// Statistics
// ============================================================================

/// Print MS2 DIA peakel statistics to stdout
pub fn print_ms2_statistics(peakels: &[DiaMs2PeakelRecord]) {
    if peakels.is_empty() {
        return;
    }

    let total_area: f64 = peakels.iter().map(|p| p.area as f64).sum();
    let avg_duration = peakels.iter().map(|p| p.duration).sum::<f32>() / peakels.len() as f32;
    let avg_peaks = peakels.iter().map(|p| p.peaks_count as f32).sum::<f32>() / peakels.len() as f32;

    let min_mz = peakels.iter().map(|p| p.mz).fold(f64::INFINITY, f64::min);
    let max_mz = peakels.iter().map(|p| p.mz).fold(f64::NEG_INFINITY, f64::max);
    let min_rt = peakels.iter().map(|p| p.elution_time).fold(f32::INFINITY, f32::min);
    let max_rt = peakels.iter().map(|p| p.elution_time).fold(f32::NEG_INFINITY, f32::max);

    println!();
    println!("=== MS2 DIA Peakel Statistics ===");
    println!("Total peakels: {}", peakels.len());
    println!("Total area: {:.2e}", total_area);
    println!("Average duration: {:.2}s", avg_duration);
    println!("Average peaks per peakel: {:.1}", avg_peaks);
    println!("m/z range: {:.2} - {:.2}", min_mz, max_mz);
    println!("RT range: {:.2}s - {:.2}s", min_rt, max_rt);
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_simplifier_peakel_apex_index() {
        let peakel = SimplifierPeakel {
            id: 1,
            mz: 500.0,
            elution_time: 100.0,
            duration: 10.0,
            gap_count: 0,
            apex_intensity: 10000.0,
            area: 50000.0,
            amplitude: 100.0,
            peaks_count: 5,
            first_spectrum_id: 100,
            apex_spectrum_id: 102,
            last_spectrum_id: 104,
            isolation_window_id: 1,
            precursor_mz: 500.0,
            spectrum_ids: vec![100, 101, 102, 103, 104],
            elution_times: vec![98.0, 99.0, 100.0, 101.0, 102.0],
            mz_values: vec![500.0, 500.1, 500.0, 500.1, 500.0],
            intensities: vec![1000.0, 5000.0, 10000.0, 5000.0, 1000.0],
        };

        assert_eq!(peakel.apex_index(), Some(2));
    }
}
