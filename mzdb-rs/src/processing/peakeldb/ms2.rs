//! MS2 DIA PeakelDB - Compact format for DIA MS2 peakels with isolation windows
//!
//! This module provides utilities for creating and reading MS2 DIA peakelDB files.
//! The schema is aligned with the MS1 peakelDB format for consistency, with additional
//! tables for DIA-specific data (isolation windows, MS2 spectra).
//!
//! # Schema Overview
//!
//! - `peakeldb_file`: File-level metadata (same as MS1)
//! - `lcms_map`: Map metadata with ms_level=2 (same as MS1)
//! - `isolation_window`: DIA-specific isolation window definitions
//! - `ms2_spectrum_ref`: MS2 spectrum metadata (elution times, cycles, window assignment)
//! - `peakel`: Peakel data with isolation_window_id
//! - `peakel_rtree`: R-tree spatial index for fast queries
//!
//! # Compact MessagePack Format
//!
//! Peakel blobs use a compact 3-array format: `[spectrum_ids (i32), mz_values (f32), intensities (f32)]`
//! saving ~50% over the legacy 4-array format by:
//! - Removing redundant elution_times (available from `ms2_spectrum_ref` table)
//! - Using i32 for spectrum IDs (sufficient for any real dataset)
//! - Using f32 for m/z values (already f32 in memory)

use std::collections::HashMap;
use std::path::Path;

use anyhow_ext::{Context, Result};
use rusqlite::{params, Connection};

use crate::model::IsolationWindow;
use crate::model::SpectrumHeader;
use crate::processing::signal::ms2_detection::DiaMs2PeakelRecord;
use crate::processing::model::HasPeakelData;
use crate::processing::Peakel;
use super::common::{chrono_lite_timestamp, ExtendedPeakel, PeakelWriterStats, PeakelDbWriter};

// ============================================================================
// MS2 Compact PeakelSerializer - 3-array MessagePack format
// ============================================================================

/// MS2 peakel serialization using the compact 3-array MessagePack format.
///
/// # Format
/// MessagePack tuple of 3 arrays: `[spectrum_ids (i32), mz_values (f32), intensities (f32)]`
///
/// Elution times are not stored in the blob; they are recovered from the `ms2_spectrum_ref`
/// table on read.
struct Ms2PeakelSerializer;

impl Ms2PeakelSerializer {
    /// Serialize peakel data to compact MessagePack bytes (3-array format).
    ///
    /// Spectrum IDs are narrowed from i64 to i32 on write.
    fn to_msgpack<T: HasPeakelData>(peakel: &T) -> Result<Vec<u8>> {
        let spectrum_ids_i32: Vec<i32> = peakel.spectrum_ids().iter()
            .map(|&id| id as i32)
            .collect();

        let data = (
            &spectrum_ids_i32,
            peakel.mz_values(),
            peakel.intensity_values(),
        );
        rmp_serde::to_vec(&data)
            .map_err(|e| anyhow_ext::anyhow!("msgpack serialization error: {}", e))
    }

    /// Deserialize compact MessagePack bytes to a new Peakel.
    ///
    /// Requires an RT lookup (spectrum_id → elution_time) to reconstruct
    /// the elution_times vector that is not stored in the compact format.
    fn from_msgpack(bytes: &[u8], rt_lookup: &HashMap<i64, f32>) -> Result<Peakel> {
        let (spectrum_ids_i32, mz_values, intensity_values):
            (Vec<i32>, Vec<f32>, Vec<f32>) =
            rmp_serde::from_slice(bytes)
                .map_err(|e| anyhow_ext::anyhow!("msgpack deserialization error: {}", e))?;

        // Widen spectrum IDs from i32 to i64
        let spectrum_ids: Vec<i64> = spectrum_ids_i32.iter().map(|&id| id as i64).collect();

        // Reconstruct elution times from lookup
        let elution_times: Vec<f32> = spectrum_ids.iter()
            .map(|id| {
                *rt_lookup.get(id).unwrap_or_else(|| {
                    log::warn!("No elution time found for spectrum_id {}", id);
                    &0.0
                })
            })
            .collect();

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

/// MS2 DIA PeakelDB schema (aligned with MS1 schema)
pub struct Ms2PeakelDbSchema;

impl Ms2PeakelDbSchema {
    /// SQL schema for MS2 DIA peakelDB
    ///
    /// This schema is aligned with the MS1 peakelDB schema for consistency:
    /// - Same table names: peakeldb_file, lcms_map, peakel, peakel_rtree
    /// - Same base field names: moz (not mz), peak_count (not peaks_count)
    /// - Additional: isolation_window and ms2_spectrum_ref tables
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

CREATE TABLE ms2_spectrum_ref (
    id INTEGER PRIMARY KEY,
    initial_id INTEGER NOT NULL,
    cycle INTEGER NOT NULL,
    elution_time REAL NOT NULL,
    isolation_window_id INTEGER NOT NULL,
    FOREIGN KEY (isolation_window_id) REFERENCES isolation_window (id)
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
    peak_count INTEGER NOT NULL,
    peaks BLOB NOT NULL,
    serialized_properties TEXT,
    first_spectrum_id INTEGER NOT NULL,
    apex_spectrum_id INTEGER NOT NULL,
    last_spectrum_id INTEGER NOT NULL,
    isolation_window_id INTEGER NOT NULL,
    map_id INTEGER NOT NULL,
    FOREIGN KEY (first_spectrum_id) REFERENCES ms2_spectrum_ref (id),
    FOREIGN KEY (apex_spectrum_id) REFERENCES ms2_spectrum_ref (id),
    FOREIGN KEY (last_spectrum_id) REFERENCES ms2_spectrum_ref (id),
    FOREIGN KEY (isolation_window_id) REFERENCES isolation_window (id),
    FOREIGN KEY (map_id) REFERENCES lcms_map (id)
);

CREATE VIRTUAL TABLE peakel_rtree USING rtree(
    id,
    min_mz, max_mz,
    min_time, max_time,
    min_intensity, max_intensity
);

CREATE INDEX ms2_spectrum_ref_iw_idx ON ms2_spectrum_ref (isolation_window_id);
CREATE INDEX peakel_moz_idx ON peakel (moz);
CREATE INDEX peakel_elution_time_idx ON peakel (elution_time);
CREATE INDEX peakel_first_spectrum_idx ON peakel (first_spectrum_id);
CREATE INDEX peakel_apex_spectrum_idx ON peakel (apex_spectrum_id);
CREATE INDEX peakel_last_spectrum_idx ON peakel (last_spectrum_id);
CREATE INDEX peakel_isolation_window_idx ON peakel (isolation_window_id);
CREATE INDEX peakel_map_id_idx ON peakel (map_id);
"#;
}

// ============================================================================
// Writer
// ============================================================================

/// Writer for MS2 DIA peakelDB files
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
/// use mzdb::MzDbReader;
/// use mzdb::processing::peakeldb::{Ms2PeakelDbWriter, PeakelDbWriter};
/// use mzdb::IsolationWindow;
///
/// let reader = MzDbReader::open("input.mzDB").unwrap();
/// let windows = reader.get_isolation_windows();
/// let mut writer = Ms2PeakelDbWriter::create(
///     "output.peakeldb", "input.mzDB", reader.get_spectrum_headers(), &windows
/// ).unwrap();
/// // ... write batches via write_peakels_batch() ...
/// writer.close().unwrap();
/// ```
pub struct Ms2PeakelDbWriter {
    conn: Connection,
    output_path: std::path::PathBuf,
    stats: PeakelWriterStats,
    closed: bool,
}

impl Ms2PeakelDbWriter {
    /// SQL for inserting a peakel row
    const PEAKEL_INSERT_SQL: &'static str =
        "INSERT INTO peakel (id, moz, elution_time, duration, gap_count, apex_intensity, area, 
         amplitude, intensity_cv, peak_count, peaks, serialized_properties,
         first_spectrum_id, apex_spectrum_id, last_spectrum_id, 
         isolation_window_id, map_id)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17)";

    /// SQL for inserting an R-tree entry
    const RTREE_INSERT_SQL: &'static str =
        "INSERT INTO peakel_rtree (id, min_mz, max_mz, min_time, max_time, min_intensity, max_intensity) 
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)";

    /// Create a new MS2 DIA peakelDB file
    ///
    /// Creates the database, inserts metadata, isolation windows, and MS2 spectrum
    /// metadata, then begins a transaction for peakel insertion.
    ///
    /// # Arguments
    /// * `path` - Path to the output file (will be overwritten if exists)
    /// * `mzdb_filename` - Name of the source mzDB file
    /// * `spectrum_headers` - Spectrum headers from the source mzDB (for ms2_spectrum_ref table)
    /// * `windows` - Isolation windows to write
    pub fn create<P: AsRef<Path>>(
        path: P,
        mzdb_filename: &str,
        spectrum_headers: &[SpectrumHeader],
        windows: &[IsolationWindow],
    ) -> Result<Self> {
        // Remove existing file if present
        if path.as_ref().exists() {
            std::fs::remove_file(path.as_ref())?;
        }

        let output_path = path.as_ref().to_path_buf();

        // Work in memory for fast R-tree population, then flush to disk on close
        let conn = Connection::open_in_memory()
            .context("Failed to create in-memory peakelDB")?;

        // SQLite optimizations
        conn.execute_batch(
            "PRAGMA synchronous=OFF;
             PRAGMA journal_mode=OFF;
             PRAGMA temp_store=2;
             PRAGMA cache_size=100000;"
        )?;

        // Create schema
        conn.execute_batch(Ms2PeakelDbSchema::SCHEMA)?;

        // Insert peakeldb_file record
        let timestamp = chrono_lite_timestamp();
        conn.execute(
            "INSERT INTO peakeldb_file (id, name, description, raw_file_name, is_dia_experiment, 
             creation_timestamp, modification_timestamp) VALUES (?, ?, ?, ?, ?, ?, ?)",
            params![1, mzdb_filename, "Generated by mzdb2peakeldb", mzdb_filename, true, &timestamp, &timestamp],
        )?;

        // Insert lcms_map record with placeholder peakel_count (updated on close)
        conn.execute(
            "INSERT INTO lcms_map (id, ms_level, peakel_count, peakeldb_file_id) VALUES (?, ?, ?, ?)",
            params![1, 2, 0, 1],
        )?;

        // Build window lookup: target_mz (rounded to 0.1) -> window_id
        let window_lookup: HashMap<i64, i64> = windows.iter()
            .map(|w| ((w.target_mz * 10.0).round() as i64, w.id))
            .collect();

        // Insert isolation windows
        {
            let mut stmt = conn.prepare(
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

        // Insert MS2 spectrum metadata
        Self::insert_ms2_spectra(&conn, spectrum_headers, &window_lookup)?;

        // Begin transaction for peakel insertion
        conn.execute("BEGIN TRANSACTION", [])?;

        Ok(Self {
            conn,
            output_path,
            stats: PeakelWriterStats::new(),
            closed: false,
        })
    }

    /// Insert MS2 spectrum rows from the source mzDB spectrum headers
    fn insert_ms2_spectra(
        conn: &Connection,
        headers: &[SpectrumHeader],
        window_lookup: &HashMap<i64, i64>,
    ) -> Result<()> {
        let mut stmt = conn.prepare(
            "INSERT INTO ms2_spectrum_ref (id, initial_id, cycle, elution_time, isolation_window_id) 
             VALUES (?1, ?2, ?3, ?4, ?5)"
        )?;

        let mut count = 0;
        for header in headers {
            if header.ms_level == 2 {
                if let Some(precursor_mz) = header.precursor_mz {
                    let window_key = (precursor_mz * 10.0).round() as i64;
                    if let Some(&window_id) = window_lookup.get(&window_key) {
                        stmt.execute(params![
                            header.id,
                            header.initial_id,
                            header.cycle,
                            header.time,
                            window_id,
                        ])?;
                        count += 1;
                    } else {
                        log::warn!(
                            "MS2 spectrum {} (precursor_mz={:.4}) has no matching isolation window",
                            header.id, precursor_mz
                        );
                    }
                }
            }
        }

        log::info!("Inserted {} MS2 spectrum records into peakelDB", count);

        Ok(())
    }

    /// Write a batch of DIA MS2 peakels to the database (internal implementation)
    ///
    /// Can be called multiple times to write peakels incrementally.
    /// Peakel IDs come from `DiaMs2PeakelRecord::id()`.
    fn write_peakels_batch_impl(&mut self, peakels: &[DiaMs2PeakelRecord]) -> Result<()> {
        if peakels.is_empty() {
            return Ok(());
        }

        let mut peakel_stmt = self.conn.prepare_cached(Self::PEAKEL_INSERT_SQL)?;
        let mut rtree_stmt = self.conn.prepare_cached(Self::RTREE_INSERT_SQL)?;

        for peakel in peakels {
            self.stats.add_peakel(peakel);

            // Serialize peaks data using compact 3-array format
            let peaks_blob = Ms2PeakelSerializer::to_msgpack(peakel)?;

            // Calculate min/max for R-tree using HasPeakelData trait methods
            let min_mz = peakel.min_mz();
            let max_mz = peakel.max_mz();
            let min_time = peakel.min_time();
            let max_time = peakel.max_time();
            let min_intensity = peakel.calc_min_intensity();
            let apex_intensity = peakel.apex_intensity();
            let amplitude = if min_intensity == 0.0 {0.0} else {apex_intensity / min_intensity};
            let intensity_cv = peakel.calc_intensity_cv();

            peakel_stmt.execute(params![
                peakel.id(),
                peakel.mz(),
                peakel.elution_time(),
                peakel.duration(),
                peakel.gap_count(),
                apex_intensity,
                peakel.area(),
                amplitude,
                intensity_cv,
                peakel.peaks_count(),
                peaks_blob,
                Option::<String>::None, // serialized_properties
                peakel.first_spectrum_id(),
                peakel.apex_spectrum_id(),
                peakel.last_spectrum_id(),
                peakel.isolation_window_id,
                1, // map_id
            ])?;

            rtree_stmt.execute(params![
                peakel.id(),
                min_mz,
                max_mz,
                min_time as f64,
                max_time as f64,
                min_intensity as f64,
                peakel.apex_intensity() as f64,
            ])?;
        }

        Ok(())
    }

    /// Finalize the database: commit the transaction, then flush to disk.
    fn close_impl(&mut self) -> Result<()> {
        if self.closed {
            return Ok(());
        }

        self.closed = true;

        let peakel_count = self.finalize_peakeldb()?;

        // Flush in-memory database to disk as a single sequential write
        let path_str = self.output_path.to_str()
            .context("Invalid output path")?;
        self.conn.execute("VACUUM INTO ?1", params![path_str])
            .context("Failed to flush peakelDB to disk")?;

        log::info!("MS2 DIA peakelDB closed: {} peakels written to {:?}", peakel_count, self.output_path);

        Ok(())
    }
}

impl PeakelDbWriter for Ms2PeakelDbWriter {
    type Record = DiaMs2PeakelRecord;

    fn connection(&mut self) -> &Connection {
        &self.conn
    }

    fn write_peakels_batch(&mut self, peakels: &[DiaMs2PeakelRecord]) -> Result<()> {
        self.write_peakels_batch_impl(peakels)
    }

    fn close(&mut self) -> Result<()> {
        self.close_impl()
    }

    fn stats(&self) -> &PeakelWriterStats {
        &self.stats
    }
}

impl Drop for Ms2PeakelDbWriter {
    fn drop(&mut self) {
        if !self.closed {
            if let Err(e) = self.close_impl() {
                log::error!("Error closing MS2 peakelDB writer on drop: {:?}", e);
            }
        }
    }
}

// ============================================================================
// Reader
// ============================================================================

/// Reader for MS2 DIA peakeldb SQLite files
pub struct Ms2PeakelDbReader {
    conn: Connection,
    /// Cached RT lookup built from the ms2_spectrum_ref table
    rt_lookup: HashMap<i64, f32>,
}

impl Ms2PeakelDbReader {
    /// Open a peakeldb file
    ///
    /// Loads the ms2_spectrum_ref table into memory to build the RT lookup
    /// used for deserializing compact peakel blobs.
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let conn = Connection::open(path.as_ref())
            .context("Failed to open peakeldb file")?;

        // Build RT lookup from ms2_spectrum_ref table
        let rt_lookup = Self::load_rt_lookup(&conn)?;

        Ok(Self { conn, rt_lookup })
    }

    /// Load the spectrum_id → elution_time mapping from ms2_spectrum_ref table
    fn load_rt_lookup(conn: &Connection) -> Result<HashMap<i64, f32>> {
        let mut stmt = conn.prepare(
            "SELECT id, elution_time FROM ms2_spectrum_ref"
        )?;

        let mut lookup = HashMap::new();
        let rows = stmt.query_map([], |row| {
            Ok((row.get::<_, i64>(0)?, row.get::<_, f32>(1)?))
        })?;

        for row in rows {
            let (id, rt) = row?;
            lookup.insert(id, rt);
        }

        log::info!("Loaded RT lookup with {} MS2 spectrum entries", lookup.len());

        Ok(lookup)
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

    /// Get a reference to the RT lookup
    pub fn rt_lookup(&self) -> &HashMap<i64, f32> {
        &self.rt_lookup
    }

    /// Read all peakels from the database
    pub fn read_all_peakels(&self) -> Result<Vec<ExtendedPeakel>> {
        let mut stmt = self.conn.prepare(
            "SELECT id, moz, elution_time, duration, gap_count, apex_intensity,
                    area, amplitude, peak_count, peaks, serialized_properties,
                    first_spectrum_id, apex_spectrum_id, last_spectrum_id,
                    isolation_window_id
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
                row.get::<_, i32>(8)?,    // peak_count -> peaks_count
                peaks_blob,                   // peaks (index 9)
                row.get::<_, i64>(11)?,   // first_spectrum_id
                row.get::<_, i64>(12)?,   // apex_spectrum_id
                row.get::<_, i64>(13)?,   // last_spectrum_id
                row.get::<_, i64>(14)?,   // isolation_window_id
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
            ) = result?;

            // Deserialize compact blob with RT lookup
            let data = Ms2PeakelSerializer::from_msgpack(&peaks_blob, &self.rt_lookup)?;

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
                data,
            ));
        }

        log::info!("Loaded {} peakels from peakeldb", peakels.len());
        Ok(peakels)
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compact_msgpack_roundtrip() -> Result<()> {
        let peakel = Peakel::from_vectors(
            vec![100, 101, 102, 103, 104],
            vec![98.0, 99.0, 100.0, 101.0, 102.0],
            vec![500.0, 500.1, 500.0, 500.1, 500.0],
            vec![1000.0, 5000.0, 10000.0, 5000.0, 1000.0],
            None,
            None,
            0,
        )?;

        let record = DiaMs2PeakelRecord::new(peakel.clone(), 1);

        // Serialize
        let blob = Ms2PeakelSerializer::to_msgpack(&record)?;

        // Build RT lookup
        let mut rt_lookup = HashMap::new();
        for i in 0..5 {
            rt_lookup.insert(100 + i as i64, 98.0 + i as f32);
        }

        // Deserialize
        let restored = Ms2PeakelSerializer::from_msgpack(&blob, &rt_lookup)?;

        // Verify
        assert_eq!(restored.peaks_count(), 5);
        assert_eq!(restored.spectrum_ids(), peakel.spectrum_ids());
        assert_eq!(restored.mz_values(), peakel.mz_values());
        assert_eq!(restored.intensity_values(), peakel.intensity_values());
        assert_eq!(restored.elution_times(), peakel.elution_times());

        Ok(())
    }

    #[test]
    fn test_compact_blob_smaller_than_legacy() -> Result<()> {
        let peakel = Peakel::from_vectors(
            vec![100, 101, 102, 103, 104],
            vec![98.0, 99.0, 100.0, 101.0, 102.0],
            vec![500.0, 500.1, 500.0, 500.1, 500.0],
            vec![1000.0, 5000.0, 10000.0, 5000.0, 1000.0],
            None,
            None,
            0,
        )?;

        let record = DiaMs2PeakelRecord::new(peakel.clone(), 1);
        let compact_blob = Ms2PeakelSerializer::to_msgpack(&record)?;

        // Legacy format for comparison: (i64, f32, f64, f32) = 24 bytes/point
        let legacy_blob = rmp_serde::to_vec(&(
            peakel.spectrum_ids(),
            peakel.elution_times(),
            // mz as f64 in legacy
            &peakel.mz_values().iter().map(|&mz| mz as f64).collect::<Vec<f64>>(),
            peakel.intensity_values(),
        )).unwrap();

        assert!(compact_blob.len() < legacy_blob.len(),
            "Compact ({} bytes) should be smaller than legacy ({} bytes)",
            compact_blob.len(), legacy_blob.len());

        Ok(())
    }

    #[test]
    fn test_extended_peakel_apex_index() -> Result<()> {
        let data = Peakel::from_vectors(
            vec![100, 101, 102, 103, 104],
            vec![98.0, 99.0, 100.0, 101.0, 102.0],
            vec![500.0, 500.1, 500.0, 500.1, 500.0],
            vec![1000.0, 5000.0, 10000.0, 5000.0, 1000.0],
            None,
            None,
            0,
        )?;

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
            data,
        );

        assert_eq!(peakel.apex_index(), Some(2));
        
        Ok(())
    }
}
