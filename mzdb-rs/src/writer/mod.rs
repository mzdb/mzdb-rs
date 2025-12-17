//! mzDB Writer Module
//!
//! This module provides functionality to create mzDB files from mass spectrometry data.
//! It implements the mzDB 0.7 format specification with support for:
//!
//! - Efficient spectrum insertion with automatic bounding box partitioning
//! - MS1 and MSn data with configurable bounding box sizes
//! - DIA/SWATH support with isolation window tracking
//! - Metadata management (instruments, software, samples, runs)
//! - R-tree spatial indexing for fast queries
//! - SQLite optimization for high-performance writes
//!
//! # Architecture
//!
//! The writer is based on the mzdb4s/io Scala implementation and follows a similar
//! architecture with Rust-specific optimizations:
//!
//! - **Bounding Box Caching**: Spectra are accumulated in memory before being flushed to disk
//! - **Binary Serialization**: Direct binary encoding of peak data with configurable precision
//! - **Spatial Partitioning**: Automatic m/z and retention time slicing
//! - **Transaction Batching**: All operations within a single SQLite transaction
//!
//! # Example
//!
//! ```no_run
//! use mzdb::writer::{MzDbWriter, MzDbWriterBuilder};
//! use mzdb::{BBSizes, DataMode, PeakEncoding, ByteOrder};
//! 
//! // Configure bounding box sizes
//! let bb_sizes = BBSizes {
//!     bb_mz_height_ms1: 10.0,
//!     bb_mz_height_msn: 10000.0,
//!     bb_rt_width_ms1: 5.0,
//!     bb_rt_width_msn: 60.0,
//! };
//!
//! // Create writer with builder pattern
//! let mut writer = MzDbWriterBuilder::new("output.mzDB")
//!     .bb_sizes(bb_sizes)
//!     .is_dia(false)
//!     .build()?;
//!
//! writer.open()?;
//!
//! // Insert spectra
//! // ... spectrum insertion logic ...
//!
//! writer.close()?;
//! # Ok::<(), anyhow::Error>(())
//! ```

mod schema;
mod bounding_box;
mod data_encoding;
mod run_slice;
mod metadata;
mod spectrum_writer;

use anyhow::{Context, Result};
use rusqlite::{Connection, Statement};
use std::path::Path;

use crate::metadata::*;
use crate::model::*;

pub use bounding_box::{BoundingBoxCache, BoundingBoxWriter};
pub use data_encoding::DataEncodingRegistry;
pub use run_slice::RunSliceFactory;
pub use schema::MZDB_SCHEMA;

pub use metadata::WriterMetadata;

/// Main mzDB writer for creating mzDB files
///
/// This writer handles all aspects of mzDB file creation including:
/// - Database initialization and schema creation
/// - Metadata insertion
/// - Spectrum data writing with bounding box partitioning
/// - Index creation and optimization
pub struct MzDbWriter {
    /// Database connection
    connection: Option<Connection>,
    
    /// Path to the mzDB file
    db_path: String,
    
    /// Metadata for the file
    metadata: WriterMetadata,
    
    /// Bounding box dimensions
    bb_sizes: BBSizes,
    
    /// Whether this is DIA/SWATH data
    is_dia: bool,
    
    /// Model version (currently 0.7)
    model_version: f32,
    
    /// Counter for inserted spectra
    inserted_spectra_count: i64,
    
    /// Bounding box cache for accumulating spectrum data
    bb_cache: BoundingBoxCache,
    
    /// Data encoding registry
    data_encoding_registry: DataEncodingRegistry,
    
    /// Run slice factory
    run_slice_factory: RunSliceFactory,
    
    /// Prepared statement for bounding box insertion
    bbox_insert_stmt: Option<Statement<'static>>,
    
    /// Prepared statement for R-tree insertion
    rtree_insert_stmt: Option<Statement<'static>>,
    
    /// Prepared statement for MSn R-tree insertion
    msn_rtree_insert_stmt: Option<Statement<'static>>,
    
    /// Prepared statement for spectrum insertion
    spectrum_insert_stmt: Option<Statement<'static>>,
}

impl MzDbWriter {
    /// Create a new MzDbWriter
    ///
    /// # Arguments
    /// * `db_path` - Path where the mzDB file will be created
    /// * `metadata` - Metadata for the file
    /// * `bb_sizes` - Bounding box dimensions
    /// * `is_dia` - Whether this is DIA/SWATH data
    pub fn new(
        db_path: impl AsRef<Path>,
        metadata: WriterMetadata,
        bb_sizes: BBSizes,
        is_dia: bool,
    ) -> Result<Self> {
        Ok(Self {
            connection: None,
            db_path: db_path.as_ref().to_string_lossy().to_string(),
            metadata,
            bb_sizes,
            is_dia,
            model_version: 0.7,
            inserted_spectra_count: 0,
            bb_cache: BoundingBoxCache::new(bb_sizes),
            data_encoding_registry: DataEncodingRegistry::new(),
            run_slice_factory: RunSliceFactory::new(1), // run_id = 1
            bbox_insert_stmt: None,
            rtree_insert_stmt: None,
            msn_rtree_insert_stmt: None,
            spectrum_insert_stmt: None,
        })
    }
    
    /// Open the database connection and initialize the schema
    ///
    /// This method:
    /// 1. Creates the SQLite database file
    /// 2. Sets optimization pragmas
    /// 3. Creates the schema
    /// 4. Prepares INSERT statements
    /// 5. Begins a transaction
    /// 6. Inserts metadata
    pub fn open(&mut self) -> Result<()> {
        // Open connection
        let mut conn = Connection::open(&self.db_path)
            .context("Failed to open database connection")?;
        
        // Apply SQLite optimizations (same as mzdb4s)
        conn.execute_batch(
            "PRAGMA encoding='UTF-8';
             PRAGMA synchronous=OFF;
             PRAGMA journal_mode=OFF;
             PRAGMA temp_store=2;
             PRAGMA cache_size=-100000;
             PRAGMA page_size=4096;
             PRAGMA automatic_index=OFF;
             PRAGMA locking_mode=EXCLUSIVE;
             PRAGMA foreign_keys=OFF;
             PRAGMA ignore_check_constraints=ON;
             BEGIN TRANSACTION;"
        ).context("Failed to set SQLite pragmas")?;
        
        // Create schema
        conn.execute_batch(MZDB_SCHEMA)
            .context("Failed to create mzDB schema")?;
        
        // TODO: Prepare INSERT statements
        // This requires converting the statements to 'static lifetime
        // which is complex in Rust. We'll use direct execute calls instead.
        
        // Insert metadata
        metadata::insert_metadata(&mut conn, &self.metadata, &self.bb_sizes, self.is_dia)
            .context("Failed to insert metadata")?;
        
        self.connection = Some(conn);
        
        Ok(())
    }
    
    /// Insert a spectrum into the mzDB file
    ///
    /// This is the main method for adding spectrum data. It handles:
    /// - Automatic bounding box assignment based on m/z and RT
    /// - Cache management and flushing
    /// - Run slice creation
    /// - Data encoding registration
    ///
    /// # Arguments
    /// * `spectrum` - The spectrum to insert
    /// * `data_encoding` - Data encoding specification
    pub fn insert_spectrum(
        &mut self,
        spectrum: &Spectrum,
        data_encoding: &DataEncoding,
    ) -> Result<()> {
        spectrum_writer::insert_spectrum(
            self,
            spectrum,
            data_encoding,
        )
    }
    
    /// Close the database and finalize the mzDB file
    ///
    /// This method:
    /// 1. Flushes remaining cached bounding boxes
    /// 2. Converts temporary spectrum table to permanent
    /// 3. Inserts data encodings
    /// 4. Inserts run slices
    /// 5. Creates all indexes
    /// 6. Commits the transaction
    pub fn close(mut self) -> Result<()> {
        let conn = self.connection.take()
            .context("No active connection")?;
        
        // Flush remaining bounding boxes
        self.flush_all_bb_rows()?;
        
        // Convert temporary spectrum table to permanent
        conn.execute("CREATE TABLE spectrum AS SELECT * FROM tmp_spectrum;", [])
            .context("Failed to create permanent spectrum table")?;
        
        // Insert data encodings
        data_encoding::insert_data_encodings(&conn, &self.data_encoding_registry)?;
        
        // Insert run slices
        run_slice::insert_run_slices(&conn, &self.run_slice_factory)?;
        
        // Create indexes
        self.create_indexes(&conn)?;
        
        // Commit transaction
        conn.execute_batch("COMMIT TRANSACTION;")
            .context("Failed to commit transaction")?;
        
        // Update sqlite_sequence
        let conn2 = Connection::open(&self.db_path)?;
        conn2.execute(
            "INSERT INTO sqlite_sequence VALUES ('spectrum', ?1);",
            [self.inserted_spectra_count]
        )?;
        
        Ok(())
    }
    
    /// Flush all cached bounding box rows
    fn flush_all_bb_rows(&mut self) -> Result<()> {
        let bb_row_keys = self.bb_cache.get_bb_row_keys();
        for (ms_level, iso_win_opt) in bb_row_keys {
            self.flush_bb_row(ms_level, iso_win_opt)?;
        }
        Ok(())
    }
    
    /// Flush a specific bounding box row
    fn flush_bb_row(
        &mut self,
        ms_level: i64,
        isolation_window: Option<MzRange>,
    ) -> Result<()> {
        bounding_box::flush_bb_row(
            self,
            ms_level,
            isolation_window,
        )
    }
    
    /// Create all database indexes
    fn create_indexes(&self, conn: &Connection) -> Result<()> {
        conn.execute_batch(
            "CREATE UNIQUE INDEX spectrum_initial_id_idx ON spectrum (initial_id ASC,run_id ASC);
             CREATE INDEX spectrum_ms_level_idx ON spectrum (ms_level ASC,run_id ASC);
             CREATE UNIQUE INDEX run_name_idx ON run (name);
             CREATE UNIQUE INDEX run_slice_mz_range_idx ON run_slice (begin_mz ASC,end_mz ASC,ms_level ASC,run_id ASC);
             CREATE INDEX bounding_box_run_slice_idx ON bounding_box (run_slice_id ASC);
             CREATE INDEX bounding_box_first_spectrum_idx ON bounding_box (first_spectrum_id ASC);
             CREATE UNIQUE INDEX controlled_vocabulary_full_name_idx ON cv (full_name);
             CREATE INDEX controlled_vocabulary_uri_idx ON cv (uri);
             CREATE UNIQUE INDEX source_file_name_idx ON source_file (name);
             CREATE UNIQUE INDEX sample_name_idx ON sample (name);
             CREATE UNIQUE INDEX software_name_idx ON software (name);
             CREATE UNIQUE INDEX instrument_configuration_name_idx ON instrument_configuration (name);
             CREATE UNIQUE INDEX processing_method_number_idx ON processing_method (number ASC);
             CREATE UNIQUE INDEX data_processing_name_idx ON data_processing (name);
             CREATE UNIQUE INDEX chromatogram_name_idx ON chromatogram (name);
             CREATE UNIQUE INDEX cv_term_name_idx ON cv_term (name ASC);
             CREATE UNIQUE INDEX user_term_name_idx ON user_term (name ASC);
             CREATE UNIQUE INDEX cv_unit_name_idx ON cv_unit (name ASC);
             CREATE INDEX spectrum_bb_first_spectrum_id_idx ON spectrum (bb_first_spectrum_id ASC);"
        ).context("Failed to create indexes")?;
        
        Ok(())
    }
}

/// Builder pattern for MzDbWriter
pub struct MzDbWriterBuilder {
    db_path: String,
    metadata: Option<WriterMetadata>,
    bb_sizes: Option<BBSizes>,
    is_dia: bool,
}

impl MzDbWriterBuilder {
    /// Create a new builder
    pub fn new(db_path: impl AsRef<Path>) -> Self {
        Self {
            db_path: db_path.as_ref().to_string_lossy().to_string(),
            metadata: None,
            bb_sizes: None,
            is_dia: false,
        }
    }
    
    /// Set metadata
    pub fn metadata(mut self, metadata: WriterMetadata) -> Self {
        self.metadata = Some(metadata);
        self
    }
    
    /// Set bounding box sizes
    pub fn bb_sizes(mut self, bb_sizes: BBSizes) -> Self {
        self.bb_sizes = Some(bb_sizes);
        self
    }
    
    /// Set whether this is DIA data
    pub fn is_dia(mut self, is_dia: bool) -> Self {
        self.is_dia = is_dia;
        self
    }
    
    /// Build the writer
    pub fn build(self) -> Result<MzDbWriter> {
        let metadata = self.metadata
            .unwrap_or_else(|| WriterMetadata::with_defaults());
        let bb_sizes = self.bb_sizes
            .unwrap_or_else(|| BBSizes {
                bb_mz_height_ms1: 10.0,
                bb_mz_height_msn: 10000.0,
                bb_rt_width_ms1: 5.0,
                bb_rt_width_msn: 60.0,
            });
        
        MzDbWriter::new(self.db_path, metadata, bb_sizes, self.is_dia)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_writer_builder() {
        let metadata = WriterMetadata::with_defaults();
        let builder = MzDbWriterBuilder::new("/tmp/test.mzDB")
            .metadata(metadata)
            .is_dia(false);
        
        assert!(builder.build().is_ok());
    }
}
