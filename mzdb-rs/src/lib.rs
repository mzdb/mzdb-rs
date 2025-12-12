//! mzdb-rs: A Rust library for reading mzDB mass spectrometry files
//!
//! # Example
//! ```no_run
//! use mzdb::MzDbReader;
//!
//! let reader = MzDbReader::open("path/to/file.mzDB").unwrap();
//! for spectrum in reader.iter_spectra(None).unwrap() {
//!     println!("Spectrum {}: {} peaks", spectrum.header.id, spectrum.data.peaks_count);
//! }
//! ```

pub mod cache;
pub mod iterator;
pub mod model;
pub mod mzdb;
pub mod queries;

// Re-export main types for convenience
pub use model::{
    AcquisitionMode, BBSizes, BoundingBox, BoundingBoxIndex, ByteOrder, DataEncoding,
    DataEncodingsCache, DataMode, DataPrecision, EntityCache, IsolationWindow, PeakEncoding,
    Spectrum, SpectrumData, SpectrumHeader, SpectrumSlice, XicMethod, XicPeak,
};

use anyhow::Result;
use anyhow_ext::Context;
use rusqlite::Connection;

use crate::iterator::for_each_spectrum;
use crate::mzdb::create_entity_cache;
use crate::queries::{get_ms_xic, get_mzdb_version, get_pwiz_mzdb_version, get_spectrum};

/// Main entry point for reading mzDB files
pub struct MzDbReader {
    connection: Connection,
    entity_cache: EntityCache,
}

impl MzDbReader {
    /// Open an mzDB file for reading
    pub fn open(path: &str) -> Result<Self> {
        let connection = Connection::open(path).dot()?;
        let entity_cache = create_entity_cache(&connection).dot()?;
        Ok(Self {
            connection,
            entity_cache,
        })
    }

    /// Get the mzDB format version
    pub fn get_version(&self) -> Result<Option<String>> {
        get_mzdb_version(&self.connection)
    }

    /// Get the pwiz-mzDB writer version
    pub fn get_writer_version(&self) -> Result<Option<String>> {
        get_pwiz_mzdb_version(&self.connection)
    }

    /// Get the bounding box sizes
    pub fn get_bb_sizes(&self) -> &BBSizes {
        &self.entity_cache.bb_sizes
    }

    /// Get all spectrum headers
    pub fn get_spectrum_headers(&self) -> &[SpectrumHeader] {
        &self.entity_cache.spectrum_headers
    }

    /// Get a spectrum by ID
    pub fn get_spectrum(&self, spectrum_id: i64) -> Result<Spectrum> {
        get_spectrum(&self.connection, spectrum_id, &self.entity_cache)
    }

    /// Iterate over all spectra, optionally filtering by MS level
    pub fn iter_spectra(&self, ms_level: Option<u8>) -> Result<Vec<Spectrum>> {
        let mut spectra = Vec::new();
        for_each_spectrum(&self.connection, &self.entity_cache, ms_level, |s| {
            spectra.push(s.clone());
            Ok(())
        })?;
        Ok(spectra)
    }

    /// Get an extracted ion chromatogram (XIC)
    pub fn get_xic(
        &self,
        mz: f64,
        mz_tol_ppm: f64,
        min_rt: Option<f32>,
        max_rt: Option<f32>,
        method: XicMethod,
    ) -> Result<Vec<XicPeak>> {
        get_ms_xic(
            &self.connection,
            mz,
            mz_tol_ppm,
            min_rt,
            max_rt,
            method,
            &self.entity_cache,
        )
    }

    /// Get access to the underlying SQLite connection for advanced queries
    pub fn connection(&self) -> &Connection {
        &self.connection
    }

    /// Get access to the entity cache for advanced queries
    pub fn entity_cache(&self) -> &EntityCache {
        &self.entity_cache
    }
}
