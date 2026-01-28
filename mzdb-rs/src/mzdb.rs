//! Core mzDB file operations
//!
//! This module provides the main entry point for working with mzDB files,
//! including the MzDbReader struct and entity cache creation.
#![allow(unused)]

use anyhow_ext::{Context, Result};
use rusqlite::Connection;

use crate::cache::create_entity_cache;
use crate::chromatogram::*;
use crate::iterator::{for_each_spectrum as iterator_for_each_spectrum, SpectrumIterator};
use crate::metadata::*;
use crate::model::*;
use crate::queries::*;
use crate::queries::is_dia_data;
use crate::rtree::*;

// ============================================================================
// MzDbReaderBuilder - Configurable Reader Construction
// ============================================================================

/// Temporary storage location for SQLite
#[derive(Clone, Copy, Debug, Default)]
pub enum TempStore {
    /// Use SQLite default (usually file-based)
    #[default]
    Default,
    /// Store temporary tables/indices in files
    File,
    /// Store temporary tables/indices in memory
    Memory,
}

/// Builder for configuring MzDbReader with SQLite optimizations
///
/// # Example
///
/// ```no_run
/// use mzdb::{MzDbReader, TempStore};
///
/// // For CLI tools processing a single file - maximize performance
/// let file_size = std::fs::metadata("file.mzDB").unwrap().len();
/// let reader = MzDbReader::builder("file.mzDB")
///     .read_only()
///     .mmap_size(file_size)
///     .temp_store(TempStore::Memory)
///     .build()
///     .unwrap();
///
/// // Auto-detect file size for mmap
/// let reader = MzDbReader::builder("file.mzDB")
///     .read_only()
///     .mmap_entire_file()
///     .build()
///     .unwrap();
/// ```
pub struct MzDbReaderBuilder {
    path: String,
    mmap_size: Option<u64>,
    read_only: bool,
    cache_size: Option<i64>,
    temp_store: TempStore,
}

impl MzDbReaderBuilder {
    /// Create a new builder for the given path
    pub fn new(path: impl Into<String>) -> Self {
        Self {
            path: path.into(),
            mmap_size: None,
            read_only: false,
            cache_size: None,
            temp_store: TempStore::Default,
        }
    }
    
    /// Set memory-mapped I/O size in bytes
    /// 
    /// Recommended: Set to file size for read-heavy workloads.
    /// SQLite recommends 256MB+ for general use.
    /// 
    /// When mmap is enabled, SQLite can access database pages directly
    /// from the memory-mapped region, eliminating a memory copy operation.
    /// Benchmarks show ~60% throughput improvement for blob reads.
    pub fn mmap_size(mut self, size: u64) -> Self {
        self.mmap_size = Some(size);
        self
    }
    
    /// Automatically set mmap_size to the file size
    /// 
    /// This maps the entire database file into memory, which provides
    /// optimal read performance when processing the complete file.
    pub fn mmap_entire_file(mut self) -> Self {
        if let Ok(metadata) = std::fs::metadata(&self.path) {
            self.mmap_size = Some(metadata.len());
        }
        self
    }
    
    /// Open database in read-only mode
    /// 
    /// Enables additional optimizations:
    /// - `query_only = ON` pragma
    /// - Opens with SQLITE_OPEN_READONLY flag
    /// 
    /// Use this for CLI tools and analysis pipelines that don't modify the file.
    pub fn read_only(mut self) -> Self {
        self.read_only = true;
        self
    }
    
    /// Set SQLite page cache size
    /// 
    /// - Positive value = number of pages (page size typically 4096 bytes)
    /// - Negative value = kibibytes (e.g., -400000 = ~400 MiB)
    /// 
    /// Note: When using mmap, the OS manages page caching, so this setting
    /// has less impact. Without mmap, a larger cache can improve performance.
    pub fn cache_size(mut self, size: i64) -> Self {
        self.cache_size = Some(size);
        self
    }
    
    /// Set temporary storage location
    /// 
    /// SQLite creates temporary tables and indices for some queries.
    /// Using `TempStore::Memory` can speed up complex queries.
    pub fn temp_store(mut self, store: TempStore) -> Self {
        self.temp_store = store;
        self
    }
    
    /// Build the MzDbReader with configured settings
    pub fn build(self) -> Result<MzDbReader> {
        let flags = if self.read_only {
            rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY
                | rusqlite::OpenFlags::SQLITE_OPEN_NO_MUTEX
        } else {
            rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE
                | rusqlite::OpenFlags::SQLITE_OPEN_NO_MUTEX
        };
        
        let connection = Connection::open_with_flags(&self.path, flags).dot()?;
        
        // Apply PRAGMAs
        if let Some(mmap_size) = self.mmap_size {
            connection.pragma_update(None, "mmap_size", mmap_size).dot()?;
        }
        
        if let Some(cache_size) = self.cache_size {
            connection.pragma_update(None, "cache_size", cache_size).dot()?;
        }
        
        match self.temp_store {
            TempStore::Default => {},
            TempStore::File => { connection.pragma_update(None, "temp_store", 1).dot()?; },
            TempStore::Memory => { connection.pragma_update(None, "temp_store", 2).dot()?; },
        }
        
        if self.read_only {
            connection.pragma_update(None, "query_only", true).dot()?;
        }
        
        let entity_cache = create_entity_cache(&connection).dot()?;
        
        Ok(MzDbReader { connection, entity_cache })
    }
}

// ============================================================================
// MzDbReader
// ============================================================================

/// Main entry point for reading mzDB files
///
/// The `MzDbReader` provides a high-level API for accessing all data in an mzDB file,
/// including spectra, chromatograms, metadata, and spatial queries.
///
/// # Example
///
/// ```no_run
/// use mzdb::MzDbReader;
///
/// let reader = MzDbReader::open("path/to/file.mzDB").unwrap();
///
/// // Basic info
/// println!("Version: {:?}", reader.get_version());
/// println!("Spectra: {}", reader.get_spectrum_count());
///
/// // Access metadata
/// if let Some(run) = reader.get_default_run().unwrap() {
///     println!("Run: {}", run.name);
/// }
/// ```
///
/// # Performance Optimization
///
/// For best performance, use the builder API with mmap enabled:
///
/// ```no_run
/// use mzdb::MzDbReader;
///
/// let reader = MzDbReader::builder("file.mzDB")
///     .read_only()
///     .mmap_entire_file()
///     .build()
///     .unwrap();
/// ```
pub struct MzDbReader {
    connection: Connection,
    entity_cache: EntityCache,
}

impl MzDbReader {
    // ========================================================================
    // Construction
    // ========================================================================

    /// Open an mzDB file for reading
    pub fn open(path: &str) -> Result<Self> {
        let connection = Connection::open(path).dot()?;
        let entity_cache = create_entity_cache(&connection).dot()?;
        Ok(Self {
            connection,
            entity_cache,
        })
    }

    /// Open an mzDB file with custom SQLite flags
    pub fn open_with_flags(path: &str, flags: rusqlite::OpenFlags) -> Result<Self> {
        let connection = Connection::open_with_flags(path, flags).dot()?;
        let entity_cache = create_entity_cache(&connection).dot()?;
        Ok(Self {
            connection,
            entity_cache,
        })
    }
    
    /// Create a builder for configuring the reader with SQLite optimizations
    ///
    /// # Example
    ///
    /// ```no_run
    /// use mzdb::MzDbReader;
    ///
    /// let reader = MzDbReader::builder("file.mzDB")
    ///     .read_only()
    ///     .mmap_entire_file()
    ///     .build()
    ///     .unwrap();
    /// ```
    pub fn builder(path: impl Into<String>) -> MzDbReaderBuilder {
        MzDbReaderBuilder::new(path)
    }

    // ========================================================================
    // File-level metadata
    // ========================================================================

    /// Get the mzDB format version
    pub fn get_version(&self) -> Result<Option<String>> {
        get_mzdb_version(&self.connection)
    }

    /// Get the pwiz-mzDB writer version
    pub fn get_writer_version(&self) -> Result<Option<String>> {
        get_pwiz_mzdb_version(&self.connection)
    }

    /// Get complete file metadata from the mzdb table
    pub fn get_file_metadata(&self) -> Result<Option<MzDbMetadata>> {
        get_mzdb_metadata(&self.connection)
    }

    /// Get the bounding box sizes
    pub fn get_bb_sizes(&self) -> &BBSizes {
        &self.entity_cache.bb_sizes
    }

    // ========================================================================
    // Spectrum access
    // ========================================================================

    /// Get all spectrum headers
    pub fn get_spectrum_headers(&self) -> &[SpectrumHeader] {
        &self.entity_cache.spectrum_headers
    }

    /// Get the total number of spectra
    pub fn get_spectrum_count(&self) -> usize {
        self.entity_cache.spectrum_headers.len()
    }

    /// Get a spectrum by ID
    pub fn get_spectrum(&self, spectrum_id: i64) -> Result<Spectrum> {
        get_spectrum(&self.connection, spectrum_id, &self.entity_cache)
    }

    /// Iterate over all spectra using a callback function
    ///
    /// This is a callback-based approach that processes each spectrum in order.
    /// For a more idiomatic iterator-based approach, use `iter_spectra()`.
    ///
    /// # Arguments
    /// * `ms_level` - Optional MS level filter (e.g., Some(1) for MS1 only, None for all levels)
    /// * `on_each_spectrum` - Callback function called for each spectrum
    ///
    /// # Example
    /// ```no_run
    /// use mzdb::MzDbReader;
    ///
    /// let reader = MzDbReader::open("file.mzDB").unwrap();
    /// reader.for_each_spectrum(Some(1), |spectrum| {
    ///     println!("MS1 spectrum: {}", spectrum.header.id);
    ///     Ok(())
    /// }).unwrap();
    /// ```
    pub fn for_each_spectrum<F>(&self, ms_level: Option<u8>, on_each_spectrum: F) -> Result<()>
    where
        F: FnMut(&Spectrum) -> Result<()>,
    {
        iterator_for_each_spectrum(&self.connection, &self.entity_cache, ms_level, on_each_spectrum)
    }

    /// Iterate over all spectra using a fallible iterator
    ///
    /// This returns a fallible iterator that yields spectra. This is a more idiomatic
    /// Rust approach compared to the callback-based `for_each_spectrum()`.
    ///
    /// # Arguments
    /// * `ms_level` - Optional MS level filter (e.g., Some(1) for MS1 only, None for all levels)
    ///
    /// # Example
    /// ```no_run
    /// use mzdb::MzDbReader;
    /// use fallible_iterator::FallibleIterator;
    ///
    /// let reader = MzDbReader::open("file.mzDB").unwrap();
    /// let mut iter = reader.iter_spectra(Some(1)).unwrap();
    ///
    /// while let Some(spectrum) = iter.next().unwrap() {
    ///     println!("MS1 spectrum: {}", spectrum.header.id);
    /// }
    /// ```
    pub fn iter_spectra(&self, ms_level: Option<u8>) -> Result<SpectrumIterator<'_>> {
        SpectrumIterator::new(&self.connection, &self.entity_cache, ms_level)
    }

    /// Get the maximum MS level in the file
    pub fn get_max_ms_level(&self) -> Result<Option<i64>> {
        get_max_ms_level(&self.connection)
    }

    /// Get the count of spectra at a specific MS level
    pub fn get_spectra_count_by_ms_level(&self, ms_level: i64) -> Result<Option<i64>> {
        get_spectra_count_by_ms_level(&self.connection, ms_level)
    }

    /// Get the last retention time in the file
    pub fn get_last_time(&self) -> Result<Option<f32>> {
        get_last_time(&self.connection)
    }

    /// Get the last cycle number in the file
    pub fn get_last_cycle_number(&self) -> Result<Option<i64>> {
        get_last_cycle_number(&self.connection)
    }

    // ========================================================================
    // XIC generation
    // ========================================================================

    /// Get an extracted ion chromatogram (XIC)
    pub fn get_xic(
        &self,
        mz: f32,
        mz_tol_ppm: f32,
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

    // ========================================================================
    // DIA (Data Independent Acquisition) support
    // ========================================================================

    /// Get all MS2 spectra for a specific isolation window (by precursor m/z)
    ///
    /// This method retrieves all MS2 spectra that fall within a specified isolation
    /// window, which is useful for DIA data processing where spectra are grouped
    /// by their precursor m/z range.
    ///
    /// # Arguments
    /// * `window_mz` - The center m/z of the isolation window
    /// * `mz_tolerance` - The m/z tolerance for matching (default: 0.5 Da)
    ///
    /// # Example
    /// ```no_run
    /// use mzdb::MzDbReader;
    ///
    /// let reader = MzDbReader::open("dia_file.mzDB").unwrap();
    /// let spectra = reader.get_ms2_spectra_for_isolation_window(500.0, 0.5).unwrap();
    /// println!("Found {} MS2 spectra in window 500.0 m/z", spectra.len());
    /// ```
    pub fn get_ms2_spectra_for_isolation_window(
        &self,
        window_mz: f64,
        mz_tolerance: f64,
    ) -> Result<Vec<Spectrum>> {
        let min_mz = window_mz - mz_tolerance;
        let max_mz = window_mz + mz_tolerance;
        
        let mut spectra = Vec::new();
        
        for header in self.get_spectrum_headers() {
            if header.ms_level == 2 {
                if let Some(prec_mz) = header.precursor_mz {
                    if prec_mz >= min_mz && prec_mz <= max_mz {
                        if let Ok(spectrum) = self.get_spectrum(header.id) {
                            spectra.push(spectrum);
                        }
                    }
                }
            }
        }
        
        // Sort by retention time for proper chromatographic processing
        spectra.sort_by(|a, b| {
            a.header.time.partial_cmp(&b.header.time)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        
        Ok(spectra)
    }

    /// Get MS2 spectra for a DIA isolation window using efficient SQL filtering
    ///
    /// This method is much faster than `get_ms2_spectra_for_isolation_window` because
    /// it uses SQL to filter by `main_precursor_mz` directly, avoiding individual
    /// spectrum queries.
    ///
    /// # Arguments
    /// * `main_precursor_mz` - The target precursor m/z value for the isolation window
    /// * `precursor_mz_tol` - Optional m/z tolerance in Daltons (default: 0.1)
    ///
    /// # Returns
    /// A vector of spectra sorted by retention time
    ///
    /// # Example
    /// ```no_run
    /// use mzdb::MzDbReader;
    ///
    /// let reader = MzDbReader::open("dia_file.mzDB").unwrap();
    /// // Use default 0.1 Da tolerance
    /// let spectra = reader.get_dia_spectra_for_window(500.0, None).unwrap();
    /// println!("Found {} MS2 spectra in window 500.0 m/z", spectra.len());
    /// ```
    pub fn get_dia_spectra_for_window(
        &self,
        main_precursor_mz: f64,
        precursor_mz_tol: Option<f64>,
    ) -> Result<Vec<Spectrum>> {
        crate::iterator::collect_dia_spectra(&self.connection, &self.entity_cache, main_precursor_mz, precursor_mz_tol)
    }

    /// Iterate over MS2 spectra for a DIA isolation window
    ///
    /// Returns a streaming iterator that efficiently loads spectra on-demand
    /// using tolerance-based filtering by `main_precursor_mz`. This is more memory-efficient
    /// than `get_dia_spectra_for_window` for large isolation windows.
    ///
    /// # Arguments
    /// * `main_precursor_mz` - The target precursor m/z value for the isolation window
    /// * `precursor_mz_tol` - Optional m/z tolerance in Daltons (default: 0.1)
    ///
    /// # Example
    /// ```no_run
    /// use mzdb::MzDbReader;
    /// use fallible_iterator::FallibleIterator;
    ///
    /// let reader = MzDbReader::open("dia_file.mzDB").unwrap();
    /// // Use default 0.1 Da tolerance
    /// let mut iter = reader.iter_dia_spectra(500.0, None).unwrap();
    /// while let Some(spectrum) = iter.next().unwrap() {
    ///     println!("Spectrum: {} at RT {:.2}", spectrum.header.id, spectrum.header.time);
    /// }
    /// ```
    pub fn iter_dia_spectra(
        &self,
        main_precursor_mz: f64,
        precursor_mz_tol: Option<f64>,
    ) -> Result<SpectrumIterator<'_>> {
        SpectrumIterator::new_dia(&self.connection, &self.entity_cache, main_precursor_mz, precursor_mz_tol)
    }

    /// Iterate over MS2 spectra for a DIA isolation window using a callback
    ///
    /// Uses efficient tolerance-based filtering by `main_precursor_mz`.
    /// Spectra are yielded in retention time order.
    ///
    /// # Arguments
    /// * `main_precursor_mz` - The target precursor m/z value for the isolation window
    /// * `precursor_mz_tol` - Optional m/z tolerance in Daltons (default: 0.1)
    /// * `on_each_spectrum` - Callback function called for each spectrum
    pub fn for_each_dia_spectrum<F>(
        &self,
        main_precursor_mz: f64,
        precursor_mz_tol: Option<f64>,
        on_each_spectrum: F,
    ) -> Result<()>
    where
        F: FnMut(&Spectrum) -> Result<()>,
    {
        crate::iterator::for_each_dia_spectrum(
            &self.connection,
            &self.entity_cache,
            main_precursor_mz,
            precursor_mz_tol,
            on_each_spectrum,
        )
    }

    /// Get all unique isolation windows (precursor m/z values) for MS2 spectra
    ///
    /// This is useful for discovering all DIA windows in a file before processing.
    /// Windows are grouped by rounding to 0.1 m/z.
    pub fn get_isolation_windows(&self) -> Vec<f64> {
        use std::collections::BTreeSet;
        
        let mut windows: BTreeSet<i64> = BTreeSet::new();
        for header in self.get_spectrum_headers() {
            if header.ms_level == 2 {
                if let Some(prec_mz) = header.precursor_mz {
                    // Round to 1 decimal place for grouping
                    let window_key = (prec_mz * 10.0).round() as i64;
                    windows.insert(window_key);
                }
            }
        }
        
        windows.into_iter().map(|k| k as f64 / 10.0).collect()
    }

    /// Get all unique isolation windows with their actual bounds from XML metadata
    ///
    /// Returns a vector of (target_mz, lower_bound, upper_bound) tuples.
    /// This parses the isolation window offsets from the precursor_list XML metadata
    /// to get the exact window bounds rather than approximations.
    ///
    /// # Example
    /// ```no_run
    /// use mzdb::MzDbReader;
    ///
    /// let reader = MzDbReader::open("dia_file.mzDB").unwrap();
    /// for (target_mz, lower, upper) in reader.get_isolation_windows_with_bounds() {
    ///     println!("Window: {:.4} Da, range [{:.4}, {:.4}]", target_mz, lower, upper);
    /// }
    /// ```
    pub fn get_isolation_windows_with_bounds(&self) -> Vec<(f64, f64, f64)> {
        use std::collections::HashMap;
        use crate::metadata::parse_isolation_window_offsets_from_xml;
        
        let mut windows: HashMap<i64, (f64, f64, f64)> = HashMap::new();
        
        for header in self.get_spectrum_headers() {
            if header.ms_level == 2 {
                if let Some(prec_mz) = header.precursor_mz {
                    let window_key = (prec_mz * 10.0).round() as i64;
                    
                    // Skip if we already have this window
                    if windows.contains_key(&window_key) {
                        continue;
                    }
                    
                    // Try to parse isolation window offsets from XML
                    let (lower_offset, upper_offset) = if let Some(ref xml) = header.precursor_list_str {
                        parse_isolation_window_offsets_from_xml(xml)
                    } else {
                        (None, None)
                    };
                    
                    let (lower_bound, upper_bound) = match (lower_offset, upper_offset) {
                        (Some(lo), Some(uo)) => {
                            // Use actual offsets from XML
                            (prec_mz - lo, prec_mz + uo)
                        }
                        _ => {
                            // Fallback: assume ±2 Da (common for 4 Da windows)
                            (prec_mz - 2.0, prec_mz + 2.0)
                        }
                    };
                    
                    windows.insert(window_key, (prec_mz, lower_bound, upper_bound));
                }
            }
        }
        
        let mut result: Vec<_> = windows.into_values().collect();
        result.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap());
        result
    }

    // ========================================================================
    // Chromatogram access
    // ========================================================================

    /// List all chromatogram headers
    pub fn list_chromatograms(&self) -> Result<Vec<ChromatogramHeader>> {
        list_chromatograms(&self.connection)
    }

    /// Get the number of chromatograms
    pub fn get_chromatogram_count(&self) -> Result<i64> {
        get_chromatogram_count(&self.connection)
    }

    /// Get a complete chromatogram by ID
    pub fn get_chromatogram(&self, id: i64) -> Result<Chromatogram> {
        get_chromatogram(&self.connection, id)
    }

    /// Get a chromatogram by name
    pub fn get_chromatogram_by_name(&self, name: &str) -> Result<Option<ChromatogramHeader>> {
        get_chromatogram_by_name(&self.connection, name)
    }

    /// Get the TIC chromatogram
    pub fn get_tic(&self) -> Result<Option<Chromatogram>> {
        if let Some(header) = get_tic_chromatogram(&self.connection)? {
            let data = get_chromatogram_data(&self.connection, header.id)?;
            Ok(Some(Chromatogram { header, data }))
        } else {
            Ok(None)
        }
    }

    /// List SRM chromatograms
    pub fn list_srm_chromatograms(&self) -> Result<Vec<ChromatogramHeader>> {
        list_srm_chromatograms(&self.connection)
    }

    // ========================================================================
    // Run and sample metadata
    // ========================================================================

    /// List all runs
    pub fn list_runs(&self) -> Result<Vec<Run>> {
        list_runs(&self.connection)
    }

    /// Get a specific run by ID
    pub fn get_run(&self, id: i64) -> Result<Option<Run>> {
        get_run(&self.connection, id)
    }

    /// Get the default (first) run
    pub fn get_default_run(&self) -> Result<Option<Run>> {
        let runs = list_runs(&self.connection)?;
        Ok(runs.into_iter().next())
    }

    /// List all samples
    pub fn list_samples(&self) -> Result<Vec<Sample>> {
        list_samples(&self.connection)
    }

    /// Get a specific sample by ID
    pub fn get_sample(&self, id: i64) -> Result<Option<Sample>> {
        get_sample(&self.connection, id)
    }

    // ========================================================================
    // Software and instrument metadata
    // ========================================================================

    /// List all software entries
    pub fn list_software(&self) -> Result<Vec<Software>> {
        list_software(&self.connection)
    }

    /// Get a specific software entry by ID
    pub fn get_software(&self, id: i64) -> Result<Option<Software>> {
        get_software(&self.connection, id)
    }

    /// Get software by name pattern
    pub fn get_software_by_name(&self, name_pattern: &str) -> Result<Option<Software>> {
        get_software_by_name(&self.connection, name_pattern)
    }

    /// List all instrument configurations
    pub fn list_instrument_configurations(&self) -> Result<Vec<InstrumentConfiguration>> {
        list_instrument_configurations(&self.connection)
    }

    /// Get a specific instrument configuration by ID
    pub fn get_instrument_configuration(&self, id: i64) -> Result<Option<InstrumentConfiguration>> {
        get_instrument_configuration(&self.connection, id)
    }

    // ========================================================================
    // Source files
    // ========================================================================

    /// List all source files
    pub fn list_source_files(&self) -> Result<Vec<SourceFile>> {
        list_source_files(&self.connection)
    }

    /// Get a specific source file by ID
    pub fn get_source_file(&self, id: i64) -> Result<Option<SourceFile>> {
        get_source_file(&self.connection, id)
    }

    // ========================================================================
    // Data processing
    // ========================================================================

    /// List all data processing workflows
    pub fn list_data_processings(&self) -> Result<Vec<DataProcessing>> {
        list_data_processings(&self.connection)
    }

    /// Get a specific data processing by ID
    pub fn get_data_processing(&self, id: i64) -> Result<Option<DataProcessing>> {
        get_data_processing(&self.connection, id)
    }

    /// List all processing methods
    pub fn list_processing_methods(&self) -> Result<Vec<ProcessingMethod>> {
        list_processing_methods(&self.connection)
    }

    /// Get processing methods for a specific workflow
    pub fn get_processing_methods_for_workflow(&self, data_processing_id: i64) -> Result<Vec<ProcessingMethod>> {
        get_processing_methods_for_workflow(&self.connection, data_processing_id)
    }

    // ========================================================================
    // Scan settings and targets
    // ========================================================================

    /// List all scan settings
    pub fn list_scan_settings(&self) -> Result<Vec<ScanSettings>> {
        list_scan_settings(&self.connection)
    }

    /// Get specific scan settings by ID
    pub fn get_scan_settings(&self, id: i64) -> Result<Option<ScanSettings>> {
        get_scan_settings(&self.connection, id)
    }

    /// List all targets (inclusion list)
    pub fn list_targets(&self) -> Result<Vec<Target>> {
        list_targets(&self.connection)
    }

    /// Get targets for specific scan settings
    pub fn get_targets_for_scan_settings(&self, scan_settings_id: i64) -> Result<Vec<Target>> {
        get_targets_for_scan_settings(&self.connection, scan_settings_id)
    }

    // ========================================================================
    // Controlled vocabularies
    // ========================================================================

    /// List all controlled vocabularies
    pub fn list_controlled_vocabularies(&self) -> Result<Vec<ControlledVocabulary>> {
        list_controlled_vocabularies(&self.connection)
    }

    /// Get a specific controlled vocabulary by ID
    pub fn get_controlled_vocabulary(&self, id: &str) -> Result<Option<ControlledVocabulary>> {
        get_controlled_vocabulary(&self.connection, id)
    }

    /// List all CV terms
    pub fn list_cv_terms(&self) -> Result<Vec<CvTerm>> {
        list_cv_terms(&self.connection)
    }

    /// Get a specific CV term by accession
    pub fn get_cv_term(&self, accession: &str) -> Result<Option<CvTerm>> {
        get_cv_term(&self.connection, accession)
    }

    /// Search CV terms by name
    pub fn search_cv_terms(&self, name_pattern: &str) -> Result<Vec<CvTerm>> {
        search_cv_terms(&self.connection, name_pattern)
    }

    /// List all CV units
    pub fn list_cv_units(&self) -> Result<Vec<CvUnit>> {
        list_cv_units(&self.connection)
    }

    /// List all user-defined terms
    pub fn list_user_terms(&self) -> Result<Vec<UserTerm>> {
        list_user_terms(&self.connection)
    }

    /// Get a specific user term by ID
    pub fn get_user_term(&self, id: i64) -> Result<Option<UserTerm>> {
        get_user_term(&self.connection, id)
    }

    // ========================================================================
    // Shared parameter trees and schemas
    // ========================================================================

    /// List all shared parameter trees
    pub fn list_shared_param_trees(&self) -> Result<Vec<SharedParamTree>> {
        list_shared_param_trees(&self.connection)
    }

    /// Get a specific shared parameter tree by ID
    pub fn get_shared_param_tree(&self, id: i64) -> Result<Option<SharedParamTree>> {
        get_shared_param_tree(&self.connection, id)
    }

    /// List all parameter tree schemas
    pub fn list_param_tree_schemas(&self) -> Result<Vec<ParamTreeSchema>> {
        list_param_tree_schemas(&self.connection)
    }

    /// Get a specific parameter tree schema by name
    pub fn get_param_tree_schema(&self, name: &str) -> Result<Option<ParamTreeSchema>> {
        get_param_tree_schema(&self.connection, name)
    }

    /// List table to schema mappings
    pub fn list_table_param_tree_schemas(&self) -> Result<Vec<TableParamTreeSchema>> {
        list_table_param_tree_schemas(&self.connection)
    }

    /// Get the schema name for a specific table
    pub fn get_schema_for_table(&self, table_name: &str) -> Result<Option<String>> {
        get_schema_for_table(&self.connection, table_name)
    }

    // ========================================================================
    // R-tree spatial queries
    // ========================================================================

    /// Check if R-tree index is available
    pub fn has_rtree(&self) -> Result<bool> {
        has_rtree(&self.connection)
    }

    /// Check if MSn R-tree index is available (for DIA)
    pub fn has_msn_rtree(&self) -> Result<bool> {
        has_msn_rtree(&self.connection)
    }

    /// Check if the file appears to be DIA data
    ///
    /// DIA data typically has many MS2 spectra with similar precursor m/z patterns
    /// and an MSn R-tree index.
    pub fn is_dia(&self) -> Result<bool> {
        is_dia_data(&self.connection)
    }

    /// Get R-tree statistics
    pub fn get_rtree_stats(&self) -> Result<Option<RTreeStats>> {
        get_rtree_stats(&self.connection)
    }

    /// Query bounding boxes in an m/z range
    pub fn query_bounding_boxes_in_mz_range(
        &self,
        min_mz: f64,
        max_mz: f64,
    ) -> Result<Vec<BoundingBoxRTreeEntry>> {
        query_bounding_boxes_in_mz_range(&self.connection, min_mz, max_mz)
    }

    /// Query bounding boxes at a specific m/z with ppm tolerance
    pub fn query_bounding_boxes_at_mz_ppm(
        &self,
        mz: f64,
        ppm_tolerance: f64,
    ) -> Result<Vec<BoundingBoxRTreeEntry>> {
        query_bounding_boxes_at_mz_ppm(&self.connection, mz, ppm_tolerance)
    }

    /// Query bounding boxes in a time range
    pub fn query_bounding_boxes_in_time_range(
        &self,
        min_time: f64,
        max_time: f64,
    ) -> Result<Vec<BoundingBoxRTreeEntry>> {
        query_bounding_boxes_in_time_range(&self.connection, min_time, max_time)
    }

    /// Query bounding boxes in a 2D region (m/z x time)
    pub fn query_bounding_boxes_in_region(
        &self,
        min_mz: f64,
        max_mz: f64,
        min_time: f64,
        max_time: f64,
    ) -> Result<Vec<BoundingBoxRTreeEntry>> {
        query_bounding_boxes_in_region(&self.connection, min_mz, max_mz, min_time, max_time)
    }

    /// Query bounding boxes in a region with ppm tolerance
    pub fn query_bounding_boxes_in_region_ppm(
        &self,
        mz: f64,
        ppm_tolerance: f64,
        min_time: f64,
        max_time: f64,
    ) -> Result<Vec<BoundingBoxRTreeEntry>> {
        query_bounding_boxes_in_region_ppm(&self.connection, mz, ppm_tolerance, min_time, max_time)
    }

    /// Query bounding boxes containing a specific point
    pub fn query_bounding_boxes_containing_point(
        &self,
        mz: f64,
        time: f64,
    ) -> Result<Vec<BoundingBoxRTreeEntry>> {
        query_bounding_boxes_containing_point(&self.connection, mz, time)
    }

    /// Query MSn bounding boxes for DIA (by parent m/z)
    pub fn query_msn_bounding_boxes_for_dia(
        &self,
        ms_level: i64,
        parent_mz: f64,
        parent_mz_tolerance: f64,
    ) -> Result<Vec<BoundingBoxMsnRTreeEntry>> {
        query_msn_bounding_boxes_for_dia(&self.connection, ms_level, parent_mz, parent_mz_tolerance)
    }

    /// Query MSn bounding boxes in a region
    pub fn query_msn_bounding_boxes_in_region(
        &self,
        ms_level: i64,
        min_mz: f64,
        max_mz: f64,
        min_time: f64,
        max_time: f64,
    ) -> Result<Vec<BoundingBoxMsnRTreeEntry>> {
        query_msn_bounding_boxes_in_region(&self.connection, ms_level, min_mz, max_mz, min_time, max_time)
    }

    /// Get unique parent m/z windows from the MSn R-tree (for DIA/SWATH data)
    pub fn get_parent_mz_windows(&self) -> Result<Vec<(f64, f64)>> {
        get_parent_mz_windows(&self.connection)
    }

    /// Get the R-tree entry for a specific bounding box
    pub fn get_bounding_box_rtree_entry(&self, bb_id: i64) -> Result<Option<BoundingBoxRTreeEntry>> {
        get_bounding_box_rtree_entry(&self.connection, bb_id)
    }

    /// Get the minimum m/z of a bounding box from R-tree
    pub fn get_bounding_box_min_mz(&self, bb_id: i64) -> Result<Option<f64>> {
        get_bounding_box_min_mz(&self.connection, bb_id)
    }

    /// Get the maximum m/z of a bounding box from R-tree
    pub fn get_bounding_box_max_mz(&self, bb_id: i64) -> Result<Option<f64>> {
        get_bounding_box_max_mz(&self.connection, bb_id)
    }

    /// Get the minimum time of a bounding box from R-tree
    pub fn get_bounding_box_min_time(&self, bb_id: i64) -> Result<Option<f64>> {
        get_bounding_box_min_time(&self.connection, bb_id)
    }

    /// Get the maximum time of a bounding box from R-tree
    pub fn get_bounding_box_max_time(&self, bb_id: i64) -> Result<Option<f64>> {
        get_bounding_box_max_time(&self.connection, bb_id)
    }

    /// Get the MSn R-tree entry for a specific bounding box
    pub fn get_bounding_box_msn_rtree_entry(&self, bb_id: i64) -> Result<Option<BoundingBoxMsnRTreeEntry>> {
        get_bounding_box_msn_rtree_entry(&self.connection, bb_id)
    }

    // ========================================================================
    // Advanced access
    // ========================================================================

    /// Get access to the underlying SQLite connection for advanced queries
    pub fn connection(&self) -> &Connection {
        &self.connection
    }

    /// Get access to the entity cache for advanced queries
    pub fn entity_cache(&self) -> &EntityCache {
        &self.entity_cache
    }

    /// Get source file to scan settings mappings
    pub fn list_source_file_scan_settings_maps(&self) -> Result<Vec<SourceFileScanSettingsMap>> {
        list_source_file_scan_settings_maps(&self.connection)
    }
}
