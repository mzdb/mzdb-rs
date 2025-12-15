//! mzdb-rs: A comprehensive Rust library for reading mzDB mass spectrometry files
//!
//! This library provides full support for the mzDB 0.7.0 file format specification,
//! including all metadata tables, chromatograms, and spatial index queries.
//!
//! # Features
//!
//! - **Spectrum Access**: Read individual spectra or iterate over all spectra
//! - **Chromatograms**: Access TIC, SRM, and other chromatogram types
//! - **XIC Generation**: Extract ion chromatograms for specific m/z values
//! - **Metadata**: Full access to all metadata tables (samples, instruments, software, etc.)
//! - **R-tree Queries**: Efficient spatial queries using SQLite R-tree indices
//! - **DIA/SWATH Support**: Access MSn data with parent m/z filtering
//!
//! # Quick Start
//!
//! ```no_run
//! use mzdb::MzDbReader;
//!
//! let reader = MzDbReader::open("path/to/file.mzDB").unwrap();
//!
//! // Get file metadata
//! println!("mzDB version: {:?}", reader.get_version());
//!
//! // Iterate over spectra
//! for spectrum in reader.iter_spectra(None).unwrap() {
//!     println!("Spectrum {}: {} peaks", spectrum.header.id, spectrum.data.peaks_count);
//! }
//!
//! // Get chromatograms
//! for chrom in reader.list_chromatograms().unwrap() {
//!     println!("Chromatogram: {}", chrom.name);
//! }
//! ```
//!
//! # Module Organization
//!
//! - [`model`]: Core data structures (Spectrum, DataEncoding, etc.)
//! - [`queries`]: Low-level database query functions
//! - [`iterator`]: Efficient iteration utilities
//! - [`metadata`]: Metadata table structures and queries
//! - [`chromatogram`]: Chromatogram structures and queries
//! - [`rtree`]: R-tree spatial index queries
//! - [`cache`]: Query caching utilities
//! - [`mzdb`]: Core mzDB operations

pub mod cache;
pub mod chromatogram;
pub mod iterator;
pub mod metadata;
pub mod model;
pub mod mzdb;
pub mod queries;
pub mod queries_extended;
pub mod query_utils;
pub mod rtree;

// Re-export main types for convenience
pub use model::{
    AcquisitionMode, BBSizes, BoundingBox, BoundingBoxIndex, ByteOrder, DataEncoding,
    DataEncodingsCache, DataMode, DataPrecision, EntityCache, IsolationWindow, PeakEncoding,
    Spectrum, SpectrumData, SpectrumHeader, SpectrumSlice, XicMethod, XicPeak,
    RunSlice, RunSliceHeader, RunSliceData,
};

// Re-export chromatogram types
pub use chromatogram::{
    Chromatogram, ChromatogramData, ChromatogramHeader, ChromatogramType,
};

// Re-export metadata types
pub use metadata::{
    ControlledVocabulary, CvTerm, CvUnit, DataProcessing, InstrumentConfiguration,
    MzDbMetadata, ParamTreeSchema, ProcessingMethod, Run, Sample, ScanSettings,
    SharedParamTree, Software, SourceFile, SourceFileScanSettingsMap, 
    TableParamTreeSchema, Target, UserTerm,
};

// Re-export rtree types
pub use rtree::{
    BoundingBoxMsnRTreeEntry, BoundingBoxRTreeEntry, RTreeStats,
};

// Re-export queries_extended types and functions
pub use queries_extended::MzDbStats;

// Re-export query utility functions
pub use query_utils::{
    get_table_records_count, get_table_count_exact, table_exists,
    query_single_i64, query_single_i64_required, query_single_f32, query_single_f64,
    query_single_string, query_all_strings,
    query_single_i64_with_params, query_single_f64_with_params, query_single_string_with_params,
};

use anyhow_ext::{Context, Result};
use rusqlite::Connection;

use crate::chromatogram::{
    get_chromatogram, get_chromatogram_by_name, get_chromatogram_count,
    get_chromatogram_data, get_tic_chromatogram, list_chromatograms, list_srm_chromatograms,
};
use crate::iterator::for_each_spectrum;
use crate::metadata::{
    get_controlled_vocabulary, get_cv_term, get_data_processing, get_instrument_configuration,
    get_mzdb_metadata, get_param_tree_schema, get_run, get_sample, get_scan_settings,
    get_schema_for_table, get_shared_param_tree, get_software, get_software_by_name,
    get_source_file, get_user_term, list_controlled_vocabularies, list_cv_terms, list_cv_units,
    list_data_processings, list_instrument_configurations, list_param_tree_schemas,
    list_processing_methods, list_runs, list_samples, list_scan_settings,
    list_shared_param_trees, list_software, list_source_files,
    list_table_param_tree_schemas, list_targets, list_user_terms,
    get_processing_methods_for_workflow, get_targets_for_scan_settings,
    list_source_file_scan_settings_maps, search_cv_terms,
};
use crate::mzdb::create_entity_cache;
use crate::queries::{
    get_ms_xic, get_mzdb_version, get_pwiz_mzdb_version, get_spectrum,
    get_max_ms_level, get_spectra_count_by_ms_level, get_last_time, get_last_cycle_number,
};
use crate::rtree::{
    get_rtree_stats, get_parent_mz_windows, has_msn_rtree, has_rtree,
    query_bounding_boxes_at_mz_ppm, query_bounding_boxes_containing_point,
    query_bounding_boxes_in_mz_range, query_bounding_boxes_in_region,
    query_bounding_boxes_in_region_ppm, query_bounding_boxes_in_time_range,
    query_msn_bounding_boxes_for_dia, query_msn_bounding_boxes_in_region,
    get_bounding_box_rtree_entry, get_bounding_box_min_mz, get_bounding_box_max_mz,
    get_bounding_box_min_time, get_bounding_box_max_time, get_bounding_box_msn_rtree_entry,
};

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

    /// Iterate over all spectra, optionally filtering by MS level
    pub fn iter_spectra(&self, ms_level: Option<u8>) -> Result<Vec<Spectrum>> {
        let mut spectra = Vec::new();
        for_each_spectrum(&self.connection, &self.entity_cache, ms_level, |s| {
            spectra.push(s.clone());
            Ok(())
        })?;
        Ok(spectra)
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

#[cfg(test)]
mod tests {
    use super::*;
    
    // Basic compile-time tests to ensure the API is consistent
    #[test]
    fn test_exports() {
        // Just verify types are accessible
        let _: Option<Spectrum> = None;
        let _: Option<Chromatogram> = None;
        let _: Option<Run> = None;
        let _: Option<Sample> = None;
        let _: Option<Software> = None;
        let _: Option<InstrumentConfiguration> = None;
        let _: Option<DataProcessing> = None;
        let _: Option<ProcessingMethod> = None;
        let _: Option<ControlledVocabulary> = None;
        let _: Option<CvTerm> = None;
        let _: Option<CvUnit> = None;
        let _: Option<UserTerm> = None;
        let _: Option<BoundingBoxRTreeEntry> = None;
        let _: Option<BoundingBoxMsnRTreeEntry> = None;
    }
}
