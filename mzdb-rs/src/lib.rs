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
//! use fallible_iterator::FallibleIterator;
//!
//! let reader = MzDbReader::open("path/to/file.mzDB").unwrap();
//!
//! // Get file metadata
//! println!("mzDB version: {:?}", reader.get_version());
//!
//! // Iterate over spectra
//! let mut iter = reader.iter_spectra(None).unwrap();
//! while let Some(spectrum) = iter.next().unwrap() {
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
//! - [`xml`]: XML parsing for param_tree, scan_list, precursor_list, etc.
//! - [`cache`]: Query caching utilities
//! - [`mzdb`]: Core mzDB operations
//! - [`query_utils`]: Database query helper functions

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
pub mod xml;

#[cfg(feature = "writer")]
pub mod writer;

// Re-export main types for convenience
pub use model::{
    AcquisitionMode, BBSizes, BoundingBox, BoundingBoxIndex, ByteOrder, DataEncoding,
    DataEncodingsCache, DataMode, DataPrecision, EntityCache, MzRange, PeakEncoding,
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

// Re-export xml types and parsing functions
pub use xml::{
    // Core parameter types
    CvParam, UserParam, UserText, ParamTree,
    // Structured XML types
    FileContent, ComponentList, InstrumentComponent,
    ScanList, Scan, ScanWindow,
    PrecursorList, Precursor, IsolationWindow as XmlIsolationWindow, 
    SelectedIon, Activation,
    ProductList, Product,
    // Parsing functions
    parse_param_tree, parse_file_content, parse_component_list,
    parse_scan_list, parse_precursor_list, parse_product_list,
    // Convenience extraction functions
    extract_isolation_window, extract_selected_ion_mz, extract_collision_energy,
    extract_scan_time, find_param_value, find_user_param_value, find_user_text,
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

// Re-export MzDbReader from mzdb module
pub use crate::mzdb::MzDbReader;


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
