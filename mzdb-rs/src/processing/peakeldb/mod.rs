//! PeakelDB - Peakel Database utilities for LC-MS data
//!
//! This module provides utilities for creating and reading peakelDB SQLite databases
//! that store detected peakels from mzDB files.
//!
//! # Database Variants
//!
//! There are two peakelDB schema variants:
//!
//! - **MS1 PeakelDB** (`ms1` module): Legacy format for MS1 peakels with lcms_map structure
//! - **MS2 DIA PeakelDB** (`ms2` module): Format for DIA MS2 peakels with isolation windows
//!
//! # Common Types
//!
//! The `common` module provides shared types:
//! - `PeakelData`: Raw peaks data arrays (spectrum_ids, elution_times, mz_values, intensities)
//! - `ExtendedPeakel`: Complete peakel with summary fields + raw data
//!
//! # Example
//!
//! ```no_run
//! use mzdb::processing::peakeldb::{Ms2PeakelDbReader, ExtendedPeakel, PeakelData};
//!
//! // Read MS2 DIA peakeldb
//! let reader = Ms2PeakelDbReader::open("peakels.peakeldb").unwrap();
//! let windows = reader.read_isolation_windows().unwrap();
//! let peakels = reader.read_all_peakels().unwrap();
//! ```

pub mod common;
pub mod ms1;
pub mod ms2;

// Re-export common types
pub use common::{
    PeakelData, ExtendedPeakel,
    parse_peaks_blob, parse_peaks_blob_to_peakel_data,
    extract_i64_array, extract_f32_array, extract_f64_array,
    chrono_lite_timestamp,
};

// Re-export the trait from the model module
pub use crate::processing::model::HasPeakelData;

pub use ms1::{
    Ms1PeakelDbWriter, Ms1PeakelDbSchema, Ms1PeakelRecord,
    serialize_ms1_peakel_data,
    write_ms1_peakels_tsv, print_ms1_statistics,
};

pub use ms2::{
    Ms2PeakelDbReader, Ms2PeakelDbWriter, Ms2PeakelDbSchema,
    SimplifierPeakel,
    print_ms2_statistics,
};
