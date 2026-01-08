//! Thermo RAW to mzDB Converter
//!
//! This module provides functionality to convert Thermo Fisher Scientific RAW files
//! directly to mzDB format using the thernio library.
//!
//! # Features
//!
//! - Direct RAW file reading without intermediate conversions
//! - Automatic DIA/DDA acquisition mode detection
//! - XML metadata generation from RAW file properties
//! - Efficient spectrum data conversion
//! - Support for MS1 and MSn data
//!
//! # Example
//!
//! ```no_run
//! use mzdb::writer::thermo::convert_raw_to_mzdb;
//! use mzdb::BBSizes;
//!
//! let bb_sizes = BBSizes {
//!     bb_mz_height_ms1: 10.0,
//!     bb_mz_height_msn: 10000.0,
//!     bb_rt_width_ms1: 5.0,
//!     bb_rt_width_msn: 60.0,
//! };
//!
//! // Acquisition mode (DIA vs DDA) is auto-detected from the RAW file
//! convert_raw_to_mzdb(
//!     "input.raw",
//!     "output.mzDB",
//!     bb_sizes,
//! ).unwrap();
//! ```

mod xml_builder;
mod converter;

pub use converter::convert_raw_to_mzdb;
