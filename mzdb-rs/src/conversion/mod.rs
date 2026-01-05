//! Conversion utilities for mzDB files
//!
//! This module provides converters to export mzDB data to various formats:
//! - MGF (Mascot Generic Format) for MS/MS search engines

pub mod mgf;

pub use mgf::{MgfWriter, MgfWriterWithStats, MgfExportOptions, MgfExportStats};
