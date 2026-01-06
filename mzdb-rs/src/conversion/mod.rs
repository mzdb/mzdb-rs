//! Conversion utilities for mzDB files
//!
//! This module provides converters to export mzDB data to various formats:
//! - MGF (Mascot Generic Format) for MS/MS search engines
//! - DIA (Data-Independent Acquisition) conversion from DDA files

pub mod mgf;
pub use mgf::{MgfWriter, MgfWriterWithStats, MgfExportOptions, MgfExportStats};

#[cfg(feature = "dda2dia")]
pub mod diafication;
#[cfg(feature = "dda2dia")]
pub use diafication::{
    Dda2DiaConverter, DiaConversionOptions, DiaConversionStats,
    Peakel, PeakelDbReader, DiaWindow, generate_dia_windows,
};
