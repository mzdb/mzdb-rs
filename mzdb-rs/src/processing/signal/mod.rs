//! Signal processing algorithms for LC-MS data
//!
//! This module contains:
//! - Signal filtering (Savitzky-Golay smoothing, baseline removal)
//! - Peak detection algorithms (Basic, Smart, Histogram-based)

pub mod filtering;
pub mod detection;
