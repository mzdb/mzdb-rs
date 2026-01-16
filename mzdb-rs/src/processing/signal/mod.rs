//! Signal processing algorithms for LC-MS data
//!
//! This module contains:
//! - Signal filtering (Savitzky-Golay smoothing, baseline removal)
//! - Peak detection algorithms (Basic, Smart, Histogram-based)
//! - MS1 peakel detection using walking algorithm
//! - MS2 DIA peakel detection using walking algorithm

pub mod filtering;
pub mod finder;
pub mod detection;
pub mod ms1_detection;
pub mod ms2_detection;
