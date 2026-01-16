//! DIA (Data Independent Acquisition) MS2 Peakel Detection
//!
//! This module re-exports the DIA detection types from `signal::ms2_detection`.
//!
//! For new code, consider importing directly from `processing::signal::ms2_detection`.
//!
//! # Example
//!
//! ```no_run
//! use mzdb::MzDbReader;
//! use mzdb::processing::dia::{DiaMs2PeakelDetector, DiaMs2PeakelConfig};
//!
//! let reader = MzDbReader::open("dia_file.mzDB").unwrap();
//! let detector = DiaMs2PeakelDetector::new();
//! let (windows, peakels) = detector.detect_all_peakels(&reader).unwrap();
//! println!("Detected {} peakels across {} windows", peakels.len(), windows.len());
//! ```

use std::path::PathBuf;

// Re-export all types from ms2_detection
pub use crate::processing::signal::ms2_detection::{
    IsolationWindow,
    DiaMs2PeakelRecord,
    DiaMs2PeakelConfig,
    DiaMs2PeakelDetector,
};

/// Write DIA MS2 peakels to a SQLite database (modified peakelDB format)
///
/// The schema includes an additional isolation_window table and
/// peakel table with isolation_window_id foreign key.
///
/// Note: This is a convenience wrapper around `Ms2PeakelDbWriter`.
/// For more control, use `Ms2PeakelDbWriter` directly.
pub fn write_dia_peakeldb(
    path: &PathBuf,
    mzdb_filename: &str,
    windows: &[IsolationWindow],
    peakels: &[DiaMs2PeakelRecord],
) -> anyhow_ext::Result<()> {
    use crate::processing::peakeldb::Ms2PeakelDbWriter;

    let writer = Ms2PeakelDbWriter::create(path)?;
    writer.write_peakels(mzdb_filename, windows, peakels)?;

    log::info!("DIA MS2 peakelDB created with {} isolation windows and {} peakels",
               windows.len(), peakels.len());

    Ok(())
}
