//! mzdbcheck - Verify mzDB files and determine DDA/DIA acquisition type
//!
//! This command-line tool checks mzDB files to determine if they are DDA or DIA
//! acquisitions and verifies that DIA files can be loaded correctly.
//!
//! # Usage
//!
//! ```bash
//! mzdbcheck <mzdb_file> [options]
//! ```
//!
//! # Examples
//!
//! ```bash
//! # Basic check
//! mzdbcheck input.mzDB
//!
//! # Full DIA verification
//! mzdbcheck input.mzDB --verify
//! ```

use std::path::PathBuf;
use std::process;

use anyhow_ext::Result;
use clap::Parser;

use mzdb::MzDbReader;

/// Verify mzDB files and determine DDA/DIA acquisition type
#[derive(Parser, Debug)]
#[command(name = "mzdbcheck")]
#[command(author = "mzdb-rs")]
#[command(version = "0.3.0")]
#[command(about = "Check mzDB files for DDA/DIA acquisition type and verify integrity", long_about = None)]
struct Args {
    /// Input mzDB file path
    #[arg(value_name = "FILE")]
    input: PathBuf,

    /// Perform full DIA verification (read spectra, query R-tree)
    #[arg(long)]
    verify: bool,

    /// Enable verbose output
    #[arg(short, long)]
    verbose: bool,
}

fn main() {
    let args = Args::parse();

    // Initialize logging
    if args.verbose {
        env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("debug")).init();
    }

    let mzdb_path = args.input.to_string_lossy();

    if args.verify {
        // Full DIA verification
        match verify_dia_file(&mzdb_path) {
            Ok(result) => {
                print_verification_result(&result, &mzdb_path);
                if !result.is_valid_dia {
                    process::exit(1);
                }
            }
            Err(e) => {
                eprintln!("Error checking file: {}", e);
                process::exit(1);
            }
        }
    } else {
        // Basic check
        match check_mzdb(&mzdb_path) {
            Ok(result) => {
                print_check_result(&result, &mzdb_path);
            }
            Err(e) => {
                eprintln!("Error checking file: {}", e);
                process::exit(1);
            }
        }
    }
}

// ============================================================================
// Output Formatting
// ============================================================================

fn print_check_result(result: &MzDbCheckResult, path: &str) {
    println!("mzDB File Check");
    println!("===============");
    println!("File: {}", path);
    println!();

    if let Some(ref version) = result.version {
        println!("mzDB version: {}", version);
    }

    println!("Total spectra: {}", result.total_spectra);

    if let Some(max_level) = result.max_ms_level {
        println!("Max MS level: {}", max_level);
    }

    if let Some(ms1) = result.ms1_count {
        println!("MS1 spectra: {}", ms1);
    }

    if let Some(ms2) = result.ms2_count {
        println!("MS2 spectra: {}", ms2);
    }

    println!();
    println!("DIA Analysis:");
    println!("  Has MSn R-tree: {}", result.has_msn_rtree);

    if !result.dia_windows.is_empty() {
        println!(
            "  Parent m/z windows (DIA): {} windows found",
            result.dia_windows.len()
        );
        for (i, (min_mz, max_mz)) in result.dia_windows.iter().enumerate() {
            if i < 5 || i >= result.dia_windows.len().saturating_sub(3) {
                println!("    Window {}: {:.2} - {:.2} Da", i + 1, min_mz, max_mz);
            } else if i == 5 {
                println!("    ...");
            }
        }
    } else {
        println!("  No parent m/z windows found (likely DDA)");
    }

    if !result.sample_spectra.is_empty() {
        println!();
        println!("Sample MS2 spectra:");
        for spec in &result.sample_spectra {
            println!("  Spectrum {} (cycle {}):", spec.id, spec.cycle);
            println!("    Title: {}", spec.title);
            println!("    Time: {:.2} s", spec.time);
            println!("    Precursor m/z: {:?}", spec.precursor_mz);
            println!("    Peaks: {}", spec.peaks_count);
            if let Some((min, max)) = spec.isolation_window {
                let center = (min + max) / 2.0;
                let width = max - min;
                println!(
                    "    Isolation window: {:.2}-{:.2} Da (center={:.2}, width={:.1})",
                    min, max, center, width
                );
            }
        }
    }

    println!();
    println!("=== CONCLUSION ===");
    if result.is_dia {
        println!("This file appears to be a DIA/SWATH acquisition:");
        println!("  - MSn R-tree is present");
        println!("  - {} DIA windows detected", result.dia_windows.len());
    } else {
        println!("This file appears to be a DDA acquisition:");
        if !result.has_msn_rtree {
            println!("  - MSn R-tree is NOT present");
        }
        if result.dia_windows.is_empty() {
            println!("  - No parent m/z windows detected");
        }
    }
}

fn print_verification_result(result: &DiaVerificationResult, path: &str) {
    let check = &result.check_result;

    println!("DIA File Verification");
    println!("=====================");
    println!("File: {}", path);
    println!();

    if let Some(ref version) = check.version {
        println!("mzDB version: {}", version);
    }

    println!("Total spectra: {}", check.total_spectra);
    println!();

    println!("Verification Results:");
    println!("  Windows accessible: {}", result.windows_accessible);
    println!("  Spectra readable: {}", result.spectra_readable);
    println!("  R-tree queryable: {}", result.rtree_queryable);
    println!();

    if result.is_valid_dia {
        println!("=== RESULT: VALID DIA FILE ===");
    } else {
        println!("=== RESULT: INVALID OR NOT DIA ===");
        if let Some(ref msg) = result.error_message {
            println!("Error: {}", msg);
        }
    }
}

// ============================================================================
// Data Structures
// ============================================================================

/// Result of checking an mzDB file
#[derive(Debug, Clone)]
pub struct MzDbCheckResult {
    /// mzDB version string
    pub version: Option<String>,
    /// Total number of spectra
    pub total_spectra: usize,
    /// Maximum MS level
    pub max_ms_level: Option<i64>,
    /// Number of MS1 spectra
    pub ms1_count: Option<i64>,
    /// Number of MS2 spectra
    pub ms2_count: Option<i64>,
    /// Whether the MSn R-tree exists
    pub has_msn_rtree: bool,
    /// DIA/SWATH windows if detected
    pub dia_windows: Vec<(f64, f64)>,
    /// Whether this appears to be a DIA file
    pub is_dia: bool,
    /// Sample MS2 spectra info for verification
    pub sample_spectra: Vec<SampleSpectrumInfo>,
}

/// Info about a sample spectrum for verification
#[derive(Debug, Clone)]
pub struct SampleSpectrumInfo {
    /// Spectrum ID
    pub id: i64,
    /// Cycle number
    pub cycle: i64,
    /// Title
    pub title: String,
    /// Retention time in seconds
    pub time: f32,
    /// Precursor m/z if available
    pub precursor_mz: Option<f64>,
    /// Number of peaks
    pub peaks_count: i64,
    /// Isolation window (min, max) if detected
    pub isolation_window: Option<(f64, f64)>,
}

/// Result of DIA file verification
#[derive(Debug, Clone)]
pub struct DiaVerificationResult {
    /// Whether the file is a valid DIA file
    pub is_valid_dia: bool,
    /// Basic check results
    pub check_result: MzDbCheckResult,
    /// Whether DIA windows are accessible
    pub windows_accessible: bool,
    /// Whether spectra can be read
    pub spectra_readable: bool,
    /// Whether R-tree queries work
    pub rtree_queryable: bool,
    /// Error message if verification failed
    pub error_message: Option<String>,
}

// ============================================================================
// MzDB Checking/Verification
// ============================================================================

/// Check an mzDB file and determine if it's DDA or DIA
pub fn check_mzdb(mzdb_path: &str) -> Result<MzDbCheckResult> {
    let reader = MzDbReader::open(mzdb_path)?;

    // Get version
    let version = reader.get_version()?;

    // Get spectrum counts
    let total_spectra = reader.get_spectrum_count();
    let max_ms_level = reader.get_max_ms_level()?;
    let ms1_count = reader.get_spectra_count_by_ms_level(1)?;
    let ms2_count = reader.get_spectra_count_by_ms_level(2)?;

    // Check for MSn R-tree
    let has_msn_rtree = reader.has_msn_rtree()?;

    // Get DIA windows
    let dia_windows = reader.get_parent_mz_windows()?;

    // Sample MS2 spectra
    let mut sample_spectra = Vec::new();
    let headers = reader.get_spectrum_headers();
    let mut ms2_seen = 0;

    for header in headers.iter() {
        if header.ms_level == 2 {
            ms2_seen += 1;
            if ms2_seen <= 3 {
                let isolation_window = header
                    .precursor_list_str
                    .as_ref()
                    .and_then(|s| mzdb::extract_isolation_window(s));

                sample_spectra.push(SampleSpectrumInfo {
                    id: header.id,
                    cycle: header.cycle,
                    title: header.title.clone(),
                    time: header.time,
                    precursor_mz: header.precursor_mz,
                    peaks_count: header.peaks_count,
                    isolation_window,
                });
            }
            if ms2_seen >= 3 {
                break;
            }
        }
    }

    // Determine if DIA
    let is_dia = has_msn_rtree && !dia_windows.is_empty();

    Ok(MzDbCheckResult {
        version,
        total_spectra,
        max_ms_level,
        ms1_count,
        ms2_count,
        has_msn_rtree,
        dia_windows,
        is_dia,
        sample_spectra,
    })
}

/// Verify that a DIA file can be loaded and accessed correctly
pub fn verify_dia_file(mzdb_path: &str) -> Result<DiaVerificationResult> {
    let reader = MzDbReader::open(mzdb_path)?;
    let check = check_mzdb(mzdb_path)?;

    if !check.is_dia {
        return Ok(DiaVerificationResult {
            is_valid_dia: false,
            check_result: check,
            windows_accessible: false,
            spectra_readable: false,
            rtree_queryable: false,
            error_message: Some("File does not appear to be a DIA file".to_string()),
        });
    }

    // Try to access DIA windows
    let windows_accessible = !check.dia_windows.is_empty();

    // Try to read some spectra
    let mut spectra_readable = false;
    if let Some(header) = reader.get_spectrum_headers().iter().find(|h| h.ms_level == 2) {
        if let Ok(spectrum) = reader.get_spectrum(header.id) {
            spectra_readable = spectrum.data.peaks_count > 0 || header.peaks_count == 0;
        }
    } else {
        spectra_readable = true; // No MS2 spectra is still valid
    }

    // Try to query R-tree
    let rtree_queryable = if check.has_msn_rtree {
        if let Some(&(min_mz, max_mz)) = check.dia_windows.first() {
            let center = (min_mz + max_mz) / 2.0;
            // Use a tolerance that covers the window
            let half_width = (max_mz - min_mz) / 2.0;
            log::debug!("R-tree query: center={}, half_width={}", center, half_width);
            match reader.query_msn_bounding_boxes_for_dia(2, center, half_width) {
                Ok(results) => {
                    log::debug!("R-tree query returned {} results", results.len());
                    !results.is_empty()
                }
                Err(e) => {
                    log::debug!("R-tree query error: {}", e);
                    false
                }
            }
        } else {
            true
        }
    } else {
        true // No MSn R-tree expected, so this is OK
    };

    // DIA file is valid if all checks pass
    let is_valid = windows_accessible && spectra_readable && rtree_queryable;

    Ok(DiaVerificationResult {
        is_valid_dia: is_valid,
        check_result: check,
        windows_accessible,
        spectra_readable,
        rtree_queryable,
        error_message: None,
    })
}
