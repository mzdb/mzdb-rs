//! mzdbcheck - Verify mzDB files and determine DDA/DIA acquisition type
//!
//! This command-line tool checks mzDB files to determine if they are DDA or DIA
//! acquisitions, verifies that DIA files can be loaded correctly, and checks
//! bounding box data integrity.
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
//!
//! # Check bounding box integrity
//! mzdbcheck input.mzDB --check-bb
//! ```

use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::process;

use anyhow_ext::Result;
use clap::Parser;
use rusqlite::Connection;

use mzdb::MzDbReader;

/// Verify mzDB files and determine DDA/DIA acquisition type
#[derive(Parser, Debug)]
#[command(
    name = "mzdbcheck",
    author,
    version,
    about = "Check mzDB files for DDA/DIA acquisition type and verify integrity",
    long_about = None
)]
struct Args {
    /// Input mzDB file path
    #[arg(value_name = "FILE")]
    input: PathBuf,

    /// Perform full DIA verification (read spectra, query R-tree)
    #[arg(long)]
    check_dia: bool,

    /// Check bounding box integrity (spectrum-BB linkage)
    #[arg(long)]
    check_bb: bool,

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

    // Basic check (always performed)
    let mzdb_check_result = match check_mzdb(&mzdb_path) {
        Ok(result) => {
            print_check_result(&result, &mzdb_path);
            result
        }
        Err(e) => {
            eprintln!("Error checking file: {}", e);
            process::exit(1);
        }
    };

    // Bounding box integrity check
    if args.check_bb  {
        println!("\n{}", "=".repeat(80));
        println!("BOUNDING BOX INTEGRITY CHECK");
        println!("{}", "=".repeat(80));

        match verify_bounding_box_integrity(&mzdb_path) {
            Ok(bb_result) => {
                print_bb_integrity_result(&bb_result);

                if !bb_result.is_valid {
                    eprintln!("\n❌ BOUNDING BOX INTEGRITY CHECK FAILED");
                    process::exit(1);
                }
            }
            Err(e) => {
                eprintln!("Error checking bounding box integrity: {}", e);
                process::exit(1);
            }
        }
    }

    // Full DIA verification
    if args.check_dia {
        println!("\n{}", "=".repeat(80));
        println!("DIA VERIFICATION");
        println!("{}", "=".repeat(80));

        let dia_check_result = if !mzdb_check_result.is_dia {
            Ok(DiaVerificationResult {
                is_valid_dia: false,
                windows_accessible: false,
                spectra_readable: false,
                rtree_queryable: false,
                error_message: Some("File does not appear to be a DIA file".to_string()),
            })
        } else {
            verify_dia_file(&mzdb_path, &mzdb_check_result)
        };

        match dia_check_result {
            Ok(result) => {
                print_verification_result(&result, &mzdb_path);

                if !result.is_valid_dia {
                    process::exit(1);
                }
            }
            Err(e) => {
                eprintln!("Error verifying DIA file: {}", e);
                process::exit(1);
            }
        }
    }

    println!("\n✓ All checks passed");
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

    println!("DIA File Verification");
    println!("=====================");
    println!("File: {}", path);
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

fn print_bb_integrity_result(result: &BoundingBoxIntegrityResult) {
    println!();
    println!("Statistics:");
    println!("  Total spectra:                 {}", result.total_spectra);
    println!("  Spectra without BB ref:        {} (NULL or 0)", result.spectra_without_bb);
    println!("  Spectra requiring BB:          {}", result.spectra_requiring_bb);
    println!("  Total bounding boxes:          {}", result.total_bounding_boxes);
    println!();

    if result.is_valid {
        println!("✓ All bounding box integrity checks passed!");
        println!();
        println!("  - All bb_first_spectrum_id references are valid");
        println!("  - All spectra with BB refs are present in their BBs");
    } else {
        println!("✗ Bounding box integrity issues detected:");
        println!();

        if !result.spectra_with_invalid_bb_ref.is_empty() {
            println!("Invalid bb_first_spectrum_id references ({} spectra):",
                     result.spectra_with_invalid_bb_ref.len());
            for (i, issue) in result.spectra_with_invalid_bb_ref.iter().enumerate() {
                if i < 10 {
                    println!("  - Spectrum {} (MS{}): {}",
                             issue.spectrum_id, issue.ms_level, issue.reason);
                } else if i == 10 {
                    println!("  ... and {} more", result.spectra_with_invalid_bb_ref.len() - 10);
                    break;
                }
            }
            println!();
        }

        if !result.spectra_missing_in_bb.is_empty() {
            println!("Spectra missing from BB blobs ({} spectra):",
                     result.spectra_missing_in_bb.len());
            for (i, issue) in result.spectra_missing_in_bb.iter().enumerate() {
                if i < 10 {
                    println!("  - Spectrum {} (MS{}): {}",
                             issue.spectrum_id, issue.ms_level, issue.reason);
                } else if i == 10 {
                    println!("  ... and {} more", result.spectra_missing_in_bb.len() - 10);
                    break;
                }
            }
            println!();
        }

        if !result.errors.is_empty() {
            println!("Additional errors:");
            for error in &result.errors {
                println!("  - {}", error);
            }
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
    /// Whether DIA windows are accessible
    pub windows_accessible: bool,
    /// Whether spectra can be read
    pub spectra_readable: bool,
    /// Whether R-tree queries work
    pub rtree_queryable: bool,
    /// Error message if verification failed
    pub error_message: Option<String>,
}

/// Result of bounding box integrity verification
#[derive(Debug, Clone)]
pub struct BoundingBoxIntegrityResult {
    /// Whether all checks passed
    pub is_valid: bool,
    /// Total number of spectra checked
    pub total_spectra: usize,
    /// Number of spectra with NULL or 0 bb_first_spectrum_id
    pub spectra_without_bb: usize,
    /// Number of spectra that should have BB data
    pub spectra_requiring_bb: usize,
    /// Spectra with invalid bb_first_spectrum_id (doesn't exist in spectrum table)
    pub spectra_with_invalid_bb_ref: Vec<SpectrumBBIssue>,
    /// Spectra with valid BB ref but no slice found in any linked BB
    pub spectra_missing_in_bb: Vec<SpectrumBBIssue>,
    /// Total number of bounding boxes
    pub total_bounding_boxes: usize,
    /// Error messages
    pub errors: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct SpectrumBBIssue {
    pub spectrum_id: i64,
    pub bb_first_spectrum_id: Option<i64>,
    pub ms_level: i64,
    pub reason: String,
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
pub fn verify_dia_file(mzdb_path: &str, mzdb_check_result: &MzDbCheckResult) -> Result<DiaVerificationResult> {
    let reader = MzDbReader::open(mzdb_path)?;

    // Try to access DIA windows
    let windows_accessible = !mzdb_check_result.dia_windows.is_empty();

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
    let rtree_queryable = if mzdb_check_result.has_msn_rtree {
        if let Some(&(min_mz, max_mz)) = mzdb_check_result.dia_windows.first() {
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
        windows_accessible,
        spectra_readable,
        rtree_queryable,
        error_message: None,
    })
}

/// Verify bounding box integrity for a given mzDB file
///
/// This function performs comprehensive checks:
/// 1. All spectra have valid bb_first_spectrum_id references (except NULL/0)
/// 2. All spectra with bb_first_spectrum_id are actually present in linked BBs
/// 3. BB data blobs contain slices for their referenced spectra
///
/// According to mzDB spec:
/// - bb_first_spectrum_id can be NULL or 0 for spectra without BB data
/// - If bb_first_spectrum_id is set, it must reference an existing spectrum
/// - That spectrum should be the first one in its bounding box
pub fn verify_bounding_box_integrity(mzdb_path: &str) -> Result<BoundingBoxIntegrityResult> {
    let conn = Connection::open(mzdb_path)?;

    let mut result = BoundingBoxIntegrityResult {
        is_valid: true,
        total_spectra: 0,
        spectra_without_bb: 0,
        spectra_requiring_bb: 0,
        spectra_with_invalid_bb_ref: Vec::new(),
        spectra_missing_in_bb: Vec::new(),
        total_bounding_boxes: 0,
        errors: Vec::new(),
    };

    // Step 1: Get total spectrum count
    result.total_spectra = conn.query_row(
        "SELECT COUNT(*) FROM spectrum",
        [],
        |row| row.get::<_, usize>(0),
    )?;

    // Step 2: Get total BB count
    result.total_bounding_boxes = conn.query_row(
        "SELECT COUNT(*) FROM bounding_box",
        [],
        |row| row.get::<_, usize>(0),
    )?;

    // Step 3: Build a set of valid spectrum IDs
    let mut valid_spectrum_ids = HashSet::new();
    {
        let mut stmt = conn.prepare("SELECT id FROM spectrum")?;
        let ids = stmt.query_map([], |row| row.get::<_, i64>(0))?;
        for id in ids {
            valid_spectrum_ids.insert(id?);
        }
    }

    // Step 4: Check all spectra for valid bb_first_spectrum_id references
    let mut spectra_needing_check: HashMap<i64, (i64, i64)> = HashMap::new(); // spectrum_id -> (bb_first_spectrum_id, ms_level)

    {
        let mut stmt = conn.prepare(
            "SELECT id, bb_first_spectrum_id, ms_level FROM spectrum"
        )?;
        let mut rows = stmt.query([])?;

        while let Some(row) = rows.next()? {
            let spectrum_id: i64 = row.get(0)?;
            let bb_first_spectrum_id: Option<i64> = row.get(1)?;
            let ms_level: i64 = row.get(2)?;

            if let Some(bb_first_id) = bb_first_spectrum_id {
                // Skip if bb_first_spectrum_id is 0 (treated as NULL)
                if bb_first_id == 0 {
                    result.spectra_without_bb += 1;
                    continue;
                }

                result.spectra_requiring_bb += 1;

                // Check if bb_first_spectrum_id references a valid spectrum
                if !valid_spectrum_ids.contains(&bb_first_id) {
                    result.spectra_with_invalid_bb_ref.push(SpectrumBBIssue {
                        spectrum_id,
                        bb_first_spectrum_id: Some(bb_first_id),
                        ms_level,
                        reason: format!(
                            "bb_first_spectrum_id={} does not exist in spectrum table",
                            bb_first_id
                        ),
                    });
                    result.is_valid = false;
                } else {
                    // Mark this spectrum as needing verification in BB blobs
                    spectra_needing_check.insert(spectrum_id, (bb_first_id, ms_level));
                }
            } else {
                result.spectra_without_bb += 1;
            }
        }
    }

    // Step 5: For each spectrum requiring BB, verify it exists in the linked BBs
    // Build map: bb_first_spectrum_id -> Vec<bounding_box_ids>
    let mut bb_map: HashMap<i64, Vec<i64>> = HashMap::new();
    {
        let mut stmt = conn.prepare(
            "SELECT id, first_spectrum_id FROM bounding_box"
        )?;
        let mut rows = stmt.query([])?;

        while let Some(row) = rows.next()? {
            let bb_id: i64 = row.get(0)?;
            let first_spectrum_id: i64 = row.get(1)?;
            bb_map.entry(first_spectrum_id).or_insert_with(Vec::new).push(bb_id);
        }
    }

    // Step 6: Parse BB blobs to verify spectrum presence
    for (spectrum_id, (bb_first_id, ms_level)) in spectra_needing_check.iter() {
        if let Some(bb_ids) = bb_map.get(bb_first_id) {
            let mut found_in_any_bb = false;

            for &bb_id in bb_ids {
                // Get BB blob data
                let blob_data: Vec<u8> = match conn.query_row(
                    "SELECT data FROM bounding_box WHERE id = ?1",
                    [bb_id],
                    |row| row.get(0),
                ) {
                    Ok(data) => data,
                    Err(e) => {
                        result.errors.push(format!(
                            "Failed to read BB {} data: {}",
                            bb_id, e
                        ));
                        continue;
                    }
                };

                // Parse the blob to find spectrum IDs
                if let Ok(spectrum_ids_in_bb) = parse_bb_blob_spectrum_ids(&blob_data) {
                    if spectrum_ids_in_bb.contains(spectrum_id) {
                        found_in_any_bb = true;
                        break;
                    }
                }
            }

            if !found_in_any_bb {
                result.spectra_missing_in_bb.push(SpectrumBBIssue {
                    spectrum_id: *spectrum_id,
                    bb_first_spectrum_id: Some(*bb_first_id),
                    ms_level: *ms_level,
                    reason: format!(
                        "Spectrum not found in any BB linked by bb_first_spectrum_id={}",
                        bb_first_id
                    ),
                });
                result.is_valid = false;
            }
        } else {
            // No BB found with this first_spectrum_id
            result.spectra_missing_in_bb.push(SpectrumBBIssue {
                spectrum_id: *spectrum_id,
                bb_first_spectrum_id: Some(*bb_first_id),
                ms_level: *ms_level,
                reason: format!(
                    "No bounding_box found with first_spectrum_id={}",
                    bb_first_id
                ),
            });
            result.is_valid = false;
        }
    }

    Ok(result)
}

/// Parse a bounding box blob to extract spectrum IDs
///
/// BB blob format:
/// - For each spectrum slice:
///   - spectrum_id (4 bytes, i32, little-endian)
///   - data_points_count (4 bytes, i32, little-endian)
///   - For each data point:
///     - m/z (8 bytes, f64, little-endian)
///     - intensity (4 bytes, f32, little-endian)
fn parse_bb_blob_spectrum_ids(blob: &[u8]) -> Result<HashSet<i64>> {
    let mut spectrum_ids = HashSet::new();
    let mut offset = 0;

    while offset + 8 <= blob.len() {
        // Read spectrum_id (i32)
        if offset + 4 > blob.len() {
            break;
        }
        let spectrum_id = i32::from_le_bytes([
            blob[offset],
            blob[offset + 1],
            blob[offset + 2],
            blob[offset + 3],
        ]) as i64;
        offset += 4;

        // Read data_points_count (i32)
        if offset + 4 > blob.len() {
            break;
        }
        let data_points_count = i32::from_le_bytes([
            blob[offset],
            blob[offset + 1],
            blob[offset + 2],
            blob[offset + 3],
        ]);
        offset += 4;

        spectrum_ids.insert(spectrum_id);

        // Skip the peak data (12 bytes per peak: 8 for m/z + 4 for intensity)
        let peak_data_size = (data_points_count as usize) * 12;
        offset += peak_data_size;

        if offset > blob.len() {
            // Blob is malformed or truncated
            break;
        }
    }

    Ok(spectrum_ids)
}