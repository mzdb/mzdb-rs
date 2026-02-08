//! PeakelDB Regression Tests
//!
//! These tests verify that the Rust peakel detection implementation produces
//! results that match the Scala reference implementation.
//!
//! # Test Data
//!
//! The tests require the following files in the `data/` directory:
//! - `OVEMB150205_12.mzdbrs.mzDB` - Source mzDB file for peakel detection
//! - `OVEMB150205_12.ref.peakelDB` - Scala reference peakelDB for comparison
//!
//! # Running the Tests
//!
//! ```bash
//! cargo test --features processing --test peakeldb_regression_tests
//! ```
//!
//! For verbose output with statistics:
//! ```bash
//! cargo test --features processing --test peakeldb_regression_tests -- --nocapture
//! ```
//!
//! # Expected Results
//!
//! When the implementation is correct, the tests should achieve:
//! - Match rate: 100% (all reference peakels found in Rust output)
//! - Exact m/z match: 100%
//! - Exact intensity match: 100%
//! - Exact area match: 100%
//! - Intensity CV within 0.01: 100%
//!
//! The Rust implementation should produce identical results to the Scala reference.

#![cfg(feature = "processing")]

use std::collections::HashMap;
use std::path::Path;

use rusqlite::Connection;

use mzdb::MzDbReader;
use mzdb::processing::{
    Ms1PeakelDetector, Ms1PeakelConfig, Ms1PeakelDbWriter,
    PeakelDbWriter, Peakel, HasPeakelData,
};

// ============================================================================
// Test Data Paths
// ============================================================================

// Note: put those files in data folder to enable the tests (otherwise they are skipped)
const TEST_MZDB_PATH: &str = "data/OVEMB150205_12.mzdbrs.mzDB";
const REFERENCE_PEAKELDB_PATH: &str = "data/OVEMB150205_12.ref.peakelDB";

// ============================================================================
// Test Configuration (matching Scala reference defaults)
// ============================================================================

/// Default configuration matching the Scala reference implementation
fn default_ms1_config() -> Ms1PeakelConfig {
    Ms1PeakelConfig {
        mz_tol_ppm: 10.0,
        min_intensity: 0.0,
        min_peaks: 5,
        max_consecutive_gaps: 3,
        max_total_gaps: usize::MAX,
        max_time_window: 1200.0,
        intensity_percentile: 0.9,
        min_peakel_amplitude: 1.5,
        min_peakel_duration: 0.0,
        algorithm: "smart".to_string(),
        skip_apex_boundary_check: true, // Match Scala reference
    }
}

// ============================================================================
// Reference Peakel Data Structure
// ============================================================================

/// Peakel data loaded from reference peakelDB for comparison
#[derive(Debug, Clone)]
#[allow(dead_code)]
struct ReferencePeakel {
    id: i64,
    apex_spectrum_id: i64,
    first_spectrum_id: i64,
    last_spectrum_id: i64,
    moz: f64,
    elution_time: f32,
    apex_intensity: f32,
    area: f32,
    intensity_cv: f32,
    peak_count: i32,
}

/// Load reference peakels from the Scala-generated peakelDB
fn load_reference_peakels(path: &str) -> anyhow_ext::Result<Vec<ReferencePeakel>> {
    let conn = Connection::open(path)?;

    let mut stmt = conn.prepare(
        "SELECT id, apex_spectrum_id, first_spectrum_id, last_spectrum_id,
                moz, elution_time, apex_intensity, area, intensity_cv, peak_count
         FROM peakel
         ORDER BY id"
    )?;

    let peakels = stmt.query_map([], |row| {
        Ok(ReferencePeakel {
            id: row.get(0)?,
            apex_spectrum_id: row.get(1)?,
            first_spectrum_id: row.get(2)?,
            last_spectrum_id: row.get(3)?,
            moz: row.get(4)?,
            elution_time: row.get(5)?,
            apex_intensity: row.get(6)?,
            area: row.get(7)?,
            intensity_cv: row.get(8)?,
            peak_count: row.get(9)?,
        })
    })?
    .collect::<Result<Vec<_>, _>>()?;

    Ok(peakels)
}

// ============================================================================
// Comparison Utilities
// ============================================================================

/// Key for matching peakels: (apex_spectrum_id, first_spectrum_id, last_spectrum_id)
type PeakelKey = (i64, i64, i64);

/// Comparison statistics
#[derive(Debug, Default)]
struct ComparisonStats {
    reference_count: usize,
    rust_count: usize,
    matched_count: usize,
    unmatched_in_reference: usize,
    unmatched_in_rust: usize,
    exact_mz_matches: usize,
    exact_rt_matches: usize,
    exact_intensity_matches: usize,
    exact_area_matches: usize,
    exact_cv_matches: usize,
    exact_peaks_matches: usize,
    max_mz_diff_ppm: f64,
    max_rt_diff_sec: f32,
    max_intensity_ratio: f32,
}

impl ComparisonStats {
    fn print_summary(&self) {
        println!("\n======================================================================");
        println!("PEAKELDB REGRESSION TEST RESULTS");
        println!("======================================================================\n");

        println!("### PEAKEL COUNTS ###");
        println!("  Reference (Scala): {}", self.reference_count);
        println!("  Rust:              {}", self.rust_count);
        println!("  Difference:        {:+}\n", self.rust_count as i64 - self.reference_count as i64);

        let match_pct = 100.0 * self.matched_count as f64 / self.reference_count as f64;
        println!("### MATCHING RESULTS ###");
        println!("  Matched peakels:     {} ({:.2}% of reference)", self.matched_count, match_pct);
        println!("  Unmatched in Ref:    {}", self.unmatched_in_reference);
        println!("  Unmatched in Rust:   {}\n", self.unmatched_in_rust);

        let exact_mz_pct = 100.0 * self.exact_mz_matches as f64 / self.matched_count as f64;
        let exact_rt_pct = 100.0 * self.exact_rt_matches as f64 / self.matched_count as f64;
        let exact_int_pct = 100.0 * self.exact_intensity_matches as f64 / self.matched_count as f64;
        let exact_area_pct = 100.0 * self.exact_area_matches as f64 / self.matched_count as f64;
        let exact_cv_pct = 100.0 * self.exact_cv_matches as f64 / self.matched_count as f64;
        let exact_peaks_pct = 100.0 * self.exact_peaks_matches as f64 / self.matched_count as f64;

        println!("### EXACT MATCH RATES ###");
        println!("  M/z identical:       {} ({:.1}%)", self.exact_mz_matches, exact_mz_pct);
        println!("  RT identical:        {} ({:.1}%)", self.exact_rt_matches, exact_rt_pct);
        println!("  Intensity identical: {} ({:.1}%)", self.exact_intensity_matches, exact_int_pct);
        println!("  Area identical:      {} ({:.1}%)", self.exact_area_matches, exact_area_pct);
        println!("  CV within 0.01:      {} ({:.1}%)", self.exact_cv_matches, exact_cv_pct);
        println!("  Peaks count same:    {} ({:.1}%)\n", self.exact_peaks_matches, exact_peaks_pct);

        println!("### MAXIMUM DIFFERENCES ###");
        println!("  Max m/z diff:        {:.4} ppm", self.max_mz_diff_ppm);
        println!("  Max RT diff:         {:.4} s", self.max_rt_diff_sec);
        println!("  Max intensity ratio: {:.6}", self.max_intensity_ratio);
    }
}

/// Compare Rust peakels against reference and return statistics
fn compare_peakels(
    rust_peakels: &[Peakel],
    reference_peakels: &[ReferencePeakel],
) -> ComparisonStats {
    let mut stats = ComparisonStats {
        reference_count: reference_peakels.len(),
        rust_count: rust_peakels.len(),
        ..Default::default()
    };

    // Group reference peakels by key (apex, first, last)
    // Note: multiple peakels can share the same key (different m/z values)
    let mut ref_by_key: HashMap<PeakelKey, Vec<&ReferencePeakel>> = HashMap::new();
    for p in reference_peakels {
        let key = (p.apex_spectrum_id, p.first_spectrum_id, p.last_spectrum_id);
        ref_by_key.entry(key).or_default().push(p);
    }

    // Group Rust peakels by key
    let mut rust_by_key: HashMap<PeakelKey, Vec<&Peakel>> = HashMap::new();
    for p in rust_peakels {
        let key = (
            p.apex_spectrum_id().unwrap_or(0),
            p.first_spectrum_id().unwrap_or(0),
            p.last_spectrum_id().unwrap_or(0),
        );
        rust_by_key.entry(key).or_default().push(p);
    }

    // Track matched Rust peakels to find unmatched ones
    let mut matched_rust_indices: std::collections::HashSet<(PeakelKey, usize)> = std::collections::HashSet::new();

    // Match peakels by key and m/z
    for (key, ref_list) in &ref_by_key {
        if let Some(rust_list) = rust_by_key.get(key) {
            for ref_p in ref_list {
                // Find matching Rust peakel by m/z (within 1e-6 Da for exact match)
                let mut best_match: Option<(usize, &Peakel)> = None;
                let mut best_mz_diff = f64::INFINITY;

                for (idx, rust_p) in rust_list.iter().enumerate() {
                    let rust_mz = rust_p.apex_mz().unwrap_or(0.0) as f64;
                    let mz_diff = (rust_mz - ref_p.moz).abs();

                    // Use very strict tolerance - values should be identical
                    if mz_diff < 1e-6 && mz_diff < best_mz_diff {
                        best_mz_diff = mz_diff;
                        best_match = Some((idx, rust_p));
                    }
                }

                if let Some((idx, rust_p)) = best_match {
                    stats.matched_count += 1;
                    matched_rust_indices.insert((*key, idx));

                    // Compare values
                    let rust_mz = rust_p.apex_mz().unwrap_or(0.0) as f64;
                    let rust_rt = rust_p.apex_elution_time().unwrap_or(0.0);
                    let rust_int = rust_p.apex_intensity().unwrap_or(0.0);
                    let rust_area = rust_p.calc_area();
                    let rust_cv = rust_p.calc_intensity_cv();
                    let rust_peaks = rust_p.peaks_count() as i32;

                    // Check exact matches
                    let mz_diff = (rust_mz - ref_p.moz).abs();
                    let mz_ppm_diff = mz_diff / ref_p.moz * 1e6;
                    if mz_ppm_diff < 1.0 {  // Within 1 ppm is considered identical
                        stats.exact_mz_matches += 1;
                    }
                    stats.max_mz_diff_ppm = stats.max_mz_diff_ppm.max(mz_ppm_diff);

                    let rt_diff = (rust_rt - ref_p.elution_time).abs();
                    if rt_diff < 1e-6 {
                        stats.exact_rt_matches += 1;
                    }
                    stats.max_rt_diff_sec = stats.max_rt_diff_sec.max(rt_diff);

                    let int_ratio = if ref_p.apex_intensity > 0.0 {
                        rust_int / ref_p.apex_intensity
                    } else {
                        1.0
                    };
                    if (int_ratio - 1.0).abs() < 1e-6 {
                        stats.exact_intensity_matches += 1;
                    }
                    stats.max_intensity_ratio = stats.max_intensity_ratio.max((int_ratio - 1.0).abs());

                    let area_ratio = if ref_p.area > 0.0 {
                        rust_area / ref_p.area
                    } else {
                        1.0
                    };
                    if (area_ratio - 1.0).abs() < 1e-6 {
                        stats.exact_area_matches += 1;
                    }

                    let cv_diff = (rust_cv - ref_p.intensity_cv).abs();
                    if cv_diff < 0.01 {
                        stats.exact_cv_matches += 1;
                    }

                    if rust_peaks == ref_p.peak_count {
                        stats.exact_peaks_matches += 1;
                    }
                } else {
                    stats.unmatched_in_reference += 1;
                }
            }
        } else {
            stats.unmatched_in_reference += ref_list.len();
        }
    }

    // Count unmatched Rust peakels
    for (key, rust_list) in &rust_by_key {
        for (idx, _) in rust_list.iter().enumerate() {
            if !matched_rust_indices.contains(&(*key, idx)) {
                stats.unmatched_in_rust += 1;
            }
        }
    }

    stats
}

// ============================================================================
// Regression Tests
// ============================================================================

/// Main regression test: generate peakelDB and compare against reference
#[test]
fn test_ms1_peakeldb_matches_scala_reference() {
    // Skip if test files don't exist
    if !Path::new(TEST_MZDB_PATH).exists() {
        eprintln!("Skipping test: {} not found", TEST_MZDB_PATH);
        return;
    }
    if !Path::new(REFERENCE_PEAKELDB_PATH).exists() {
        eprintln!("Skipping test: {} not found", REFERENCE_PEAKELDB_PATH);
        return;
    }

    println!("\n=== MS1 PeakelDB Regression Test ===\n");
    println!("Source mzDB: {}", TEST_MZDB_PATH);
    println!("Reference peakelDB: {}", REFERENCE_PEAKELDB_PATH);

    // Load reference peakels
    println!("\nLoading reference peakels...");
    let reference_peakels = load_reference_peakels(REFERENCE_PEAKELDB_PATH)
        .expect("Failed to load reference peakelDB");
    println!("Loaded {} reference peakels", reference_peakels.len());

    // Open mzDB and detect peakels
    println!("\nDetecting peakels from mzDB...");
    let reader = MzDbReader::open(TEST_MZDB_PATH)
        .expect("Failed to open mzDB file");

    let config = default_ms1_config();
    let detector = Ms1PeakelDetector::with_config(config);

    let rust_peakels = detector.detect_all_peakels(&reader)
        .expect("Failed to detect peakels");
    println!("Detected {} Rust peakels", rust_peakels.len());

    // Compare
    println!("\nComparing peakels...");
    let stats = compare_peakels(&rust_peakels, &reference_peakels);
    stats.print_summary();

    // Assertions
    let match_rate = stats.matched_count as f64 / stats.reference_count as f64;
    assert!(
        match_rate >= 0.99,
        "Match rate {:.2}% is below threshold of 99%. Found {} matches out of {} reference peakels.",
        match_rate * 100.0,
        stats.matched_count,
        stats.reference_count
    );

    // Check that exact match rates are high
    let exact_mz_rate = stats.exact_mz_matches as f64 / stats.matched_count as f64;
    assert!(
        exact_mz_rate >= 0.99,
        "Exact m/z match rate {:.2}% is below threshold of 99%",
        exact_mz_rate * 100.0
    );

    let exact_int_rate = stats.exact_intensity_matches as f64 / stats.matched_count as f64;
    assert!(
        exact_int_rate >= 0.99,
        "Exact intensity match rate {:.2}% is below threshold of 99%",
        exact_int_rate * 100.0
    );

    let exact_area_rate = stats.exact_area_matches as f64 / stats.matched_count as f64;
    assert!(
        exact_area_rate >= 0.99,
        "Exact area match rate {:.2}% is below threshold of 99%",
        exact_area_rate * 100.0
    );

    println!("\n✅ Regression test PASSED: Rust implementation matches Scala reference");
}

/// Test that peakelDB file can be written and read back correctly
#[test]
fn test_peakeldb_write_and_read() {
    // Skip if test file doesn't exist
    if !Path::new(TEST_MZDB_PATH).exists() {
        eprintln!("Skipping test: {} not found", TEST_MZDB_PATH);
        return;
    }

    println!("\n=== PeakelDB Write/Read Test ===\n");

    // Detect peakels
    let reader = MzDbReader::open(TEST_MZDB_PATH)
        .expect("Failed to open mzDB file");

    let config = default_ms1_config();
    let detector = Ms1PeakelDetector::with_config(config);
    let peakels = detector.detect_all_peakels(&reader)
        .expect("Failed to detect peakels");

    println!("Detected {} peakels", peakels.len());

    // Write to temp file
    let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
    let temp_path = temp_dir.path().join("test_output.peakelDB");

    println!("Writing to {:?}...", temp_path);
    let mut writer = Ms1PeakelDbWriter::create(&temp_path, "test.mzDB", false)
        .expect("Failed to create peakelDB writer");
    writer.write_all_peakels(&peakels)
        .expect("Failed to write peakels");
    writer.close()
        .expect("Failed to close peakelDB writer");

    // Read back and verify count
    let conn = Connection::open(&temp_path)
        .expect("Failed to open written peakelDB");
    let count: i64 = conn.query_row("SELECT COUNT(*) FROM peakel", [], |row| row.get(0))
        .expect("Failed to count peakels");

    println!("Read back {} peakels", count);

    assert_eq!(
        count as usize,
        peakels.len(),
        "Written peakel count doesn't match detected count"
    );

    // Verify intensity_cv is populated
    let zero_cv_count: i64 = conn.query_row(
        "SELECT COUNT(*) FROM peakel WHERE intensity_cv = 0",
        [],
        |row| row.get(0)
    ).expect("Failed to count zero CV");

    println!("Peakels with zero intensity_cv: {}", zero_cv_count);

    // Most peakels should have non-zero CV (unless they have constant intensity)
    let zero_cv_rate = zero_cv_count as f64 / count as f64;
    assert!(
        zero_cv_rate < 0.1,
        "Too many peakels ({:.1}%) have zero intensity_cv",
        zero_cv_rate * 100.0
    );

    println!("\n✅ Write/Read test PASSED");
}

/// Test peakel detection with different configurations
#[test]
fn test_peakel_detection_configuration() {
    // Skip if test file doesn't exist
    if !Path::new(TEST_MZDB_PATH).exists() {
        eprintln!("Skipping test: {} not found", TEST_MZDB_PATH);
        return;
    }

    println!("\n=== Configuration Sensitivity Test ===\n");

    let reader = MzDbReader::open(TEST_MZDB_PATH)
        .expect("Failed to open mzDB file");

    // Test with default config
    let config_default = default_ms1_config();
    let detector = Ms1PeakelDetector::with_config(config_default);
    let peakels_default = detector.detect_all_peakels(&reader)
        .expect("Failed to detect peakels with default config");
    println!("Default config: {} peakels", peakels_default.len());

    // Test with stricter min_peaks
    let config_strict = Ms1PeakelConfig {
        min_peaks: 10,
        ..default_ms1_config()
    };
    let detector = Ms1PeakelDetector::with_config(config_strict);
    let peakels_strict = detector.detect_all_peakels(&reader)
        .expect("Failed to detect peakels with strict config");
    println!("Strict min_peaks (10): {} peakels", peakels_strict.len());

    // Stricter config should produce fewer peakels
    assert!(
        peakels_strict.len() < peakels_default.len(),
        "Stricter min_peaks should produce fewer peakels"
    );

    // Test with higher amplitude threshold
    let config_high_amp = Ms1PeakelConfig {
        min_peakel_amplitude: 3.0,
        ..default_ms1_config()
    };
    let detector = Ms1PeakelDetector::with_config(config_high_amp);
    let peakels_high_amp = detector.detect_all_peakels(&reader)
        .expect("Failed to detect peakels with high amplitude config");
    println!("High amplitude (3.0): {} peakels", peakels_high_amp.len());

    assert!(
        peakels_high_amp.len() < peakels_default.len(),
        "Higher amplitude threshold should produce fewer peakels"
    );

    println!("\n✅ Configuration test PASSED");
}

/// Test that intensity_cv calculation matches reference
#[test]
fn test_intensity_cv_calculation() {
    // Skip if test files don't exist
    if !Path::new(TEST_MZDB_PATH).exists() || !Path::new(REFERENCE_PEAKELDB_PATH).exists() {
        eprintln!("Skipping test: required files not found");
        return;
    }

    println!("\n=== Intensity CV Calculation Test ===\n");

    // Load reference peakels
    let reference_peakels = load_reference_peakels(REFERENCE_PEAKELDB_PATH)
        .expect("Failed to load reference peakelDB");

    // Detect Rust peakels
    let reader = MzDbReader::open(TEST_MZDB_PATH)
        .expect("Failed to open mzDB file");
    let detector = Ms1PeakelDetector::with_config(default_ms1_config());
    let rust_peakels = detector.detect_all_peakels(&reader)
        .expect("Failed to detect peakels");

    // Group by key for matching
    let mut ref_by_key: HashMap<PeakelKey, Vec<&ReferencePeakel>> = HashMap::new();
    for p in &reference_peakels {
        let key = (p.apex_spectrum_id, p.first_spectrum_id, p.last_spectrum_id);
        ref_by_key.entry(key).or_default().push(p);
    }

    let mut cv_diffs: Vec<f32> = Vec::new();
    let mut matched = 0;

    for rust_p in &rust_peakels {
        let key = (
            rust_p.apex_spectrum_id().unwrap_or(0),
            rust_p.first_spectrum_id().unwrap_or(0),
            rust_p.last_spectrum_id().unwrap_or(0),
        );

        if let Some(ref_list) = ref_by_key.get(&key) {
            let rust_mz = rust_p.apex_mz().unwrap_or(0.0) as f64;

            // Find matching reference by m/z (strict tolerance - should be identical)
            for ref_p in ref_list {
                if (rust_mz - ref_p.moz).abs() < 1e-6 {
                    let rust_cv = rust_p.calc_intensity_cv();
                    let cv_diff = (rust_cv - ref_p.intensity_cv).abs();
                    cv_diffs.push(cv_diff);
                    matched += 1;
                    break;
                }
            }
        }
    }

    println!("Matched peakels for CV comparison: {}", matched);

    if !cv_diffs.is_empty() {
        let max_diff = cv_diffs.iter().cloned().fold(0.0f32, f32::max);
        let mean_diff: f32 = cv_diffs.iter().sum::<f32>() / cv_diffs.len() as f32;
        let within_001: usize = cv_diffs.iter().filter(|&&d| d < 0.01).count();

        println!("CV difference stats:");
        println!("  Max diff: {:.4}", max_diff);
        println!("  Mean diff: {:.6}", mean_diff);
        println!("  Within 0.01: {} ({:.1}%)", within_001, 100.0 * within_001 as f64 / cv_diffs.len() as f64);

        // Almost all CV values should match within 0.01
        let within_001_rate = within_001 as f64 / cv_diffs.len() as f64;
        assert!(
            within_001_rate >= 0.99,
            "Only {:.1}% of CV values match within 0.01",
            within_001_rate * 100.0
        );
    }

    println!("\n✅ Intensity CV test PASSED");
}

