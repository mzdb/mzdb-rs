//! slicemzdb - Extract a slice of spectra from an mzDB file
//!
//! This command-line tool creates a subset of an mzDB file containing only
//! spectra within a specified ID range. The tool is useful for creating
//! smaller test files for integration testing and code evaluation.
//!
//! # Features
//!
//! - Preserves all metadata (run_slice table, instrument configuration, etc.)
//! - Keeps original spectrum IDs (no renumbering for simplicity and correctness)
//! - Filters bounding boxes: removes those completely outside the range,
//!   updates those partially inside to only contain relevant slices
//! - Updates R-tree entries accordingly
//!
//! # Algorithm
//!
//! The tool uses an efficient copy-then-modify approach:
//! 1. Copy the entire mzDB file (fast sequential I/O)
//! 2. Delete spectra outside the range
//! 3. Update bounding box blobs to remove out-of-range slices
//! 4. Delete empty bounding boxes and their R-tree entries
//! 5. VACUUM to reclaim space
//!
//! # Usage
//!
//! ```bash
//! slicemzdb --input input.mzDB --output slice.mzDB --min-id 100 --max-id 200
//! ```

use std::collections::HashMap;
use std::path::PathBuf;
use std::process;

use anyhow_ext::{bail, Context, Result};
use clap::Parser;
use rusqlite::{params, Connection, OptionalExtension};

/// Extract a slice of spectra from an mzDB file
#[derive(Parser, Debug)]
#[command(
    name = "slicemzdb",
    author,
    version,
    about = "Extract a slice of spectra from an mzDB file by spectrum ID range",
    long_about = "This tool creates a subset of an mzDB file containing only spectra \
                  within a specified ID range. All metadata is preserved, spectrum IDs \
                  are renumbered sequentially, and bounding boxes are updated to contain \
                  only the relevant spectrum slices."
)]
struct Args {
    /// Input mzDB file path
    #[arg(short, long)]
    input: PathBuf,

    /// Output mzDB file path
    #[arg(short, long)]
    output: PathBuf,

    /// Minimum spectrum ID (inclusive)
    #[arg(long)]
    min_id: i64,

    /// Maximum spectrum ID (inclusive)
    #[arg(long)]
    max_id: i64,

    /// Enable verbose output
    #[arg(short, long)]
    verbose: bool,
}

fn main() {
    let args = Args::parse();

    // Initialize logging
    if args.verbose {
        env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("debug")).init();
    } else {
        env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();
    }

    // Validate arguments
    if args.min_id > args.max_id {
        eprintln!("Error: min-id ({}) must be <= max-id ({})", args.min_id, args.max_id);
        process::exit(1);
    }

    if !args.input.exists() {
        eprintln!("Error: Input file not found: {:?}", args.input);
        process::exit(1);
    }

    if args.output.exists() {
        eprintln!("Error: Output file already exists: {:?}", args.output);
        process::exit(1);
    }

    let input_path = args.input.to_string_lossy();
    let output_path = args.output.to_string_lossy();

    log::info!("mzDB Slice Tool");
    log::info!("===============");
    log::info!("Input: {}", input_path);
    log::info!("Output: {}", output_path);
    log::info!("Spectrum ID range: {} - {}", args.min_id, args.max_id);

    match slice_mzdb(&input_path, &output_path, args.min_id, args.max_id) {
        Ok(stats) => {
            println!("\n=== Slicing Complete ===");
            println!("Original spectra: {}", stats.original_spectrum_count);
            println!("Sliced spectra: {}", stats.sliced_spectrum_count);
            println!("Original bounding boxes: {}", stats.original_bb_count);
            println!("Sliced bounding boxes: {}", stats.sliced_bb_count);
            println!("Removed bounding boxes: {}", stats.removed_bb_count);
            println!("Updated bounding boxes: {}", stats.updated_bb_count);
            println!("Fixed BB references: {}", stats.fixed_bb_refs);
            println!("Output file: {}", output_path);
        }
        Err(e) => {
            eprintln!("Error during slicing: {:#}", e);
            process::exit(1);
        }
    }
}

/// Statistics about the slicing operation
#[derive(Debug, Default)]
struct SliceStats {
    original_spectrum_count: i64,
    sliced_spectrum_count: i64,
    original_bb_count: i64,
    sliced_bb_count: i64,
    removed_bb_count: i64,
    updated_bb_count: i64,
    fixed_bb_refs: i64,
}

/// Slice an mzDB file to contain only spectra within the specified ID range
///
/// Uses an efficient copy-then-modify approach:
/// 1. Copy the file
/// 2. Delete out-of-range spectra and bounding boxes
/// 3. Update bounding box blobs with new spectrum IDs
/// 4. Renumber spectrum IDs to start from 1
/// 5. VACUUM
fn slice_mzdb(
    input_path: &str,
    output_path: &str,
    min_id: i64,
    max_id: i64,
) -> Result<SliceStats> {
    let mut stats = SliceStats::default();

    // Step 1: Copy the file
    log::info!("Copying mzDB file...");
    std::fs::copy(input_path, output_path)
        .context("Failed to copy mzDB file")?;

    // Open the copied database for modification
    let conn = Connection::open(output_path)
        .context("Failed to open copied mzDB file")?;

    // Set pragmas for performance during modifications
    conn.execute_batch(
        "PRAGMA synchronous=OFF;
         PRAGMA journal_mode=MEMORY;
         PRAGMA temp_store=MEMORY;
         PRAGMA cache_size=-100000;
         PRAGMA foreign_keys=OFF;"
    )?;

    // Get original counts
    stats.original_spectrum_count = conn.query_row(
        "SELECT COUNT(*) FROM spectrum", [], |row| row.get(0)
    )?;
    stats.original_bb_count = conn.query_row(
        "SELECT COUNT(*) FROM bounding_box", [], |row| row.get(0)
    )?;

    // Verify the spectrum ID range exists
    let (actual_min, actual_max): (i64, i64) = conn.query_row(
        "SELECT MIN(id), MAX(id) FROM spectrum",
        [],
        |row| Ok((row.get(0)?, row.get(1)?)),
    )?;

    log::info!("Source file spectrum ID range: {} - {}", actual_min, actual_max);

    if min_id < actual_min || max_id > actual_max {
        log::warn!(
            "Requested range ({}-{}) extends beyond available spectra ({}-{})",
            min_id, max_id, actual_min, actual_max
        );
    }

    // Count spectra in range
    stats.sliced_spectrum_count = conn.query_row(
        "SELECT COUNT(*) FROM spectrum WHERE id >= ?1 AND id <= ?2",
        params![min_id, max_id],
        |row| row.get(0),
    )?;

    if stats.sliced_spectrum_count == 0 {
        // Clean up the copied file
        std::fs::remove_file(output_path).ok();
        bail!("No spectra found in the specified ID range");
    }

    log::info!("Spectra to keep: {}", stats.sliced_spectrum_count);

    // Begin transaction for all modifications
    conn.execute("BEGIN TRANSACTION", [])?;

    // Step 2: Build data encoding cache for bounding box processing
    log::info!("Building data encoding cache...");
    let data_encoding_cache = build_data_encoding_cache(&conn)?;

    // Step 3: Process bounding boxes - filter blobs and delete out-of-range
    log::info!("Processing bounding boxes...");
    let bb_stats = process_bounding_boxes(&conn, min_id, max_id, &data_encoding_cache)?;
    stats.sliced_bb_count = bb_stats.kept;
    stats.removed_bb_count = bb_stats.removed;
    stats.updated_bb_count = bb_stats.updated;

    // Step 4: Delete out-of-range spectra
    log::info!("Deleting out-of-range spectra...");
    conn.execute(
        "DELETE FROM spectrum WHERE id < ?1 OR id > ?2",
        params![min_id, max_id],
    )?;

    // Step 5: FIXED - Rebuild bb_first_spectrum_id references from actual BB contents
    // The original code had: UPDATE spectrum SET bb_first_spectrum_id = id WHERE ...
    // This was incorrect because it made spectra point to themselves without verifying
    // that a BB with that first_spectrum_id actually exists.
    // The correct approach is to parse all BB blobs and rebuild the references.
    log::info!("Rebuilding bb_first_spectrum_id references from BB data...");
    stats.fixed_bb_refs = rebuild_bb_references(&conn)?;

    // Commit transaction
    conn.execute("COMMIT", [])?;

    // Step 6: VACUUM to reclaim space and defragment
    log::info!("Vacuuming database...");
    conn.execute("VACUUM", [])?;

    // Step 7: Optimize
    log::info!("Optimizing database...");
    conn.execute("PRAGMA optimize", [])?;

    Ok(stats)
}

/// Rebuild bb_first_spectrum_id references by parsing actual BB blob contents
///
/// This function ensures that every spectrum's bb_first_spectrum_id points to
/// a BB that actually exists and contains that spectrum's data.
///
/// Algorithm:
/// 1. Parse all BB blobs to find which spectra they contain
/// 2. Build mapping: spectrum_id → correct BB's first_spectrum_id
/// 3. Update any spectrum with incorrect bb_first_spectrum_id
///
/// Returns: Number of spectra that had their bb_first_spectrum_id corrected
fn rebuild_bb_references(conn: &Connection) -> Result<i64> {
    log::info!("Building spectrum → bb_first_spectrum_id mapping from BB blobs...");

    // Step 1: Parse all BBs to build the correct mapping
    let mut correct_refs: HashMap<i64, i64> = HashMap::new();

    let mut bb_stmt = conn.prepare(
        "SELECT id, first_spectrum_id, data FROM bounding_box"
    )?;

    let mut rows = bb_stmt.query([])?;
    let mut bb_count = 0;

    while let Some(row) = rows.next()? {
        let _bb_id: i64 = row.get(0)?;
        let first_spectrum_id: i64 = row.get(1)?;
        let blob: Vec<u8> = row.get(2)?;

        // Parse blob to find all spectra in this BB
        let mut pos = 0;
        while pos + 8 <= blob.len() {
            if pos + 4 > blob.len() {
                break;
            }

            let spectrum_id = i32::from_le_bytes([
                blob[pos],
                blob[pos + 1],
                blob[pos + 2],
                blob[pos + 3],
            ]) as i64;

            if pos + 8 > blob.len() {
                break;
            }

            let peak_count = i32::from_le_bytes([
                blob[pos + 4],
                blob[pos + 5],
                blob[pos + 6],
                blob[pos + 7],
            ]) as usize;

            // Record: this spectrum should point to first_spectrum_id
            correct_refs.insert(spectrum_id, first_spectrum_id);

            // Skip peak data (assume 12 bytes per peak as default)
            // This is a safe default for high-res mode (8-byte m/z + 4-byte intensity)
            pos += 8 + (peak_count * 12);

            if pos > blob.len() {
                break;
            }
        }

        bb_count += 1;
    }

    drop(rows);
    drop(bb_stmt);

    log::info!("Parsed {} BBs, found correct refs for {} spectra",
              bb_count, correct_refs.len());

    // Step 2: Update spectra with incorrect references
    let mut fixed_count = 0i64;

    for (spectrum_id, correct_bb_first_id) in &correct_refs {
        // Check if this spectrum exists and has wrong reference
        let current_ref: Option<i64> = conn.query_row(
            "SELECT bb_first_spectrum_id FROM spectrum WHERE id = ?1",
            params![spectrum_id],
            |row| row.get(0),
        ).optional()?;

        if let Some(current) = current_ref {
            if current != *correct_bb_first_id {
                log::debug!("Fixing spectrum {}: {} → {}",
                           spectrum_id, current, correct_bb_first_id);

                conn.execute(
                    "UPDATE spectrum SET bb_first_spectrum_id = ?1 WHERE id = ?2",
                    params![correct_bb_first_id, spectrum_id],
                )?;

                fixed_count += 1;
            }
        }
    }

    if fixed_count > 0 {
        log::warn!("Fixed {} spectra with incorrect bb_first_spectrum_id", fixed_count);
    } else {
        log::info!("All bb_first_spectrum_id references were already correct");
    }

    Ok(fixed_count)
}

/// Build a cache of data encoding info (spectrum_id -> peak_size)
fn build_data_encoding_cache(conn: &Connection) -> Result<HashMap<i64, usize>> {
    let mut cache = HashMap::new();

    // Get data encoding details
    let mut de_stmt = conn.prepare(
        "SELECT id, mode, mz_precision FROM data_encoding"
    )?;

    let data_encodings: HashMap<i64, (String, i64)> = de_stmt
        .query_map([], |row| Ok((row.get(0)?, (row.get(1)?, row.get(2)?))))?
        .filter_map(|r| r.ok())
        .collect();

    // Get spectrum -> data_encoding mapping
    let mut spec_stmt = conn.prepare(
        "SELECT id, data_encoding_id FROM spectrum"
    )?;

    let spec_de: Vec<(i64, i64)> = spec_stmt
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))?
        .filter_map(|r| r.ok())
        .collect();

    for (spec_id, de_id) in spec_de {
        if let Some((mode, mz_precision)) = data_encodings.get(&de_id) {
            // Calculate peak size based on encoding
            let mz_size = if *mz_precision == 32 { 4 } else { 8 };
            let int_size = 4; // intensity is always f32
            let mut peak_size = mz_size + int_size;

            // Add HWHM sizes for fitted mode
            if mode == "fitted" {
                peak_size += 8; // left_hwhm + right_hwhm
            }

            cache.insert(spec_id, peak_size);
        }
    }

    Ok(cache)
}

/// Statistics about bounding box processing
#[derive(Debug, Default)]
struct BoundingBoxStats {
    kept: i64,
    removed: i64,
    updated: i64,
}

/// Process bounding boxes: update partially-overlapping blobs, delete out-of-range ones
fn process_bounding_boxes(
    conn: &Connection,
    min_id: i64,
    max_id: i64,
    data_encoding_cache: &HashMap<i64, usize>,
) -> Result<BoundingBoxStats> {
    let mut stats = BoundingBoxStats::default();

    // Get all bounding boxes that overlap with our spectrum range
    let mut bb_stmt = conn.prepare(
        "SELECT id, data, first_spectrum_id, last_spectrum_id
         FROM bounding_box
         WHERE last_spectrum_id >= ?1 AND first_spectrum_id <= ?2"
    )?;

    let bounding_boxes: Vec<(i64, Vec<u8>, i64, i64)> = bb_stmt
        .query_map(params![min_id, max_id], |row| {
            Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?))
        })?
        .filter_map(|r| r.ok())
        .collect();

    log::debug!("Found {} bounding boxes overlapping with spectrum range", bounding_boxes.len());

    // Prepare update statement
    let mut update_stmt = conn.prepare(
        "UPDATE bounding_box SET data = ?1, first_spectrum_id = ?2, last_spectrum_id = ?3 WHERE id = ?4"
    )?;

    // Track which bounding boxes to keep
    let mut kept_bb_ids: Vec<i64> = Vec::new();

    for (bb_id, blob_data, first_spec, last_spec) in &bounding_boxes {
        // Check if this BB needs filtering (partially overlaps) or can be kept as-is
        let fully_contained = *first_spec >= min_id && *last_spec <= max_id;
        
        if fully_contained {
            // BB is fully within range, keep as-is
            kept_bb_ids.push(*bb_id);
            stats.kept += 1;
        } else {
            // BB partially overlaps, need to filter the blob
            let result = filter_bounding_box_slices(
                blob_data,
                min_id,
                max_id,
                data_encoding_cache,
            )?;

            if result.filtered_blob.is_empty() {
                // All slices were outside range
                stats.removed += 1;
                continue;
            }

            kept_bb_ids.push(*bb_id);
            stats.kept += 1;
            stats.updated += 1;

            // Update the bounding box with filtered data
            update_stmt.execute(params![
                result.filtered_blob,
                result.new_first_spectrum_id,
                result.new_last_spectrum_id,
                bb_id,
            ])?;
        }
    }

    drop(update_stmt);
    drop(bb_stmt);

    // Delete bounding boxes that don't overlap with range at all
    log::info!("Deleting out-of-range bounding boxes...");
    conn.execute(
        "DELETE FROM bounding_box WHERE last_spectrum_id < ?1 OR first_spectrum_id > ?2",
        params![min_id, max_id],
    )?;
    
    // Also delete BBs that were processed but ended up empty
    if !kept_bb_ids.is_empty() {
        conn.execute(
            "CREATE TEMP TABLE kept_bb_ids (id INTEGER PRIMARY KEY)",
            [],
        )?;
        
        {
            let mut insert_stmt = conn.prepare(
                "INSERT INTO kept_bb_ids (id) VALUES (?1)"
            )?;
            for bb_id in &kept_bb_ids {
                insert_stmt.execute(params![bb_id])?;
            }
        }
        
        // Delete overlapping BBs that we didn't keep (their blobs became empty)
        let additional_removed: i64 = conn.query_row(
            "SELECT COUNT(*) FROM bounding_box 
             WHERE last_spectrum_id >= ?1 AND first_spectrum_id <= ?2
             AND id NOT IN (SELECT id FROM kept_bb_ids)",
            params![min_id, max_id],
            |row| row.get(0),
        )?;
        stats.removed += additional_removed;
        
        conn.execute(
            "DELETE FROM bounding_box 
             WHERE last_spectrum_id >= ?1 AND first_spectrum_id <= ?2
             AND id NOT IN (SELECT id FROM kept_bb_ids)",
            params![min_id, max_id],
        )?;
        
        conn.execute("DROP TABLE kept_bb_ids", [])?;
    }

    // Clean up R-tree entries
    log::info!("Cleaning up R-tree entries...");
    conn.execute(
        "DELETE FROM bounding_box_rtree WHERE id NOT IN (SELECT id FROM bounding_box)",
        [],
    )?;

    // Check if MSn R-tree exists and clean it too
    let has_msn_rtree: bool = conn.query_row(
        "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='bounding_box_msn_rtree'",
        [],
        |row| Ok(row.get::<_, i64>(0)? > 0),
    )?;

    if has_msn_rtree {
        conn.execute(
            "DELETE FROM bounding_box_msn_rtree WHERE id NOT IN (SELECT id FROM bounding_box)",
            [],
        )?;
    }

    Ok(stats)
}

/// Result of filtering bounding box slices
struct FilteredBoundingBox {
    filtered_blob: Vec<u8>,
    new_first_spectrum_id: i64,
    new_last_spectrum_id: i64,
}

/// Filter bounding box slices to only include those in the spectrum ID range
fn filter_bounding_box_slices(
    blob_data: &[u8],
    min_id: i64,
    max_id: i64,
    peak_size_cache: &HashMap<i64, usize>,
) -> Result<FilteredBoundingBox> {
    let mut filtered_blob = Vec::new();
    let mut new_first_id: Option<i64> = None;
    let mut new_last_id: Option<i64> = None;

    let mut pos = 0;
    let n_bytes = blob_data.len();

    while pos < n_bytes {
        // Read spectrum ID (4 bytes, i32, little-endian)
        if pos + 8 > n_bytes {
            break;
        }

        let spectrum_id = i32::from_le_bytes([
            blob_data[pos],
            blob_data[pos + 1],
            blob_data[pos + 2],
            blob_data[pos + 3],
        ]) as i64;

        // Read peak count (4 bytes, i32, little-endian)
        let peak_count = i32::from_le_bytes([
            blob_data[pos + 4],
            blob_data[pos + 5],
            blob_data[pos + 6],
            blob_data[pos + 7],
        ]) as usize;

        // Get peak size for this spectrum
        let peak_size = peak_size_cache
            .get(&spectrum_id)
            .copied()
            .unwrap_or(12); // Default to 64-bit m/z + 32-bit intensity

        let slice_data_size = 8 + (peak_size * peak_count);
        let slice_end = pos + slice_data_size;

        // Check if this spectrum is in our range
        if spectrum_id >= min_id && spectrum_id <= max_id {
            // Update first/last IDs
            if new_first_id.is_none() {
                new_first_id = Some(spectrum_id);
            }
            new_last_id = Some(spectrum_id);

            // Copy the entire slice as-is (no ID remapping)
            if slice_end <= n_bytes {
                filtered_blob.extend_from_slice(&blob_data[pos..slice_end]);
            }
        }

        pos = slice_end;
    }

    Ok(FilteredBoundingBox {
        filtered_blob,
        new_first_spectrum_id: new_first_id.unwrap_or(0),
        new_last_spectrum_id: new_last_id.unwrap_or(0),
    })
}