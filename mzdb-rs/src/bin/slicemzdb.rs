//! slicemzdb - Extract a slice of spectra from an mzDB file
//!
//! This command-line tool creates a subset of an mzDB file containing only
//! spectra within a specified ID range. The tool is useful for creating
//! smaller test files for integration testing and code evaluation.
//!
//! # Features
//!
//! - Preserves all metadata (run_slice table, instrument configuration, etc.)
//! - Renumbers spectrum IDs sequentially starting from 1
//! - Preserves initial_id to track original spectrum IDs
//! - Filters bounding boxes: removes those completely outside the range,
//!   updates those partially inside to only contain relevant slices
//! - Updates R-tree entries accordingly
//!
//! # Algorithm
//!
//! The tool uses an efficient copy-then-modify approach:
//! 1. Copy the entire mzDB file (fast sequential I/O)
//! 2. Delete spectra outside the range
//! 3. Renumber remaining spectrum IDs
//! 4. Update bounding box blobs to remove out-of-range slices
//! 5. Delete empty bounding boxes and their R-tree entries
//! 6. VACUUM to reclaim space
//!
//! # Usage
//!
//! ```bash
//! slicemzdb --input input.mzDB --output slice.mzDB --min-id 100 --max-id 200
//! ```

use std::collections::HashMap;
use std::path::PathBuf;
use std::process;

use anyhow_ext::{anyhow, Context, Result};
use clap::Parser;
use rusqlite::{params, Connection};

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
            println!("Output file: {}", output_path);
        }
        Err(e) => {
            eprintln!("Error during slicing: {}", e);
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
}

/// Slice an mzDB file to contain only spectra within the specified ID range
///
/// Uses an efficient copy-then-modify approach:
/// 1. Copy the file
/// 2. Delete out-of-range data
/// 3. Renumber IDs
/// 4. VACUUM
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
        return Err(anyhow!("No spectra found in the specified ID range"));
    }

    log::info!("Spectra to keep: {}", stats.sliced_spectrum_count);

    // Begin transaction for all modifications
    conn.execute("BEGIN TRANSACTION", [])?;

    // Step 2: Build ID mapping (old_id -> new_id)
    log::info!("Building spectrum ID mapping...");
    let id_mapping = build_spectrum_id_mapping(&conn, min_id, max_id)?;

    // Step 3: Build data encoding cache for bounding box processing
    log::info!("Building data encoding cache...");
    let data_encoding_cache = build_data_encoding_cache(&conn)?;

    // Step 4: Process bounding boxes - update blobs and track which to delete
    log::info!("Processing bounding boxes...");
    let bb_stats = process_bounding_boxes_inplace(&conn, min_id, max_id, &id_mapping, &data_encoding_cache)?;
    stats.sliced_bb_count = bb_stats.kept;
    stats.removed_bb_count = bb_stats.removed;
    stats.updated_bb_count = bb_stats.updated;

    // Step 5: Delete out-of-range spectra
    log::info!("Deleting out-of-range spectra...");
    conn.execute(
        "DELETE FROM spectrum WHERE id < ?1 OR id > ?2",
        params![min_id, max_id],
    )?;

    // Step 6: Renumber spectrum IDs and update references
    log::info!("Renumbering spectrum IDs...");
    renumber_spectra(&conn, &id_mapping)?;

    // Step 7: Update sqlite_sequence
    conn.execute(
        "UPDATE sqlite_sequence SET seq = ?1 WHERE name = 'spectrum'",
        [stats.sliced_spectrum_count],
    )?;
    conn.execute(
        "UPDATE sqlite_sequence SET seq = ?1 WHERE name = 'bounding_box'",
        [stats.sliced_bb_count],
    )?;

    // Commit transaction
    conn.execute("COMMIT", [])?;

    // Step 8: VACUUM to reclaim space and defragment
    log::info!("Vacuuming database...");
    conn.execute("VACUUM", [])?;

    // Step 9: Optimize
    log::info!("Optimizing database...");
    conn.execute("PRAGMA optimize", [])?;

    Ok(stats)
}

/// Build a mapping from old spectrum IDs to new (renumbered) IDs
fn build_spectrum_id_mapping(
    conn: &Connection,
    min_id: i64,
    max_id: i64,
) -> Result<HashMap<i64, i64>> {
    let mut stmt = conn.prepare(
        "SELECT id FROM spectrum WHERE id >= ?1 AND id <= ?2 ORDER BY id"
    )?;

    let old_ids: Vec<i64> = stmt
        .query_map(params![min_id, max_id], |row| row.get(0))?
        .filter_map(|r| r.ok())
        .collect();

    let mapping: HashMap<i64, i64> = old_ids
        .into_iter()
        .enumerate()
        .map(|(new_idx, old_id)| (old_id, (new_idx as i64) + 1))
        .collect();

    Ok(mapping)
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

/// Process bounding boxes in-place: update blobs, delete empty ones, update R-trees
fn process_bounding_boxes_inplace(
    conn: &Connection,
    min_id: i64,
    max_id: i64,
    id_mapping: &HashMap<i64, i64>,
    data_encoding_cache: &HashMap<i64, usize>,
) -> Result<BoundingBoxStats> {
    let mut stats = BoundingBoxStats::default();

    // First, count bounding boxes completely outside the range (these will be deleted)
    let outside_range_count: i64 = conn.query_row(
        "SELECT COUNT(*) FROM bounding_box WHERE last_spectrum_id < ?1 OR first_spectrum_id > ?2",
        params![min_id, max_id],
        |row| row.get(0),
    )?;
    stats.removed = outside_range_count;

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

    // Track which bounding boxes to keep (with their new IDs)
    let mut bb_id_mapping: HashMap<i64, i64> = HashMap::new();
    let mut new_bb_id: i64 = 0;

    for (old_bb_id, blob_data, _first_spec, _last_spec) in &bounding_boxes {
        // Filter the bounding box blob
        let result = filter_bounding_box_slices(
            blob_data,
            min_id,
            max_id,
            id_mapping,
            data_encoding_cache,
        )?;

        if result.filtered_blob.is_empty() {
            stats.removed += 1;
            continue;
        }

        new_bb_id += 1;
        bb_id_mapping.insert(*old_bb_id, new_bb_id);

        if result.was_updated {
            stats.updated += 1;
        }
        stats.kept += 1;

        // Update the bounding box in place
        update_stmt.execute(params![
            result.filtered_blob,
            result.new_first_spectrum_id,
            result.new_last_spectrum_id,
            old_bb_id,
        ])?;
    }

    drop(update_stmt);
    drop(bb_stmt);

    // Delete bounding boxes that are completely outside the range
    log::info!("Deleting out-of-range bounding boxes...");
    conn.execute(
        "DELETE FROM bounding_box WHERE last_spectrum_id < ?1 OR first_spectrum_id > ?2",
        params![min_id, max_id],
    )?;

    // Delete orphaned R-tree entries
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

    // Now renumber bounding box IDs
    log::info!("Renumbering bounding box IDs...");
    renumber_bounding_boxes(conn, &bb_id_mapping)?;

    Ok(stats)
}

/// Result of filtering bounding box slices
struct FilteredBoundingBox {
    filtered_blob: Vec<u8>,
    new_first_spectrum_id: i64,
    new_last_spectrum_id: i64,
    was_updated: bool,
}

/// Filter bounding box slices to only include those in the spectrum ID range
fn filter_bounding_box_slices(
    blob_data: &[u8],
    min_id: i64,
    max_id: i64,
    id_mapping: &HashMap<i64, i64>,
    peak_size_cache: &HashMap<i64, usize>,
) -> Result<FilteredBoundingBox> {
    let mut filtered_blob = Vec::new();
    let mut new_first_id: Option<i64> = None;
    let mut new_last_id: Option<i64> = None;
    let mut total_slices = 0;
    let mut kept_slices = 0;

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

        total_slices += 1;

        // Get peak size for this spectrum
        let peak_size = peak_size_cache
            .get(&spectrum_id)
            .copied()
            .unwrap_or(12); // Default to 64-bit m/z + 32-bit intensity

        let slice_data_size = 8 + (peak_size * peak_count);
        let slice_end = pos + slice_data_size;

        // Check if this spectrum is in our range
        if spectrum_id >= min_id && spectrum_id <= max_id {
            // Get the new spectrum ID from mapping
            if let Some(&new_spec_id) = id_mapping.get(&spectrum_id) {
                // Update first/last IDs
                if new_first_id.is_none() {
                    new_first_id = Some(new_spec_id);
                }
                new_last_id = Some(new_spec_id);

                kept_slices += 1;

                // Write the slice with new spectrum ID
                filtered_blob.extend_from_slice(&(new_spec_id as i32).to_le_bytes());
                filtered_blob.extend_from_slice(&(peak_count as i32).to_le_bytes());

                // Copy peak data as-is
                if slice_end <= n_bytes {
                    filtered_blob.extend_from_slice(&blob_data[pos + 8..slice_end]);
                }
            }
        }

        pos = slice_end;
    }

    Ok(FilteredBoundingBox {
        filtered_blob,
        new_first_spectrum_id: new_first_id.unwrap_or(0),
        new_last_spectrum_id: new_last_id.unwrap_or(0),
        was_updated: kept_slices != total_slices,
    })
}

/// Renumber spectrum IDs in place
fn renumber_spectra(conn: &Connection, id_mapping: &HashMap<i64, i64>) -> Result<()> {
    // Create a temporary table with the mapping
    conn.execute(
        "CREATE TEMP TABLE id_map (old_id INTEGER PRIMARY KEY, new_id INTEGER)",
        [],
    )?;

    {
        let mut insert_stmt = conn.prepare(
            "INSERT INTO id_map (old_id, new_id) VALUES (?1, ?2)"
        )?;

        for (old_id, new_id) in id_mapping {
            insert_stmt.execute(params![old_id, new_id])?;
        }
    }

    // Update spectrum IDs using the mapping
    // First, use negative IDs to avoid conflicts
    conn.execute(
        "UPDATE spectrum SET id = -(SELECT new_id FROM id_map WHERE old_id = spectrum.id)",
        [],
    )?;

    // Then make them positive
    conn.execute("UPDATE spectrum SET id = -id", [])?;

    // Update bb_first_spectrum_id references
    conn.execute(
        "UPDATE spectrum SET bb_first_spectrum_id = (
            SELECT new_id FROM id_map WHERE old_id = spectrum.bb_first_spectrum_id
        ) WHERE bb_first_spectrum_id IN (SELECT old_id FROM id_map)",
        [],
    )?;

    // Drop the temporary table
    conn.execute("DROP TABLE id_map", [])?;

    Ok(())
}

/// Renumber bounding box IDs in place
fn renumber_bounding_boxes(conn: &Connection, bb_id_mapping: &HashMap<i64, i64>) -> Result<()> {
    if bb_id_mapping.is_empty() {
        return Ok(());
    }

    // Create a temporary table with the mapping
    conn.execute(
        "CREATE TEMP TABLE bb_id_map (old_id INTEGER PRIMARY KEY, new_id INTEGER)",
        [],
    )?;

    {
        let mut insert_stmt = conn.prepare(
            "INSERT INTO bb_id_map (old_id, new_id) VALUES (?1, ?2)"
        )?;

        for (old_id, new_id) in bb_id_mapping {
            insert_stmt.execute(params![old_id, new_id])?;
        }
    }

    // Update bounding_box IDs using negative values to avoid conflicts
    conn.execute(
        "UPDATE bounding_box SET id = -(SELECT new_id FROM bb_id_map WHERE old_id = bounding_box.id)
         WHERE id IN (SELECT old_id FROM bb_id_map)",
        [],
    )?;
    conn.execute(
        "UPDATE bounding_box SET id = -id WHERE id < 0",
        [],
    )?;

    // Update R-tree IDs
    conn.execute(
        "UPDATE bounding_box_rtree SET id = -(SELECT new_id FROM bb_id_map WHERE old_id = bounding_box_rtree.id)
         WHERE id IN (SELECT old_id FROM bb_id_map)",
        [],
    )?;
    conn.execute(
        "UPDATE bounding_box_rtree SET id = -id WHERE id < 0",
        [],
    )?;

    // Check if MSn R-tree exists and update it too
    let has_msn_rtree: bool = conn.query_row(
        "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='bounding_box_msn_rtree'",
        [],
        |row| Ok(row.get::<_, i64>(0)? > 0),
    )?;

    if has_msn_rtree {
        conn.execute(
            "UPDATE bounding_box_msn_rtree SET id = -(SELECT new_id FROM bb_id_map WHERE old_id = bounding_box_msn_rtree.id)
             WHERE id IN (SELECT old_id FROM bb_id_map)",
            [],
        )?;
        conn.execute(
            "UPDATE bounding_box_msn_rtree SET id = -id WHERE id < 0",
            [],
        )?;
    }

    // Drop the temporary table
    conn.execute("DROP TABLE bb_id_map", [])?;

    Ok(())
}