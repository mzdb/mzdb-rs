//! mzdb2peakeldb - Detect peakels from mzDB files
//!
//! This command-line tool extracts chromatographic peaks (peakels) from mzDB files
//! and exports them to a peakelDB SQLite database.
//!
//! Supports both:
//! - MS1 peakel detection (default)
//! - MS2 peakel detection for DIA data (--ms-level 2)
//!
//! # Usage
//!
//! ```bash
//! # MS1 peakel detection (default)
//! mzdb2peakeldb -i input.mzDB -o output.peakeldb
//!
//! # MS2 peakel detection for DIA data
//! mzdb2peakeldb -i input.mzDB -o output.peakeldb --ms-level 2
//!
//! # Use 4 threads for parallel processing
//! mzdb2peakeldb -i input.mzDB -o output.peakeldb --threads 4
//!
//! # Use all available CPUs
//! mzdb2peakeldb -i input.mzDB -o output.peakeldb --threads auto
//! ```

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use anyhow_ext::{anyhow, Context, Result, bail};
use clap::Parser;
use rusqlite::{Connection, params};
use smallvec::SmallVec;

use mzdb::MzDbReader;

#[cfg(feature = "processing")]
use mzdb::processing::{
    SmartPeakelFinder, SmartPeakelFinderConfig, PeakelFinder, BasicPeakelFinder,
    Peakel,
    // DIA types
    DiaMs2PeakelDetector, DiaMs2PeakelConfig, IsolationWindow,
    DiaMs2PeakelRecord, write_dia_peakels_tsv,
};

/// Detect peakels from mzDB files and export to peakelDB
#[derive(Parser, Debug)]
#[command(name = "mzdb2peakeldb")]
#[command(author = "mzdb-rs")]
#[command(version = "0.3.0")]
#[command(about = "Detect MS1 or MS2 peakels from mzDB files", long_about = None)]
struct Args {
    /// Path to the mzDB file
    #[arg(short = 'i', long = "input")]
    mzdb_file_path: PathBuf,

    /// Output peakelDB file path (SQLite database or TSV)
    #[arg(short = 'o', long = "output")]
    output_file_path: PathBuf,

    /// MS level to process: 1 for MS1 peakels, 2 for DIA MS2 peakels
    #[arg(long = "ms-level", default_value = "1")]
    ms_level: u8,

    /// m/z tolerance in PPM for XIC extraction
    #[arg(long = "mz-tol", default_value = "10.0")]
    mz_tol_ppm: f64,

    /// Minimum intensity threshold for peak detection
    #[arg(long = "min-intensity", default_value = "0.0")]
    min_intensity: f32,

    /// Minimum number of points per peakel
    #[arg(long = "min-peaks", default_value = "5")]
    min_peaks: usize,

    /// Maximum consecutive gaps in XIC before stopping walk
    #[arg(long = "max-gaps", default_value = "3")]
    max_consecutive_gaps: usize,

    /// Intensity percentile threshold (0.0-1.0) for MS1 detection
    #[arg(long = "intensity-pct", default_value = "0.9")]
    intensity_percentile: f32,

    /// Algorithm: 'basic' or 'smart'
    #[arg(long = "algo", default_value = "smart")]
    algo: String,

    /// Export format: 'sqlite' or 'tsv'
    #[arg(long = "format", default_value = "sqlite")]
    format: String,

    /// Number of threads: 1 for single-threaded, 'auto' for all CPUs, or a specific number
    #[arg(long = "threads", default_value = "1")]
    threads: String,

    /// Verbosity level (-v, -vv, -vvv)
    #[arg(short, long, action = clap::ArgAction::Count)]
    verbose: u8,
}

/// Configuration for MS1 peakel detection
#[derive(Debug, Clone)]
pub struct Ms1PeakelDetectionConfig {
    pub mz_tol_ppm: f64,
    pub min_peaks_count: usize,
    pub intensity_percentile: f32,
    pub max_consecutive_gaps: usize,
    pub max_time_window: f32,
    pub algorithm: String,
}

impl Default for Ms1PeakelDetectionConfig {
    fn default() -> Self {
        Self {
            mz_tol_ppm: 10.0,
            min_peaks_count: 5,
            intensity_percentile: 0.9,
            max_consecutive_gaps: 3,
            max_time_window: 1200.0,
            algorithm: "smart".to_string(),
        }
    }
}

/// Parse and validate the threads parameter, returns the number of threads to use
fn parse_threads(threads_arg: &str) -> Result<usize> {
    let available_cpus = std::thread::available_parallelism()
        .map(|p| p.get())
        .unwrap_or(1);
    
    let requested = if threads_arg.eq_ignore_ascii_case("auto") {
        available_cpus
    } else {
        threads_arg.parse::<usize>()
            .map_err(|_| anyhow!("Invalid threads value '{}'. Use a positive number or 'auto'", threads_arg))?
    };
    
    if requested < 1 {
        bail!("Threads must be at least 1, got {}", requested);
    }
    
    if requested > available_cpus {
        eprintln!("Warning: Requested {} threads but only {} CPUs available. Capping to {}.", 
                  requested, available_cpus, available_cpus);
        Ok(available_cpus)
    } else {
        Ok(requested)
    }
}

fn main() -> Result<()> {
    let args = Args::parse();

    // Initialize logging
    let log_level = match args.verbose {
        0 => log::LevelFilter::Warn,
        1 => log::LevelFilter::Info,
        2 => log::LevelFilter::Debug,
        _ => log::LevelFilter::Trace,
    };
    
    env_logger::Builder::new()
        .filter_level(log_level)
        .format_timestamp_millis()
        .init();

    // Parse and validate threads parameter
    let num_threads = parse_threads(&args.threads)?;
    
    // Check if parallel processing is available
    if num_threads > 1 {
        #[cfg(feature = "processing-parallel")]
        {
            println!("Parallel processing enabled with {} threads", num_threads);
        }
        #[cfg(not(feature = "processing-parallel"))]
        {
            eprintln!("Warning: Parallel processing requested but 'processing-parallel' feature is not enabled.");
            eprintln!("Rebuild with: cargo build --features processing-parallel");
            eprintln!("Falling back to single-threaded mode.");
        }
    }

    // Print configuration
    println!("mzdb2peakeldb - Peakel Detection");
    println!("================================");
    println!("Input mzDB: {:?}", args.mzdb_file_path);
    println!("Output: {:?}", args.output_file_path);
    println!("MS level: {}", args.ms_level);
    println!("m/z tolerance: {} ppm", args.mz_tol_ppm);
    println!("Min intensity: {}", args.min_intensity);
    println!("Min peaks per peakel: {}", args.min_peaks);
    println!("Max consecutive gaps: {}", args.max_consecutive_gaps);
    println!("Algorithm: {}", args.algo);
    println!("Output format: {}", args.format);
    println!("Threads: {}", num_threads);
    println!();

    // Open the mzDB file
    println!("Opening mzDB file...");
    let reader = MzDbReader::open(args.mzdb_file_path.to_str().unwrap())?;
    let headers = reader.get_spectrum_headers();
    
    println!("Total spectra: {}", headers.len());
    
    // Count MS levels
    let ms1_count = headers.iter().filter(|h| h.ms_level == 1).count();
    let ms2_count = headers.iter().filter(|h| h.ms_level == 2).count();
    println!("MS1 spectra: {}", ms1_count);
    println!("MS2 spectra: {}", ms2_count);
    println!();

    match args.ms_level {
        1 => {
            #[cfg(feature = "processing")]
            {
                run_ms1_detection(&args, &reader, num_threads)?;
            }
            #[cfg(not(feature = "processing"))]
            {
                eprintln!("Error: MS1 peakel detection requires the 'processing' feature.");
                eprintln!("Please rebuild with: cargo build --features processing");
                std::process::exit(1);
            }
        }
        2 => {
            #[cfg(feature = "processing")]
            {
                run_ms2_dia_detection(&args, &reader, num_threads)?;
            }
            #[cfg(not(feature = "processing"))]
            {
                eprintln!("Error: MS2 DIA peakel detection requires the 'processing' feature.");
                eprintln!("Please rebuild with: cargo build --features processing");
                std::process::exit(1);
            }
        }
        _ => {
            eprintln!("Error: Invalid MS level {}. Supported values: 1, 2", args.ms_level);
            std::process::exit(1);
        }
    }

    println!();
    println!("Done!");
    
    Ok(())
}

// ============================================================================
// MS1 Peakel Detection
// ============================================================================

#[cfg(feature = "processing")]
fn run_ms1_detection(args: &Args, reader: &MzDbReader, num_threads: usize) -> Result<()> {
    let config = Ms1PeakelDetectionConfig {
        mz_tol_ppm: args.mz_tol_ppm,
        min_peaks_count: args.min_peaks,
        intensity_percentile: args.intensity_percentile,
        max_consecutive_gaps: args.max_consecutive_gaps,
        max_time_window: 1200.0,
        algorithm: args.algo.clone(),
    };

    println!("Detecting MS1 peakels...");
    let peakels = detect_ms1_peakels(reader, &config, num_threads)?;
    
    if peakels.is_empty() {
        println!("No peakels detected. Check if the file contains MS1 data.");
        return Ok(());
    }

    println!("Detected {} MS1 peakels", peakels.len());

    // Check if mzDB file is DIA
    let is_dia = reader.is_dia().unwrap_or(false);
    println!("DIA experiment: {}", is_dia);

    // Get input filename for metadata
    let input_filename = args.mzdb_file_path
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or("unknown.mzDB");

    // Write output
    match args.format.as_str() {
        "sqlite" => {
            println!("Writing SQLite peakelDB...");
            write_ms1_peakeldb(&args.output_file_path, input_filename, is_dia, &peakels)?;
        }
        "tsv" => {
            println!("Writing TSV file...");
            write_ms1_peakels_tsv(&args.output_file_path, &peakels)?;
        }
        _ => {
            eprintln!("Warning: Unknown format '{}', defaulting to SQLite", args.format);
            write_ms1_peakeldb(&args.output_file_path, input_filename, is_dia, &peakels)?;
        }
    }

    println!("Output written to: {:?}", args.output_file_path);

    // Print statistics
    print_ms1_statistics(&peakels);

    Ok(())
}

#[cfg(feature = "processing")]
fn detect_ms1_peakels(mzdb: &MzDbReader, config: &Ms1PeakelDetectionConfig, num_threads: usize) -> Result<Vec<Peakel>> {
    // Get MS1 spectrum headers sorted by time
    let headers = mzdb.get_spectrum_headers();
    let ms1_headers: Vec<_> = headers.iter()
        .filter(|h| h.ms_level == 1)
        .collect();
    
    println!("  Found {} MS1 spectra", ms1_headers.len());
    
    if ms1_headers.is_empty() {
        return Ok(vec![]);
    }
    
    // Build a map of all peaks organized by approximate m/z bins
    let mz_bin_size = config.mz_tol_ppm * 0.001;
    
    println!("  Collecting peaks from MS1 spectra...");
    let mut peaks_by_mz_bin: BTreeMap<i64, Vec<(f64, f32, i64, f32)>> = BTreeMap::new();
    
    for header in &ms1_headers {
        let spectrum = mzdb.get_spectrum(header.id)
            .with_context(|| format!("Failed to read spectrum {}", header.id))?;
        
        let rt = header.time * 60.0; // Convert to seconds
        
        for (&mz, &intensity) in spectrum.data.mz_array.iter().zip(spectrum.data.intensity_array.iter()) {
            let bin = (mz / mz_bin_size) as i64;
            peaks_by_mz_bin.entry(bin)
                .or_default()
                .push((mz, intensity, header.id, rt));
        }
    }
    
    println!("  Found {} m/z bins with peaks", peaks_by_mz_bin.len());
    
    // Convert to Vec for parallel processing
    let bins: Vec<(i64, Vec<(f64, f32, i64, f32)>)> = peaks_by_mz_bin.into_iter().collect();
    let total_bins = bins.len();
    
    println!("  Detecting peakels in each m/z bin...");
    
    // Choose between parallel and sequential processing
    let all_peakels = if num_threads > 1 {
        #[cfg(feature = "processing-parallel")]
        {
            detect_peakels_parallel(&bins, config, total_bins, num_threads)
        }
        #[cfg(not(feature = "processing-parallel"))]
        {
            detect_peakels_sequential(&bins, config, total_bins)
        }
    } else {
        detect_peakels_sequential(&bins, config, total_bins)
    };
    
    println!("  Detected {} peakels total", all_peakels.len());
    
    Ok(all_peakels)
}

/// Sequential peakel detection (single-threaded)
#[cfg(feature = "processing")]
fn detect_peakels_sequential(
    bins: &[(i64, Vec<(f64, f32, i64, f32)>)],
    config: &Ms1PeakelDetectionConfig,
    total_bins: usize,
) -> Vec<Peakel> {
    // Create peakel finder
    let finder: Box<dyn PeakelFinder> = match config.algorithm.as_str() {
        "smart" => {
            let finder_config = SmartPeakelFinderConfig {
                min_peaks_count: config.min_peaks_count,
                use_smoothing: true,
                use_baseline_remover: false,
                ..Default::default()
            };
            Box::new(SmartPeakelFinder::with_config(finder_config))
        }
        _ => Box::new(BasicPeakelFinder::default_params()),
    };
    
    let mut all_peakels = Vec::new();
    let mut bins_processed = 0;
    
    for (_bin, peaks) in bins {
        bins_processed += 1;
        if bins_processed % 10000 == 0 {
            println!("    Processed {}/{} bins, found {} peakels so far", 
                bins_processed, total_bins, all_peakels.len());
        }
        
        let mut bin_peakels = process_single_bin(peaks, config, finder.as_ref());
        all_peakels.append(&mut bin_peakels);
    }
    
    all_peakels
}

/// Parallel peakel detection using producer-consumer pattern
/// 
/// Strategy: Use a bounded queue where producer sends batches of bins
/// and consumer threads process them. Memory is bounded by queue size.
#[cfg(all(feature = "processing", feature = "processing-parallel"))]
fn detect_peakels_parallel(
    bins: &[(i64, Vec<(f64, f32, i64, f32)>)],
    config: &Ms1PeakelDetectionConfig,
    total_bins: usize,
    num_threads: usize,
) -> Vec<Peakel> {
    use crossbeam_channel::bounded;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Mutex;
    
    let queue_size = num_threads * 2;
    let batch_size = std::cmp::max(100, total_bins / (num_threads * 4)); // Adaptive batch size
    
    println!("    Using {} threads with queue size {} and batch size {}", 
             num_threads, queue_size, batch_size);
    
    // Create bounded channel for bin batches
    type BinBatch = Vec<(i64, Vec<(f64, f32, i64, f32)>)>;
    let (tx, rx) = bounded::<BinBatch>(queue_size);
    
    // Shared results collector and progress counter
    let results: Mutex<Vec<Peakel>> = Mutex::new(Vec::new());
    let bins_processed = AtomicUsize::new(0);
    
    // Use std::thread::scope for scoped threads
    std::thread::scope(|scope| {
        // Spawn consumer threads
        for _ in 0..num_threads {
            let rx = rx.clone();
            let results = &results;
            let bins_processed = &bins_processed;
            
            scope.spawn(move || {
                // Create thread-local peakel finder
                let finder: Box<dyn PeakelFinder + Send + Sync> = match config.algorithm.as_str() {
                    "smart" => {
                        let finder_config = SmartPeakelFinderConfig {
                            min_peaks_count: config.min_peaks_count,
                            use_smoothing: true,
                            use_baseline_remover: false,
                            ..Default::default()
                        };
                        Box::new(SmartPeakelFinder::with_config(finder_config))
                    }
                    _ => Box::new(BasicPeakelFinder::default_params()),
                };
                
                let mut thread_peakels: Vec<Peakel> = Vec::new();
                
                // Process batches until channel is closed
                while let Ok(batch) = rx.recv() {
                    for (_bin_id, peaks) in batch {
                        let mut bin_peakels = process_single_bin(&peaks, config, finder.as_ref());
                        thread_peakels.append(&mut bin_peakels);
                        
                        let count = bins_processed.fetch_add(1, Ordering::Relaxed);
                        if count % 10000 == 0 {
                            println!("    Processed ~{}/{} bins", count, total_bins);
                        }
                    }
                }
                
                // Collect results
                if let Ok(mut guard) = results.lock() {
                    guard.append(&mut thread_peakels);
                }
            });
        }
        
        // Drop extra receiver clone
        drop(rx);
        
        // Producer: send bins in batches
        let mut current_batch: BinBatch = Vec::with_capacity(batch_size);
        
        for (bin_id, peaks) in bins {
            current_batch.push((*bin_id, peaks.clone()));
            
            if current_batch.len() >= batch_size {
                // Send batch (blocks if queue is full)
                let batch_to_send = std::mem::take(&mut current_batch);
                if tx.send(batch_to_send).is_err() {
                    println!("    Warning: Failed to send batch to queue");
                    break;
                }
                current_batch.reserve(batch_size);
            }
        }
        
        // Send remaining bins
        if !current_batch.is_empty() {
            let _ = tx.send(current_batch);
        }
        
        // Drop sender to signal completion
        drop(tx);
        
        // Threads are automatically joined when scope exits
    });
    
    // Extract results
    results.into_inner().unwrap_or_default()
}

/// Process a single m/z bin to find peakels
#[cfg(feature = "processing")]
fn process_single_bin(
    peaks: &[(f64, f32, i64, f32)],
    config: &Ms1PeakelDetectionConfig,
    finder: &dyn PeakelFinder,
) -> Vec<Peakel> {
    if peaks.len() < config.min_peaks_count {
        return Vec::new();
    }
    
    // Sort peaks by retention time
    let mut sorted_peaks = peaks.to_vec();
    sorted_peaks.sort_by(|a, b| a.3.partial_cmp(&b.3).unwrap());
    
    // Build RT-intensity pairs for peakel detection
    let rt_int_pairs: Vec<(f32, f64)> = sorted_peaks.iter()
        .map(|&(_, intensity, _, rt)| (rt, intensity as f64))
        .collect();
    
    // Find peakels
    let peakel_indices = finder.find_peakels_indices(&rt_int_pairs);
    
    // Convert detected indices to Peakel objects
    let mut bin_peakels = Vec::new();
    for (start, end) in peakel_indices {
        if end - start + 1 < config.min_peaks_count {
            continue;
        }
        
        let peakel_peaks = &sorted_peaks[start..=end];
        
        let spectrum_ids: SmallVec<[i64; 16]> = peakel_peaks.iter().map(|p| p.2).collect();
        let elution_times: SmallVec<[f32; 16]> = peakel_peaks.iter().map(|p| p.3).collect();
        let mz_values: SmallVec<[f64; 16]> = peakel_peaks.iter().map(|p| p.0).collect();
        let intensity_values: SmallVec<[f32; 16]> = peakel_peaks.iter().map(|p| p.1).collect();
        
        let peakel = Peakel::new(
            spectrum_ids,
            elution_times,
            mz_values,
            intensity_values,
            None,
            None,
        );
        
        bin_peakels.push(peakel);
    }
    
    bin_peakels
}

#[cfg(feature = "processing")]
fn write_ms1_peakeldb<P: AsRef<Path>>(path: P, mzdb_filename: &str, is_dia: bool, peakels: &[Peakel]) -> Result<()> {
    // Remove existing file if present
    if path.as_ref().exists() {
        std::fs::remove_file(path.as_ref())?;
    }

    let conn = Connection::open(path.as_ref())
        .context("Failed to create peakelDB file")?;

    // SQLite optimizations
    conn.execute_batch(
        "PRAGMA synchronous=OFF;
         PRAGMA journal_mode=OFF;
         PRAGMA temp_store=2;
         PRAGMA cache_size=100000;"
    )?;

    // Create schema
    conn.execute_batch(MS1_PEAKELDB_SCHEMA)?;

    // Insert peakeldb_file record
    let now = chrono_lite_timestamp();
    conn.execute(
        "INSERT INTO peakeldb_file (id, name, description, raw_file_name, is_dia_experiment, 
         creation_timestamp, modification_timestamp) VALUES (?, ?, ?, ?, ?, ?, ?)",
        params![1, mzdb_filename, "Generated by mzdb2peakeldb", mzdb_filename, is_dia, &now, &now],
    )?;

    // Insert lcms_map record
    conn.execute(
        "INSERT INTO lcms_map (id, ms_level, peakel_count, peakeldb_file_id) VALUES (?, ?, ?, ?)",
        params![1, 1, peakels.len() as i32, 1],
    )?;

    // Insert peakels
    conn.execute("BEGIN TRANSACTION", [])?;
    
    let mut peakel_stmt = conn.prepare(
        "INSERT INTO peakel (id, moz, elution_time, duration, gap_count, apex_intensity, area,
         amplitude, intensity_cv, left_hwhm_mean, left_hwhm_cv, right_hwhm_mean, right_hwhm_cv,
         is_interfering, peak_count, peaks, serialized_properties,
         first_spectrum_id, apex_spectrum_id, last_spectrum_id, map_id)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
    )?;

    let mut rtree_stmt = conn.prepare(
        "INSERT INTO peakel_rtree (id, min_mz, max_mz, min_time, max_time, min_intensity, max_intensity)
         VALUES (?, ?, ?, ?, ?, ?, ?)"
    )?;

    for (idx, peakel) in peakels.iter().enumerate() {
        let peakel_id = (idx + 1) as i64;
        
        let mz = peakel.calc_mz();
        let elution_time = peakel.apex_elution_time();
        let duration = peakel.calc_duration();
        let apex_intensity = peakel.apex_intensity();
        let area = peakel.area();
        let peak_count = peakel.peaks_count() as i32;
        
        let min_mz = peakel.mz_values.iter().cloned().fold(f64::INFINITY, f64::min);
        let max_mz = peakel.mz_values.iter().cloned().fold(f64::NEG_INFINITY, f64::max);
        let min_time = peakel.elution_times.iter().cloned().fold(f32::INFINITY, f32::min);
        let max_time = peakel.elution_times.iter().cloned().fold(f32::NEG_INFINITY, f32::max);
        let min_intensity = peakel.intensity_values.iter().cloned().fold(f32::INFINITY, f32::min);

        let peaks_blob = serialize_ms1_peakel_data(peakel)?;
        let left_hwhm_mean = peakel.left_hwhm_mean();
        let right_hwhm_mean = peakel.right_hwhm_mean();
        let first_spectrum_id = peakel.spectrum_ids.first().copied().unwrap_or(0);
        let apex_spectrum_id = peakel.apex_spectrum_id();
        let last_spectrum_id = peakel.spectrum_ids.last().copied().unwrap_or(0);
        let amplitude = apex_intensity / min_intensity.max(1.0);

        // Use Option for nullable HWHM values (null if not computed)
        let left_hwhm_opt: Option<f32> = if left_hwhm_mean > 0.0 { Some(left_hwhm_mean as f32) } else { None };
        let right_hwhm_opt: Option<f32> = if right_hwhm_mean > 0.0 { Some(right_hwhm_mean as f32) } else { None };

        peakel_stmt.execute(params![
            peakel_id,
            mz,
            elution_time,
            duration,
            0, // gap_count
            apex_intensity,
            area,
            amplitude,
            0.0, // intensity_cv
            left_hwhm_opt,
            Option::<f32>::None, // left_hwhm_cv
            right_hwhm_opt,
            Option::<f32>::None, // right_hwhm_cv
            false, // is_interfering
            peak_count,
            peaks_blob,
            Option::<String>::None, // serialized_properties
            first_spectrum_id,
            apex_spectrum_id,
            last_spectrum_id,
            1, // map_id
        ])?;

        rtree_stmt.execute(params![
            peakel_id,
            min_mz,
            max_mz,
            min_time,
            max_time,
            min_intensity as f64,
            apex_intensity as f64,
        ])?;
    }

    conn.execute("COMMIT", [])?;
    
    println!("SQLite peakelDB created with {} peakels", peakels.len());
    
    Ok(())
}

#[cfg(feature = "processing")]
fn serialize_ms1_peakel_data(peakel: &Peakel) -> Result<Vec<u8>> {
    let n = peakel.peaks_count();
    let mut data = Vec::with_capacity(4 + n * (8 + 4 + 8 + 4));
    
    data.extend_from_slice(&(n as u32).to_le_bytes());
    
    for &id in &peakel.spectrum_ids {
        data.extend_from_slice(&id.to_le_bytes());
    }
    for &time in &peakel.elution_times {
        data.extend_from_slice(&time.to_le_bytes());
    }
    for &mz in &peakel.mz_values {
        data.extend_from_slice(&mz.to_le_bytes());
    }
    for &intensity in &peakel.intensity_values {
        data.extend_from_slice(&intensity.to_le_bytes());
    }
    
    Ok(data)
}

#[cfg(feature = "processing")]
fn write_ms1_peakels_tsv<P: AsRef<Path>>(path: P, peakels: &[Peakel]) -> Result<()> {
    use std::io::Write;
    use std::fs::File;
    
    let mut file = File::create(path)?;
    
    writeln!(file, "id\tmz\telution_time\tduration\tapex_intensity\tarea\tpeaks_count\tfirst_spectrum_id\tapex_spectrum_id\tlast_spectrum_id")?;
    
    for (idx, peakel) in peakels.iter().enumerate() {
        writeln!(
            file,
            "{}\t{:.6}\t{:.4}\t{:.4}\t{:.2}\t{:.2}\t{}\t{}\t{}\t{}",
            idx + 1,
            peakel.calc_mz(),
            peakel.apex_elution_time(),
            peakel.calc_duration(),
            peakel.apex_intensity(),
            peakel.area(),
            peakel.peaks_count(),
            peakel.spectrum_ids.first().copied().unwrap_or(0),
            peakel.apex_spectrum_id(),
            peakel.spectrum_ids.last().copied().unwrap_or(0),
        )?;
    }
    
    println!("TSV file created with {} peakels", peakels.len());
    
    Ok(())
}

#[cfg(feature = "processing")]
fn print_ms1_statistics(peakels: &[Peakel]) {
    if peakels.is_empty() {
        return;
    }

    let total_area: f64 = peakels.iter().map(|p| p.area() as f64).sum();
    let avg_duration: f32 = peakels.iter().map(|p| p.calc_duration()).sum::<f32>() 
        / peakels.len() as f32;
    let avg_peaks: f32 = peakels.iter().map(|p| p.peaks_count() as f32).sum::<f32>() 
        / peakels.len() as f32;
    
    let min_mz = peakels.iter().map(|p| p.calc_mz()).fold(f64::INFINITY, f64::min);
    let max_mz = peakels.iter().map(|p| p.calc_mz()).fold(f64::NEG_INFINITY, f64::max);
    let min_rt = peakels.iter().map(|p| p.apex_elution_time()).fold(f32::INFINITY, f32::min);
    let max_rt = peakels.iter().map(|p| p.apex_elution_time()).fold(f32::NEG_INFINITY, f32::max);
    
    println!();
    println!("=== MS1 Peakel Statistics ===");
    println!("Total peakels: {}", peakels.len());
    println!("Total area: {:.2e}", total_area);
    println!("Average duration: {:.2}s", avg_duration);
    println!("Average peaks per peakel: {:.1}", avg_peaks);
    println!("m/z range: {:.2} - {:.2}", min_mz, max_mz);
    println!("RT range: {:.2}s - {:.2}s", min_rt, max_rt);
}

// ============================================================================
// MS2 DIA Peakel Detection
// ============================================================================

#[cfg(feature = "processing")]
fn run_ms2_dia_detection(args: &Args, reader: &MzDbReader, num_threads: usize) -> Result<()> {
    let config = DiaMs2PeakelConfig {
        mz_tol_ppm: args.mz_tol_ppm,
        min_intensity: args.min_intensity,
        min_peaks: args.min_peaks,
        max_consecutive_gaps: args.max_consecutive_gaps,
        max_time_window: 1200.0,
        algorithm: args.algo.clone(),
    };
    
    let detector = DiaMs2PeakelDetector::with_config(config);
    
    println!("Detecting MS2 peakels (DIA mode)...");
    let (windows, peakels) = detector.detect_all_peakels_with_threads(reader, num_threads)?;
    
    println!();
    println!("=== DIA MS2 Peakel Detection Results ===");
    println!("Isolation windows: {}", windows.len());
    println!("Total MS2 peakels: {}", peakels.len());
    
    // Print per-window statistics
    println!();
    println!("Peakels per isolation window:");
    for window in &windows {
        let window_peakel_count = peakels.iter()
            .filter(|p| p.isolation_window_id == window.id)
            .count();
        println!("  {:.1} m/z: {} peakels ({} spectra)", 
                 window.target_mz, window_peakel_count, window.spectrum_count);
    }
    
    // Top peakels by intensity
    let mut sorted_peakels = peakels.clone();
    sorted_peakels.sort_by(|a, b| 
        b.apex_intensity.partial_cmp(&a.apex_intensity).unwrap_or(std::cmp::Ordering::Equal)
    );
    
    println!();
    println!("Top 10 MS2 peakels by intensity:");
    for (i, peakel) in sorted_peakels.iter().take(10).enumerate() {
        println!("  {:2}: fragment m/z={:.4}, precursor={:.1}, RT={:.2}s, int={:.2e}, peaks={}",
            i + 1, peakel.mz, peakel.precursor_mz, peakel.elution_time, 
            peakel.apex_intensity, peakel.peaks_count);
    }
    
    // Write output
    println!();
    match args.format.as_str() {
        "sqlite" => {
            println!("Writing SQLite peakelDB (MS2/DIA format)...");
            write_ms2_dia_peakeldb(&args.output_file_path, &windows, &peakels)?;
        }
        "tsv" => {
            println!("Writing TSV file...");
            write_dia_peakels_tsv(&args.output_file_path, &peakels)?;
        }
        _ => {
            eprintln!("Warning: Unknown format '{}', defaulting to SQLite", args.format);
            write_ms2_dia_peakeldb(&args.output_file_path, &windows, &peakels)?;
        }
    }
    
    println!("Output written to: {:?}", args.output_file_path);
    
    // Print statistics
    print_ms2_statistics(&peakels);
    
    Ok(())
}

#[cfg(feature = "processing")]
fn write_ms2_dia_peakeldb(
    path: &PathBuf,
    windows: &[IsolationWindow],
    peakels: &[DiaMs2PeakelRecord],
) -> Result<()> {
    // Remove existing file if present
    if path.exists() {
        std::fs::remove_file(path)?;
    }
    
    let conn = Connection::open(path)?;
    
    // SQLite optimizations
    conn.execute_batch("
        PRAGMA synchronous=OFF;
        PRAGMA journal_mode=OFF;
        PRAGMA temp_store=2;
        PRAGMA cache_size=100000;
    ")?;
    
    // Create schema with isolation window support
    conn.execute_batch(MS2_DIA_PEAKELDB_SCHEMA)?;
    
    // Insert peakeldb_info
    let timestamp = chrono_lite_timestamp();
    conn.execute(
        "INSERT INTO peakeldb_info (id, name, description, creation_timestamp, peakel_count, ms_level) 
         VALUES (1, 'DIA MS2 peakelDB', 'Generated by mzdb2peakeldb', ?1, ?2, 2)",
        params![timestamp, peakels.len()],
    )?;
    
    // Insert isolation windows
    conn.execute("BEGIN TRANSACTION", [])?;
    {
        let mut stmt = conn.prepare(
            "INSERT INTO isolation_window (id, target_mz, lower_mz, upper_mz, spectrum_count) 
             VALUES (?1, ?2, ?3, ?4, ?5)"
        )?;
        
        for window in windows {
            stmt.execute(params![
                window.id,
                window.target_mz,
                window.lower_mz,
                window.upper_mz,
                window.spectrum_count,
            ])?;
        }
    }
    conn.execute("COMMIT", [])?;
    
    // Insert peakels
    conn.execute("BEGIN TRANSACTION", [])?;
    {
        let mut stmt = conn.prepare(
            "INSERT INTO peakel (id, mz, elution_time, duration, gap_count, apex_intensity, area, 
             amplitude, peaks_count, first_spectrum_id, apex_spectrum_id, last_spectrum_id, 
             isolation_window_id, precursor_mz, peaks) 
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15)"
        )?;
        
        let mut rtree_stmt = conn.prepare(
            "INSERT INTO peakel_rtree (id, min_mz, max_mz, min_time, max_time) 
             VALUES (?1, ?2, ?3, ?4, ?5)"
        )?;
        
        for peakel in peakels {
            let peaks_blob = peakel.peaks.to_msgpack()?;
            
            stmt.execute(params![
                peakel.id,
                peakel.mz,
                peakel.elution_time,
                peakel.duration,
                peakel.gap_count,
                peakel.apex_intensity,
                peakel.area,
                peakel.amplitude,
                peakel.peaks_count,
                peakel.first_spectrum_id,
                peakel.apex_spectrum_id,
                peakel.last_spectrum_id,
                peakel.isolation_window_id,
                peakel.precursor_mz,
                peaks_blob,
            ])?;
            
            rtree_stmt.execute(params![
                peakel.id,
                peakel.mz,
                peakel.mz,
                peakel.elution_time,
                peakel.elution_time,
            ])?;
        }
    }
    conn.execute("COMMIT", [])?;
    
    println!("DIA MS2 peakelDB created with {} isolation windows and {} peakels",
             windows.len(), peakels.len());
    
    Ok(())
}

#[cfg(feature = "processing")]
fn print_ms2_statistics(peakels: &[DiaMs2PeakelRecord]) {
    if peakels.is_empty() {
        return;
    }

    let total_area: f64 = peakels.iter().map(|p| p.area as f64).sum();
    let avg_duration: f32 = peakels.iter().map(|p| p.duration).sum::<f32>() 
        / peakels.len() as f32;
    let avg_peaks: f32 = peakels.iter().map(|p| p.peaks_count as f32).sum::<f32>() 
        / peakels.len() as f32;
    
    let min_mz = peakels.iter().map(|p| p.mz).fold(f64::INFINITY, f64::min);
    let max_mz = peakels.iter().map(|p| p.mz).fold(f64::NEG_INFINITY, f64::max);
    let min_rt = peakels.iter().map(|p| p.elution_time).fold(f32::INFINITY, f32::min);
    let max_rt = peakels.iter().map(|p| p.elution_time).fold(f32::NEG_INFINITY, f32::max);
    
    println!();
    println!("=== MS2 Peakel Statistics ===");
    println!("Total peakels: {}", peakels.len());
    println!("Total area: {:.2e}", total_area);
    println!("Average duration: {:.2}s", avg_duration);
    println!("Average peaks per peakel: {:.1}", avg_peaks);
    println!("Fragment m/z range: {:.2} - {:.2}", min_mz, max_mz);
    println!("RT range: {:.2}s - {:.2}s", min_rt, max_rt);
}

// ============================================================================
// Shared utilities
// ============================================================================

fn chrono_lite_timestamp() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let duration = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
    let secs = duration.as_secs();
    let days = secs / 86400;
    let years = 1970 + days / 365;
    let remaining_days = days % 365;
    let months = remaining_days / 30 + 1;
    let day = remaining_days % 30 + 1;
    let hour = (secs % 86400) / 3600;
    let min = (secs % 3600) / 60;
    let sec = secs % 60;
    format!("{:04}-{:02}-{:02} {:02}:{:02}:{:02}", years, months, day, hour, min, sec)
}

// ============================================================================
// Database schemas
// ============================================================================

const MS1_PEAKELDB_SCHEMA: &str = r#"
CREATE TABLE peakeldb_file (
    id INTEGER NOT NULL PRIMARY KEY,
    name VARCHAR NOT NULL,
    description VARCHAR,
    raw_file_name VARCHAR NOT NULL,
    is_dia_experiment BOOLEAN NOT NULL,
    creation_timestamp VARCHAR NOT NULL,
    modification_timestamp VARCHAR NOT NULL,
    serialized_properties TEXT
);

CREATE TABLE lcms_map (
    id INTEGER NOT NULL PRIMARY KEY,
    ms_level INTEGER NOT NULL,
    peakel_count INTEGER NOT NULL,
    serialized_properties TEXT,
    peakeldb_file_id INTEGER NOT NULL,
    FOREIGN KEY (peakeldb_file_id) REFERENCES peakeldb_file (id)
);

CREATE TABLE peakel (
    id INTEGER NOT NULL PRIMARY KEY,
    moz DOUBLE NOT NULL,
    elution_time REAL NOT NULL,
    duration REAL NOT NULL,
    gap_count INTEGER NOT NULL,
    apex_intensity REAL NOT NULL,
    area REAL NOT NULL,
    amplitude REAL NOT NULL,
    intensity_cv REAL NOT NULL,
    left_hwhm_mean REAL,
    left_hwhm_cv REAL,
    right_hwhm_mean REAL,
    right_hwhm_cv REAL,
    is_interfering BOOLEAN NOT NULL,
    peak_count INTEGER NOT NULL,
    peaks BLOB NOT NULL,
    serialized_properties TEXT,
    first_spectrum_id INTEGER NOT NULL,
    apex_spectrum_id INTEGER NOT NULL,
    last_spectrum_id INTEGER NOT NULL,
    map_id INTEGER NOT NULL,
    FOREIGN KEY (map_id) REFERENCES lcms_map (id)
);
);

CREATE VIRTUAL TABLE peakel_rtree USING rtree(
    id,
    min_mz, max_mz,
    min_time, max_time,
    min_intensity, max_intensity
);

CREATE INDEX peakel_moz_idx ON peakel (moz);
CREATE INDEX peakel_elution_time_idx ON peakel (elution_time);
CREATE INDEX peakel_map_id_idx ON peakel (map_id);
"#;

#[cfg(feature = "processing")]
const MS2_DIA_PEAKELDB_SCHEMA: &str = r#"
CREATE TABLE peakeldb_info (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    description TEXT,
    creation_timestamp TEXT NOT NULL,
    peakel_count INTEGER NOT NULL,
    ms_level INTEGER NOT NULL DEFAULT 2
);

CREATE TABLE isolation_window (
    id INTEGER PRIMARY KEY,
    target_mz REAL NOT NULL,
    lower_mz REAL NOT NULL,
    upper_mz REAL NOT NULL,
    spectrum_count INTEGER NOT NULL
);

CREATE TABLE peakel (
    id INTEGER PRIMARY KEY,
    mz REAL NOT NULL,
    elution_time REAL NOT NULL,
    duration REAL NOT NULL,
    gap_count INTEGER NOT NULL,
    apex_intensity REAL NOT NULL,
    area REAL NOT NULL,
    amplitude REAL NOT NULL,
    peaks_count INTEGER NOT NULL,
    first_spectrum_id INTEGER NOT NULL,
    apex_spectrum_id INTEGER NOT NULL,
    last_spectrum_id INTEGER NOT NULL,
    isolation_window_id INTEGER NOT NULL,
    precursor_mz REAL NOT NULL,
    peaks BLOB NOT NULL,
    FOREIGN KEY (isolation_window_id) REFERENCES isolation_window(id)
);

CREATE INDEX peakel_mz_idx ON peakel (mz);
CREATE INDEX peakel_rt_idx ON peakel (elution_time);
CREATE INDEX peakel_isolation_window_idx ON peakel (isolation_window_id);
CREATE INDEX peakel_precursor_mz_idx ON peakel (precursor_mz);

CREATE VIRTUAL TABLE peakel_rtree USING rtree(
    id,
    min_mz, max_mz,
    min_time, max_time
);
"#;
