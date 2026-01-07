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
use smallvec::SmallVec;

use mzdb::MzDbReader;

use mzdb::processing::{
    SmartPeakelFinder, SmartPeakelFinderConfig, PeakelFinder, BasicPeakelFinder,
    Peakel,
    // DIA types
    DiaMs2PeakelDetector, DiaMs2PeakelConfig, IsolationWindow,
    DiaMs2PeakelRecord,
    // PeakelDB types
    Ms1PeakelDbWriter, Ms2PeakelDbWriter,
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

impl Ms1PeakelDetectionConfig {
    /// Create a PeakelFinder based on the algorithm configuration
    fn create_finder(&self) -> Box<dyn PeakelFinder + Send + Sync> {
        match self.algorithm.as_str() {
            "smart" => {
                let finder_config = SmartPeakelFinderConfig {
                    min_peaks_count: self.min_peaks_count,
                    use_smoothing: true,
                    use_baseline_remover: false,
                    ..Default::default()
                };
                Box::new(SmartPeakelFinder::with_config(finder_config))
            }
            _ => Box::new(BasicPeakelFinder::default_params()),
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
        1 => run_ms1_detection(&args, &reader, num_threads)?,
        2 => run_ms2_dia_detection(&args, &reader, num_threads)?,
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
fn detect_peakels_sequential(
    bins: &[(i64, Vec<(f64, f32, i64, f32)>)],
    config: &Ms1PeakelDetectionConfig,
    total_bins: usize,
) -> Vec<Peakel> {
    // Create peakel finder
    let finder = config.create_finder();
    
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
#[cfg(feature = "processing-parallel")]
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
                let finder = config.create_finder();
                
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

fn write_ms1_peakeldb<P: AsRef<Path>>(path: P, mzdb_filename: &str, is_dia: bool, peakels: &[Peakel]) -> Result<()> {
    let writer = Ms1PeakelDbWriter::create(&path)?;
    writer.write_peakels(mzdb_filename, is_dia, peakels)?;
    println!("SQLite peakelDB created with {} peakels", peakels.len());
    Ok(())
}

// ============================================================================
// MS2 DIA Peakel Detection
// ============================================================================

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
    
    // Get input filename for metadata
    let input_filename = args.mzdb_file_path
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or("unknown.mzDB");

    // Write output
    println!();
    match args.format.as_str() {
        "sqlite" => {
            println!("Writing SQLite peakelDB (MS2/DIA format)...");
            write_ms2_dia_peakeldb(&args.output_file_path, input_filename, &windows, &peakels)?;
        }
        "tsv" => {
            println!("Writing TSV file...");
            write_ms2_peakels_tsv(&args.output_file_path, &peakels)?;
        }
        _ => {
            eprintln!("Warning: Unknown format '{}', defaulting to SQLite", args.format);
            write_ms2_dia_peakeldb(&args.output_file_path, input_filename, &windows, &peakels)?;
        }
    }
    
    println!("Output written to: {:?}", args.output_file_path);
    
    // Print statistics
    print_ms2_statistics(&peakels);
    
    Ok(())
}

fn write_ms2_dia_peakeldb(
    path: &PathBuf,
    mzdb_filename: &str,
    windows: &[IsolationWindow],
    peakels: &[DiaMs2PeakelRecord],
) -> Result<()> {
    let writer = Ms2PeakelDbWriter::create(path)?;
    writer.write_peakels(mzdb_filename, windows, peakels)?;
    println!("DIA MS2 peakelDB created with {} isolation windows and {} peakels",
             windows.len(), peakels.len());
    Ok(())
}

/// Write DIA MS2 peakels to a TSV file
fn write_ms2_peakels_tsv(path: &PathBuf, peakels: &[DiaMs2PeakelRecord]) -> Result<()> {
    use std::io::Write;
    use std::fs::File;

    let mut file = File::create(path)?;

    // Header with MS2-specific fields (isolation_window_id, precursor_mz)
    writeln!(
        file,
        "id\tmoz\telution_time\tduration\tgap_count\tapex_intensity\tarea\tamplitude\tpeak_count\t\
         first_spectrum_id\tapex_spectrum_id\tlast_spectrum_id\tisolation_window_id\tprecursor_mz"
    )?;

    for peakel in peakels {
        writeln!(
            file,
            "{}\t{:.6}\t{:.4}\t{:.4}\t{}\t{:.2}\t{:.2}\t{:.4}\t{}\t{}\t{}\t{}\t{}\t{:.4}",
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
        )?;
    }

    log::info!("TSV file created with {} peakels", peakels.len());
    Ok(())
}

/// Write MS1 peakels to a TSV file
fn write_ms1_peakels_tsv<P: AsRef<Path>>(path: P, peakels: &[Peakel]) -> Result<()> {
    use std::io::Write;
    use std::fs::File;
    use mzdb::processing::HasPeakelData;
    
    let mut file = File::create(path)?;
    
    writeln!(file, "id\tmoz\telution_time\tduration\tapex_intensity\tarea\tpeak_count\tfirst_spectrum_id\tapex_spectrum_id\tlast_spectrum_id")?;
    
    for (idx, peakel) in peakels.iter().enumerate() {
        writeln!(
            file,
            "{}\t{:.6}\t{:.4}\t{:.4}\t{:.2}\t{:.2}\t{}\t{}\t{}\t{}",
            idx + 1,
            peakel.calc_mz(),
            peakel.apex_elution_time().unwrap_or(0.0),
            peakel.calc_duration(),
            peakel.apex_intensity().unwrap_or(0.0),
            peakel.area(),
            peakel.peaks_count(),
            peakel.first_spectrum_id().unwrap_or(0),
            peakel.apex_spectrum_id().unwrap_or(0),
            peakel.last_spectrum_id().unwrap_or(0),
        )?;
    }
    
    Ok(())
}

// ============================================================================
// Statistics
// ============================================================================

/// Print peakel statistics (common implementation)
fn print_statistics(
    title: &str,
    count: usize,
    total_area: f64,
    avg_duration: f32,
    avg_peaks: f32,
    min_mz: f64,
    max_mz: f64,
    min_rt: f32,
    max_rt: f32,
) {
    println!();
    println!("=== {} ===", title);
    println!("Total peakels: {}", count);
    println!("Total area: {:.2e}", total_area);
    println!("Average duration: {:.2}s", avg_duration);
    println!("Average peaks per peakel: {:.1}", avg_peaks);
    println!("m/z range: {:.2} - {:.2}", min_mz, max_mz);
    println!("RT range: {:.2}s - {:.2}s", min_rt, max_rt);
}

/// Print MS1 peakel statistics to stdout
fn print_ms1_statistics(peakels: &[Peakel]) {
    use mzdb::processing::HasPeakelData;

    if peakels.is_empty() {
        return;
    }

    let n = peakels.len() as f32;
    print_statistics(
        "MS1 Peakel Statistics",
        peakels.len(),
        peakels.iter().map(|p| p.area() as f64).sum(),
        peakels.iter().map(|p| p.calc_duration()).sum::<f32>() / n,
        peakels.iter().map(|p| p.peaks_count() as f32).sum::<f32>() / n,
        peakels.iter().map(|p| p.calc_mz()).fold(f64::INFINITY, f64::min),
        peakels.iter().map(|p| p.calc_mz()).fold(f64::NEG_INFINITY, f64::max),
        peakels.iter().filter_map(|p| p.apex_elution_time()).fold(f32::INFINITY, f32::min),
        peakels.iter().filter_map(|p| p.apex_elution_time()).fold(f32::NEG_INFINITY, f32::max),
    );
}

/// Print MS2 DIA peakel statistics to stdout
fn print_ms2_statistics(peakels: &[DiaMs2PeakelRecord]) {
    if peakels.is_empty() {
        return;
    }

    let n = peakels.len() as f32;
    print_statistics(
        "MS2 DIA Peakel Statistics",
        peakels.len(),
        peakels.iter().map(|p| p.area as f64).sum(),
        peakels.iter().map(|p| p.duration).sum::<f32>() / n,
        peakels.iter().map(|p| p.peaks_count as f32).sum::<f32>() / n,
        peakels.iter().map(|p| p.mz).fold(f64::INFINITY, f64::min),
        peakels.iter().map(|p| p.mz).fold(f64::NEG_INFINITY, f64::max),
        peakels.iter().map(|p| p.elution_time).fold(f32::INFINITY, f32::min),
        peakels.iter().map(|p| p.elution_time).fold(f32::NEG_INFINITY, f32::max),
    );
}
