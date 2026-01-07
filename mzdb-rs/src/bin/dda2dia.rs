//! dda2dia - Convert DDA mzDB files to simulated DIA format
//!
//! This command-line tool converts DDA (Data-Dependent Acquisition) mzDB files
//! to simulated DIA (Data-Independent Acquisition) files using detected peakels.
//!
//! # Usage
//!
//! ```bash
//! dda2dia --mzdb input.mzDB --peakeldb peakels.peakeldb --output output.mzDB [options]
//! ```
//!
//! # Examples
//!
//! ```bash
//! # Basic conversion
//! dda2dia -m input.mzDB -p peakels.peakeldb -o output.mzDB
//!
//! # Custom window settings
//! dda2dia -m input.mzDB -p peakels.peakeldb -o output.mzDB \
//!     --window-start 400 --window-end 1000 --window-width 25
//! ```

use std::path::PathBuf;
use std::process;

use clap::Parser;

use mzdb::conversion::diafication::{Dda2DiaConverter, DiaConversionOptions};

/// Convert DDA mzDB files to simulated DIA format
#[derive(Parser, Debug)]
#[command(name = "dda2dia")]
#[command(author = "mzdb-rs")]
#[command(version = "0.3.0")]
#[command(about = "Convert DDA mzDB files to simulated DIA format using detected peakels", long_about = None)]
struct Args {
    /// Input DDA mzDB file
    #[arg(short = 'm', long = "mzdb")]
    mzdb: PathBuf,

    /// Input peakeldb file with detected peakels
    #[arg(short = 'p', long = "peakeldb")]
    peakeldb: PathBuf,

    /// Output DIA mzDB file
    #[arg(short = 'o', long = "output")]
    output: PathBuf,

    /// DIA window start m/z
    #[arg(long = "window-start", default_value = "400")]
    window_start: f64,

    /// DIA window end m/z
    #[arg(long = "window-end", default_value = "1200")]
    window_end: f64,

    /// DIA window width in Da
    #[arg(long = "window-width", default_value = "50")]
    window_width: f64,

    /// m/z tolerance for peak merging in Da
    #[arg(long = "mz-tolerance", default_value = "0.1")]
    mz_tolerance: f64,

    /// Precursor m/z tolerance in ppm
    #[arg(long = "precursor-tolerance", default_value = "10")]
    precursor_tolerance: f64,

    /// Enable verbose output
    #[arg(short, long)]
    verbose: bool,
}

fn main() {
    let args = Args::parse();

    // Initialize logging
    if args.verbose {
        env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();
    } else {
        env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("warn")).init();
    }

    // Build options
    let options = DiaConversionOptions {
        window_start: args.window_start,
        window_end: args.window_end,
        window_width: args.window_width,
        mz_tolerance: args.mz_tolerance,
        precursor_tolerance_ppm: args.precursor_tolerance,
    };

    // Print configuration
    println!("DDA to DIA Converter");
    println!("====================");
    println!("Input mzDB:     {}", args.mzdb.display());
    println!("Input peakeldb: {}", args.peakeldb.display());
    println!("Output:         {}", args.output.display());
    println!();
    println!("Options:");
    println!("  DIA window range: {:.0} - {:.0} m/z", options.window_start, options.window_end);
    println!("  DIA window width: {:.1} Da", options.window_width);
    println!("  m/z tolerance:    {:.3} Da", options.mz_tolerance);
    println!("  Precursor tol:    {:.1} ppm", options.precursor_tolerance_ppm);
    println!();

    // Create converter and run
    let mzdb_str = args.mzdb.to_string_lossy();
    let peakeldb_str = args.peakeldb.to_string_lossy();
    
    let converter = match Dda2DiaConverter::new(&mzdb_str, &peakeldb_str, options) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("Error creating converter: {}", e);
            process::exit(1);
        }
    };

    let output_str = args.output.to_string_lossy();
    match converter.convert(&output_str) {
        Ok(stats) => {
            println!("Conversion complete!");
            println!();
            println!("Statistics:");
            println!("  Input MS1 spectra:   {}", stats.input_ms1_spectra);
            println!("  Input MS2 spectra:   {}", stats.input_ms2_spectra);
            println!("  Peakels loaded:      {}", stats.peakels_loaded);
            println!("  DIA windows:         {}", stats.dia_windows);
            println!("  Rescaled spectra:    {}", stats.rescaled_spectra);
            println!("  Output DIA spectra:  {}", stats.merged_spectra);
        }
        Err(e) => {
            eprintln!("Error during conversion: {}", e);
            process::exit(1);
        }
    }
}
