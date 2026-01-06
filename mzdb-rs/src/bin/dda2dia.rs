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
//! # Required Arguments
//!
//! - `--mzdb, -m <FILE>`: Input DDA mzDB file
//! - `--peakeldb, -p <FILE>`: Input peakeldb file with detected peakels
//! - `--output, -o <FILE>`: Output DIA mzDB file
//!
//! # Options
//!
//! - `--window-start <MZ>`: DIA window start m/z (default: 400)
//! - `--window-end <MZ>`: DIA window end m/z (default: 1200)
//! - `--window-width <DA>`: DIA window width in Da (default: 50)
//! - `--mz-tolerance <DA>`: m/z tolerance for peak merging (default: 0.1)
//! - `--precursor-tolerance <PPM>`: Precursor m/z tolerance in ppm (default: 10)

use std::env;
use std::process;

use mzdb::conversion::diafication::{Dda2DiaConverter, DiaConversionOptions};

fn print_usage(program: &str) {
    eprintln!("DDA to DIA Converter");
    eprintln!();
    eprintln!("Converts a DDA mzDB file to a simulated DIA file using detected peakels.");
    eprintln!();
    eprintln!("Usage: {} --mzdb <FILE> --peakeldb <FILE> --output <FILE> [options]", program);
    eprintln!();
    eprintln!("Required arguments:");
    eprintln!("  -m, --mzdb <FILE>           Input DDA mzDB file");
    eprintln!("  -p, --peakeldb <FILE>       Input peakeldb file");
    eprintln!("  -o, --output <FILE>         Output DIA mzDB file");
    eprintln!();
    eprintln!("Options:");
    eprintln!("  --window-start <MZ>         DIA window start m/z (default: 400)");
    eprintln!("  --window-end <MZ>           DIA window end m/z (default: 1200)");
    eprintln!("  --window-width <DA>         DIA window width in Da (default: 50)");
    eprintln!("  --mz-tolerance <DA>         m/z tolerance for peak merging (default: 0.1)");
    eprintln!("  --precursor-tolerance <PPM> Precursor m/z tolerance in ppm (default: 10)");
    eprintln!("  -v, --verbose               Enable verbose output");
    eprintln!("  -h, --help                  Show this help message");
}

fn main() {
    let args: Vec<String> = env::args().collect();
    let program = &args[0];

    // Check for help flag
    if args.len() < 2 || args.iter().any(|a| a == "-h" || a == "--help") {
        print_usage(program);
        if args.len() < 2 {
            process::exit(1);
        }
        process::exit(0);
    }

    // Parse arguments
    let mut mzdb_path: Option<String> = None;
    let mut peakeldb_path: Option<String> = None;
    let mut output_path: Option<String> = None;
    let mut options = DiaConversionOptions::default();
    let mut verbose = false;

    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "-m" | "--mzdb" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("Error: --mzdb requires a value");
                    process::exit(1);
                }
                mzdb_path = Some(args[i].clone());
            }
            "-p" | "--peakeldb" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("Error: --peakeldb requires a value");
                    process::exit(1);
                }
                peakeldb_path = Some(args[i].clone());
            }
            "-o" | "--output" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("Error: --output requires a value");
                    process::exit(1);
                }
                output_path = Some(args[i].clone());
            }
            "--window-start" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("Error: --window-start requires a value");
                    process::exit(1);
                }
                match args[i].parse::<f64>() {
                    Ok(v) => options.window_start = v,
                    Err(_) => {
                        eprintln!("Error: Invalid window-start value: {}", args[i]);
                        process::exit(1);
                    }
                }
            }
            "--window-end" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("Error: --window-end requires a value");
                    process::exit(1);
                }
                match args[i].parse::<f64>() {
                    Ok(v) => options.window_end = v,
                    Err(_) => {
                        eprintln!("Error: Invalid window-end value: {}", args[i]);
                        process::exit(1);
                    }
                }
            }
            "--window-width" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("Error: --window-width requires a value");
                    process::exit(1);
                }
                match args[i].parse::<f64>() {
                    Ok(v) => options.window_width = v,
                    Err(_) => {
                        eprintln!("Error: Invalid window-width value: {}", args[i]);
                        process::exit(1);
                    }
                }
            }
            "--mz-tolerance" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("Error: --mz-tolerance requires a value");
                    process::exit(1);
                }
                match args[i].parse::<f64>() {
                    Ok(v) => options.mz_tolerance = v,
                    Err(_) => {
                        eprintln!("Error: Invalid mz-tolerance value: {}", args[i]);
                        process::exit(1);
                    }
                }
            }
            "--precursor-tolerance" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("Error: --precursor-tolerance requires a value");
                    process::exit(1);
                }
                match args[i].parse::<f64>() {
                    Ok(v) => options.precursor_tolerance_ppm = v,
                    Err(_) => {
                        eprintln!("Error: Invalid precursor-tolerance value: {}", args[i]);
                        process::exit(1);
                    }
                }
            }
            "-v" | "--verbose" => {
                verbose = true;
            }
            arg => {
                eprintln!("Error: Unknown option: {}", arg);
                print_usage(program);
                process::exit(1);
            }
        }
        i += 1;
    }

    // Validate required arguments
    let mzdb_path = match mzdb_path {
        Some(p) => p,
        None => {
            eprintln!("Error: --mzdb is required");
            print_usage(program);
            process::exit(1);
        }
    };

    let peakeldb_path = match peakeldb_path {
        Some(p) => p,
        None => {
            eprintln!("Error: --peakeldb is required");
            print_usage(program);
            process::exit(1);
        }
    };

    let output_path = match output_path {
        Some(p) => p,
        None => {
            eprintln!("Error: --output is required");
            print_usage(program);
            process::exit(1);
        }
    };

    // Initialize logging
    if verbose {
        env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();
    } else {
        env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("warn")).init();
    }

    // Print configuration
    println!("DDA to DIA Converter");
    println!("====================");
    println!("Input mzDB:    {}", mzdb_path);
    println!("Input peakeldb: {}", peakeldb_path);
    println!("Output:        {}", output_path);
    println!();
    println!("Options:");
    println!("  DIA window range: {:.0} - {:.0} m/z", options.window_start, options.window_end);
    println!("  DIA window width: {:.1} Da", options.window_width);
    println!("  m/z tolerance:    {:.3} Da", options.mz_tolerance);
    println!("  Precursor tol:    {:.1} ppm", options.precursor_tolerance_ppm);
    println!();

    // Create converter and run
    let converter = match Dda2DiaConverter::new(&mzdb_path, &peakeldb_path, options) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("Error creating converter: {}", e);
            process::exit(1);
        }
    };

    match converter.convert(&output_path) {
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
