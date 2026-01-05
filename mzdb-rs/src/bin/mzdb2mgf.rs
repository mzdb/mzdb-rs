                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                //! mzdb2mgf - Convert mzDB files to MGF format
//!
//! This command-line tool exports MS/MS spectra from mzDB files to MGF format,
//! which is widely used for peptide identification with search engines.
//!
//! # Usage
//!
//! ```bash
//! mzdb2mgf input.mzDB output.mgf [options]
//! ```
//!
//! # Options
//!
//! - `--ms-level <N>`: Export only spectra of the given MS level (default: 2)
//! - `--min-peaks <N>`: Minimum peaks required per spectrum (default: 1)
//! - `--max-peaks <N>`: Maximum peaks to export per spectrum
//! - `--min-intensity <F>`: Minimum peak intensity threshold (default: 0)

use std::env;
use std::process;

use mzdb::MzDbReader;
use mzdb::conversion::mgf::{MgfExportOptions, MgfWriter};

fn print_usage(program: &str) {
    eprintln!("Usage: {} <input.mzDB> <output.mgf> [options]", program);
    eprintln!();
    eprintln!("Options:");
    eprintln!("  --ms-level <N>      MS level to export (default: 2)");
    eprintln!("  --all-ms-levels     Export all MS levels");
    eprintln!("  --min-peaks <N>     Minimum peaks per spectrum (default: 1)");
    eprintln!("  --max-peaks <N>     Maximum peaks per spectrum");
    eprintln!("  --min-intensity <F> Minimum peak intensity (default: 0)");
    eprintln!("  --mz-precision <N>  Decimal places for m/z values (default: 6)");
    eprintln!("  --int-precision <N> Decimal places for intensity values (default: 2)");
    eprintln!("  -h, --help          Show this help message");
}

fn main() {
    let args: Vec<String> = env::args().collect();
    let program = &args[0];

    if args.len() < 3 {
        print_usage(program);
        process::exit(1);
    }

    // Check for help flag
    if args.iter().any(|a| a == "-h" || a == "--help") {
        print_usage(program);
        process::exit(0);
    }

    let input_path = &args[1];
    let output_path = &args[2];

    // Parse options
    let mut options = MgfExportOptions::default();
    let mut i = 3;
    while i < args.len() {
        match args[i].as_str() {
            "--ms-level" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("Error: --ms-level requires a value");
                    process::exit(1);
                }
                match args[i].parse::<i64>() {
                    Ok(level) => options = options.with_ms_level(level),
                    Err(_) => {
                        eprintln!("Error: Invalid MS level: {}", args[i]);
                        process::exit(1);
                    }
                }
            }
            "--all-ms-levels" => {
                options = options.with_all_ms_levels();
            }
            "--min-peaks" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("Error: --min-peaks requires a value");
                    process::exit(1);
                }
                match args[i].parse::<usize>() {
                    Ok(n) => options = options.with_min_peaks(n),
                    Err(_) => {
                        eprintln!("Error: Invalid min-peaks value: {}", args[i]);
                        process::exit(1);
                    }
                }
            }
            "--max-peaks" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("Error: --max-peaks requires a value");
                    process::exit(1);
                }
                match args[i].parse::<usize>() {
                    Ok(n) => options = options.with_max_peaks(n),
                    Err(_) => {
                        eprintln!("Error: Invalid max-peaks value: {}", args[i]);
                        process::exit(1);
                    }
                }
            }
            "--min-intensity" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("Error: --min-intensity requires a value");
                    process::exit(1);
                }
                match args[i].parse::<f32>() {
                    Ok(f) => options = options.with_min_intensity(f),
                    Err(_) => {
                        eprintln!("Error: Invalid min-intensity value: {}", args[i]);
                        process::exit(1);
                    }
                }
            }
            "--mz-precision" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("Error: --mz-precision requires a value");
                    process::exit(1);
                }
                match args[i].parse::<usize>() {
                    Ok(n) => options = options.with_mz_precision(n),
                    Err(_) => {
                        eprintln!("Error: Invalid mz-precision value: {}", args[i]);
                        process::exit(1);
                    }
                }
            }
            "--int-precision" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("Error: --int-precision requires a value");
                    process::exit(1);
                }
                match args[i].parse::<usize>() {
                    Ok(n) => options = options.with_intensity_precision(n),
                    Err(_) => {
                        eprintln!("Error: Invalid int-precision value: {}", args[i]);
                        process::exit(1);
                    }
                }
            }
            arg => {
                eprintln!("Error: Unknown option: {}", arg);
                print_usage(program);
                process::exit(1);
            }
        }
        i += 1;
    }

    // Open mzDB file
    println!("Opening mzDB file: {}", input_path);
    let mzdb = match MzDbReader::open(input_path) {
        Ok(db) => db,
        Err(e) => {
            eprintln!("Error opening mzDB file: {}", e);
            process::exit(1);
        }
    };

    // Export to MGF
    println!("Exporting to MGF: {}", output_path);
    match MgfWriter::export(&mzdb, output_path, &options) {
        Ok(count) => {
            println!("Successfully exported {} spectra to MGF", count);
        }
        Err(e) => {
            eprintln!("Error exporting to MGF: {}", e);
            process::exit(1);
        }
    }
}
