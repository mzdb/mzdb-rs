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
//! # Examples
//!
//! ```bash
//! # Export MS2 spectra (default)
//! mzdb2mgf input.mzDB output.mgf
//!
//! # Export all MS levels
//! mzdb2mgf input.mzDB output.mgf --all-ms-levels
//!
//! # Filter by minimum intensity
//! mzdb2mgf input.mzDB output.mgf --min-intensity 100
//! ```

use std::path::PathBuf;
use std::process;

use clap::Parser;

use mzdb::MzDbReader;
use mzdb::conversion::mgf::{MgfExportOptions, MgfWriter};

/// Convert mzDB files to MGF format
#[derive(Parser, Debug)]
#[command(
    name = "mzdb2mgf",
    author,
    version,
    about = "Export MS/MS spectra from mzDB to MGF format",
    long_about = None
)]
struct Args {
    /// Input mzDB file path
    #[arg(value_name = "INPUT")]
    input: PathBuf,

    /// Output MGF file path
    #[arg(value_name = "OUTPUT")]
    output: PathBuf,

    /// MS level to export (default: 2 for MS/MS)
    #[arg(long = "ms-level", default_value = "2")]
    ms_level: i64,

    /// Export all MS levels
    #[arg(long = "all-ms-levels", conflicts_with = "ms_level")]
    all_ms_levels: bool,

    /// Minimum peaks required per spectrum
    #[arg(long = "min-peaks", default_value = "1")]
    min_peaks: usize,

    /// Maximum peaks to export per spectrum
    #[arg(long = "max-peaks")]
    max_peaks: Option<usize>,

    /// Minimum peak intensity threshold
    #[arg(long = "min-intensity", default_value = "0")]
    min_intensity: f32,

    /// Decimal places for m/z values
    #[arg(long = "mz-precision", default_value = "6")]
    mz_precision: usize,

    /// Decimal places for intensity values
    #[arg(long = "int-precision", default_value = "2")]
    int_precision: usize,
}

fn main() {
    let args = Args::parse();

    // Build export options
    let mut options = MgfExportOptions::default()
        .with_min_peaks(args.min_peaks)
        .with_min_intensity(args.min_intensity)
        .with_mz_precision(args.mz_precision)
        .with_intensity_precision(args.int_precision);

    if args.all_ms_levels {
        options = options.with_all_ms_levels();
    } else {
        options = options.with_ms_level(args.ms_level);
    }

    if let Some(max_peaks) = args.max_peaks {
        options = options.with_max_peaks(max_peaks);
    }

    // Open mzDB file
    println!("Opening mzDB file: {}", args.input.display());
    let input_str = args.input.to_string_lossy();
    let mzdb = match MzDbReader::open(&input_str) {
        Ok(db) => db,
        Err(e) => {
            eprintln!("Error opening mzDB file: {:#}", e);
            process::exit(1);
        }
    };

    // Export to MGF
    println!("Exporting to MGF: {}", args.output.display());
    match MgfWriter::export(&mzdb, &args.output, &options) {
        Ok(count) => {
            println!("Successfully exported {} spectra to MGF", count);
        }
        Err(e) => {
            eprintln!("Error exporting to MGF: {:#}", e);
            process::exit(1);
        }
    }
}
