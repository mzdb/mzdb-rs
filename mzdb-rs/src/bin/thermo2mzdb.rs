//! raw2mzdb - Convert Thermo RAW files to mzDB format
//!
//! This command-line tool converts Thermo RAW mass spectrometry files
//! into the mzDB format, using configurable bounding box sizes.
//!
//! The acquisition mode (DIA vs DDA) is automatically detected from the RAW file.
//!
//! # Usage
//!
//! ```bash
//! raw2mzdb input.raw output.mzDB [options]
//! ```
//!
//! # Examples
//!
//! ```bash
//! # Convert with default bounding box sizes
//! raw2mzdb input.raw output.mzDB
//!
//! # Customize bounding box sizes
//! raw2mzdb input.raw output.mzDB \
//!   --bb-mz-height-ms1 5 \
//!   --bb-rt-width-msn 120
//! ```
//!

use std::path::PathBuf;
use std::process;

use clap::Parser;

use mzdb::writer::thermo::convert_raw_to_mzdb;
use mzdb::BBSizes;

/// Convert Thermo RAW files to mzDB format
#[derive(Parser, Debug)]
#[command(
    name = "raw2mzdb",
    author,
    version,
    about = "Convert Thermo RAW files to mzDB format",
    long_about = None
)]
struct Args {
    /// Input Thermo RAW file path
    #[arg(value_name = "INPUT")]
    input: PathBuf,

    /// Output mzDB file path
    #[arg(value_name = "OUTPUT")]
    output: PathBuf,

    /// Bounding box m/z height for MS1 spectra
    #[arg(long = "bb-mz-height-ms1", default_value = "10.0")]
    bb_mz_height_ms1: f64,

    /// Bounding box m/z height for MSn spectra
    #[arg(long = "bb-mz-height-msn", default_value = "10000.0")]
    bb_mz_height_msn: f64,

    /// Bounding box retention time width for MS1 spectra
    #[arg(long = "bb-rt-width-ms1", default_value = "30.0")]
    bb_rt_width_ms1: f32,

    /// Bounding box retention time width for MSn spectra
    #[arg(long = "bb-rt-width-msn", default_value = "0.0")]
    bb_rt_width_msn: f32,
}

fn main() {
    let args = Args::parse();

    let bb_sizes = BBSizes {
        bb_mz_height_ms1: args.bb_mz_height_ms1,
        bb_mz_height_msn: args.bb_mz_height_msn,
        bb_rt_width_ms1: args.bb_rt_width_ms1,
        bb_rt_width_msn: args.bb_rt_width_msn,
    };

    println!("Opening RAW file: {}", args.input.display());
    println!("Writing mzDB file: {}", args.output.display());

    // Acquisition mode (DIA vs DDA) is auto-detected from the RAW file
    if let Err(e) = convert_raw_to_mzdb(
        &args.input,
        &args.output,
        bb_sizes,
    ) {
        eprintln!("Error converting RAW to mzDB: {:#}", e);
        process::exit(1);
    }

    println!("Successfully converted RAW file to mzDB");
}
