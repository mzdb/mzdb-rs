//! DDA to DIA Conversion (DIAfication)
//!
//! This module provides functionality to convert DDA (Data-Dependent Acquisition)
//! mzDB files to simulated DIA (Data-Independent Acquisition) files using detected
//! peakels.
//!
//! The conversion process:
//! 1. Reads peakels from a peakeldb file
//! 2. For each MS/MS spectrum, finds matching peakels by precursor m/z
//! 3. For each peakel match, keeps the spectrum closest to apex
//! 4. Rescales spectrum intensity based on precursor intensity ratios
//! 5. Duplicates spectra across peakel data points with proportional scaling
//! 6. Groups and merges spectra by cycle and DIA window
//! 7. Writes the merged DIA spectra to a new mzDB file
//!
//! # Example
//!
//! ```no_run
//! use mzdb::conversion::diafication::{Dda2DiaConverter, DiaConversionOptions};
//!
//! let options = DiaConversionOptions::default();
//! let converter = Dda2DiaConverter::new("input.mzDB", "peakels.peakeldb", options).unwrap();
//! converter.convert("output_dia.mzDB").unwrap();
//! ```

use anyhow_ext::{bail, Context, Result};
use ordered_float::OrderedFloat;
use rmpv::Value;
use rusqlite::{params, Connection};
use std::collections::HashMap;
use std::path::Path;

use crate::model::DataPointProvider;
use crate::writer::{serialize_to_bounding_box, insert_bounding_box_data, insert_msn_rtree_entry};

// ============================================================================
// Configuration
// ============================================================================

/// Options for DDA to DIA conversion
#[derive(Clone, Debug)]
pub struct DiaConversionOptions {
    /// DIA window start m/z
    pub window_start: f64,
    /// DIA window end m/z
    pub window_end: f64,
    /// DIA window width in Da
    pub window_width: f64,
    /// m/z tolerance for peak merging in Da
    pub mz_tolerance: f64,
    /// Precursor m/z tolerance in ppm for peakel matching
    pub precursor_tolerance_ppm: f64,
}

impl Default for DiaConversionOptions {
    fn default() -> Self {
        Self {
            window_start: 400.0,
            window_end: 1200.0,
            window_width: 50.0,
            mz_tolerance: 0.1,
            precursor_tolerance_ppm: 10.0,
        }
    }
}

impl DiaConversionOptions {
    /// Create new options with custom DIA window range
    pub fn with_window_range(mut self, start: f64, end: f64) -> Self {
        self.window_start = start;
        self.window_end = end;
        self
    }

    /// Set DIA window width
    pub fn with_window_width(mut self, width: f64) -> Self {
        self.window_width = width;
        self
    }

    /// Set m/z tolerance for peak merging
    pub fn with_mz_tolerance(mut self, tolerance: f64) -> Self {
        self.mz_tolerance = tolerance;
        self
    }

    /// Set precursor m/z tolerance in ppm
    pub fn with_precursor_tolerance_ppm(mut self, tolerance: f64) -> Self {
        self.precursor_tolerance_ppm = tolerance;
        self
    }
}

// ============================================================================
// Peakel Types
// ============================================================================

/// A peakel represents a detected chromatographic peak
#[derive(Debug, Clone)]
pub struct Peakel {
    /// Unique identifier
    pub id: i32,
    /// Centroid m/z value
    pub mz: f64,
    /// Elution time at apex (seconds)
    pub elution_time: f32,
    /// Intensity at apex
    pub apex_intensity: f32,
    /// Integrated area under the curve
    pub area: f32,
    /// Duration of the peak (seconds)
    pub duration: f32,
    /// Number of data points
    pub peak_count: i32,
    /// First spectrum ID in the peakel
    pub first_spectrum_id: i64,
    /// Last spectrum ID in the peakel
    pub last_spectrum_id: i64,
    /// Spectrum ID at the apex
    pub apex_spectrum_id: i64,
    /// Spectrum IDs at each data point
    pub spectrum_ids: Vec<i64>,
    /// Retention times at each data point (seconds)
    pub elution_times: Vec<f32>,
    /// m/z values at each data point
    pub mz_values: Vec<f64>,
    /// Intensities at each data point
    pub intensities: Vec<f32>,
}

impl Peakel {
    /// Check if peakel contains a given m/z within tolerance
    pub fn contains_mz(&self, mz: f64, tolerance_ppm: f64) -> bool {
        let tolerance = self.mz * tolerance_ppm / 1_000_000.0;
        (self.mz - mz).abs() <= tolerance
    }

    /// Check if peakel contains a given spectrum_id
    pub fn contains_spectrum(&self, spectrum_id: i64) -> bool {
        self.spectrum_ids.contains(&spectrum_id)
    }

    /// Get the index of a spectrum in this peakel
    pub fn spectrum_index(&self, spectrum_id: i64) -> Option<usize> {
        self.spectrum_ids.iter().position(|&id| id == spectrum_id)
    }

    /// Get intensity at a specific spectrum, if present
    pub fn intensity_at_spectrum(&self, spectrum_id: i64) -> Option<f32> {
        self.spectrum_index(spectrum_id)
            .map(|idx| self.intensities[idx])
    }

    /// Get the apex spectrum index
    pub fn apex_index(&self) -> Option<usize> {
        self.spectrum_ids
            .iter()
            .position(|&id| id == self.apex_spectrum_id)
    }
}

// ============================================================================
// Peakel Database Reader
// ============================================================================

/// Reader for peakeldb SQLite files
pub struct PeakelDbReader {
    conn: Connection,
}

impl PeakelDbReader {
    /// Open a peakeldb file
    pub fn open(path: &str) -> Result<Self> {
        let conn = Connection::open(path).context("Failed to open peakeldb file")?;
        Ok(Self { conn })
    }

    /// Read all peakels from the database
    pub fn read_all_peakels(&self) -> Result<Vec<Peakel>> {
        let mut stmt = self.conn.prepare(
            "SELECT id, moz, elution_time, duration, apex_intensity, area, 
                    peak_count, peaks,
                    first_spectrum_id, apex_spectrum_id, last_spectrum_id
             FROM peakel",
        )?;

        let peakel_iter = stmt.query_map([], |row| {
            let peaks_blob: Vec<u8> = row.get(7)?;
            Ok((
                row.get::<_, i32>(0)?,  // id
                row.get::<_, f64>(1)?,  // moz
                row.get::<_, f32>(2)?,  // elution_time
                row.get::<_, f32>(3)?,  // duration
                row.get::<_, f32>(4)?,  // apex_intensity
                row.get::<_, f32>(5)?,  // area
                row.get::<_, i32>(6)?,  // peak_count
                peaks_blob,             // peaks (MessagePack blob)
                row.get::<_, i64>(8)?,  // first_spectrum_id
                row.get::<_, i64>(9)?,  // apex_spectrum_id
                row.get::<_, i64>(10)?, // last_spectrum_id
            ))
        })?;

        let mut peakels = Vec::new();

        for result in peakel_iter {
            let (
                id,
                mz,
                elution_time,
                duration,
                apex_intensity,
                area,
                peak_count,
                peaks_blob,
                first_spectrum_id,
                apex_spectrum_id,
                last_spectrum_id,
            ) = result?;

            // Parse the MessagePack peaks blob
            let (spectrum_ids, elution_times, mz_values, intensities) =
                parse_peaks_blob(&peaks_blob)?;

            peakels.push(Peakel {
                id,
                mz,
                elution_time,
                apex_intensity,
                area,
                duration,
                peak_count,
                first_spectrum_id,
                last_spectrum_id,
                apex_spectrum_id,
                spectrum_ids,
                elution_times,
                mz_values,
                intensities,
            });
        }

        log::info!("Loaded {} peakels from peakeldb", peakels.len());
        Ok(peakels)
    }

    /// Get the number of peakels in the database
    pub fn get_peakel_count(&self) -> Result<i64> {
        self.conn
            .query_row("SELECT COUNT(*) FROM peakel", [], |row| row.get(0))
            .context("Failed to count peakels")
    }
}

/// Parse the MessagePack-encoded peaks blob
///
/// The format is an array of 4 arrays:
/// [spectrum_ids (Long), elution_times (Float), mz_values (Double), intensity_values (Float)]
fn parse_peaks_blob(data: &[u8]) -> Result<(Vec<i64>, Vec<f32>, Vec<f64>, Vec<f32>)> {
    let value: Value =
        rmpv::decode::read_value(&mut &data[..]).context("Failed to decode MessagePack data")?;

    if let Value::Array(arrays) = value {
        if arrays.len() != 4 {
            bail!("Expected 4 arrays in peaks blob, got {}", arrays.len());
        }

        let spectrum_ids = extract_i64_array(&arrays[0])?;
        let elution_times = extract_f32_array(&arrays[1])?;
        let mz_values = extract_f64_array(&arrays[2])?;
        let intensities = extract_f32_array(&arrays[3])?;

        Ok((spectrum_ids, elution_times, mz_values, intensities))
    } else {
        bail!("Expected array in peaks blob, got {:?}", value);
    }
}

fn extract_i64_array(value: &Value) -> Result<Vec<i64>> {
    if let Value::Array(arr) = value {
        arr.iter()
            .map(|v| match v {
                Value::Integer(i) => Ok(i.as_i64().unwrap_or(0)),
                _ => Ok(0),
            })
            .collect()
    } else {
        bail!("Expected array, got {:?}", value);
    }
}

fn extract_f32_array(value: &Value) -> Result<Vec<f32>> {
    if let Value::Array(arr) = value {
        arr.iter()
            .map(|v| match v {
                Value::F32(f) => Ok(*f),
                Value::F64(f) => Ok(*f as f32),
                Value::Integer(i) => Ok(i.as_f64().unwrap_or(0.0) as f32),
                _ => Ok(0.0),
            })
            .collect()
    } else {
        bail!("Expected array, got {:?}", value);
    }
}

fn extract_f64_array(value: &Value) -> Result<Vec<f64>> {
    if let Value::Array(arr) = value {
        arr.iter()
            .map(|v| match v {
                Value::F64(f) => Ok(*f),
                Value::F32(f) => Ok(*f as f64),
                Value::Integer(i) => Ok(i.as_f64().unwrap_or(0.0)),
                _ => Ok(0.0),
            })
            .collect()
    } else {
        bail!("Expected array, got {:?}", value);
    }
}

// ============================================================================
// Spectrum Data (simplified for conversion)
// ============================================================================

/// Simplified spectrum data for conversion
#[derive(Debug, Clone)]
pub struct SimpleSpectrumData {
    /// m/z values
    pub mz_array: Vec<f64>,
    /// Intensity values
    pub intensity_array: Vec<f32>,
}

impl SimpleSpectrumData {
    /// Create new empty spectrum data
    pub fn new() -> Self {
        Self {
            mz_array: Vec::new(),
            intensity_array: Vec::new(),
        }
    }

    /// Get number of peaks
    pub fn peaks_count(&self) -> usize {
        self.mz_array.len()
    }

    /// Scale all intensities by a factor
    pub fn scale_intensities(&mut self, factor: f32) {
        for intensity in &mut self.intensity_array {
            *intensity *= factor;
        }
    }
}

impl Default for SimpleSpectrumData {
    fn default() -> Self {
        Self::new()
    }
}

impl DataPointProvider for SimpleSpectrumData {
    fn mz_array(&self) -> &[f64] {
        &self.mz_array
    }
    
    fn intensity_array(&self) -> &[f32] {
        &self.intensity_array
    }
}

// ============================================================================
// Spectrum Header (simplified)
// ============================================================================

/// Simplified spectrum header for conversion
#[derive(Debug, Clone)]
pub struct SimpleSpectrumHeader {
    pub id: i64,
    pub initial_id: i64,
    pub title: String,
    pub cycle: i32,
    pub time: f32,
    pub ms_level: i32,
    pub activation_type: Option<String>,
    pub tic: f64,
    pub base_peak_mz: f64,
    pub base_peak_intensity: f32,
    pub main_precursor_mz: Option<f64>,
    pub main_precursor_charge: Option<i32>,
    pub data_points_count: i32,
    pub param_tree: String,
    pub scan_list: Option<String>,
    pub precursor_list: Option<String>,
    pub product_list: Option<String>,
    pub data_encoding_id: i64,
    pub bb_first_spectrum_id: i64,
}

// ============================================================================
// DIA Window and Merging Types
// ============================================================================

/// DIA window definition
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct DiaWindow {
    /// Center m/z
    pub center: f64,
    /// Minimum m/z
    pub min_mz: f64,
    /// Maximum m/z
    pub max_mz: f64,
}

impl DiaWindow {
    /// Create a new DIA window
    pub fn new(center: f64, half_width: f64) -> Self {
        Self {
            center,
            min_mz: center - half_width,
            max_mz: center + half_width,
        }
    }

    /// Check if an m/z value falls within this window
    pub fn contains(&self, mz: f64) -> bool {
        mz >= self.min_mz && mz <= self.max_mz
    }
}

/// Generate DIA windows from start to end with given width
pub fn generate_dia_windows(start: f64, end: f64, width: f64) -> Vec<DiaWindow> {
    let half_width = width / 2.0;
    let mut windows = Vec::new();
    let mut center = start + half_width;

    while center <= end - half_width {
        windows.push(DiaWindow::new(center, half_width));
        center += width;
    }

    windows
}

/// A rescaled MS/MS spectrum ready for merging
#[derive(Debug, Clone)]
pub struct RescaledSpectrum {
    /// Original MS2 spectrum ID
    pub original_spectrum_id: i64,
    /// Associated peakel ID
    pub peakel_id: i32,
    /// Target cycle for this rescaled spectrum
    pub target_cycle: i32,
    /// Precursor m/z from the peakel
    pub precursor_mz: f64,
    /// Scale factor applied to intensities
    pub scale_factor: f32,
    /// The peak data (after rescaling)
    pub data: SimpleSpectrumData,
}

/// A merged DIA spectrum
#[derive(Debug, Clone)]
pub struct MergedDiaSpectrum {
    /// Target cycle
    pub cycle: i32,
    /// Center of the DIA window (m/z)
    pub window_center: f64,
    /// Lower m/z bound of the DIA window
    pub window_min_mz: f64,
    /// Upper m/z bound of the DIA window
    pub window_max_mz: f64,
    /// Retention time (from the corresponding MS1)
    pub time: f32,
    /// Merged peak data
    pub data: SimpleSpectrumData,
    /// Number of contributing spectra
    pub contributing_count: usize,
}

// ============================================================================
// Spectrum Merging Functions
// ============================================================================

/// Group rescaled spectra by cycle and DIA window
pub fn group_spectra_by_cycle_and_window<'a>(
    spectra: &'a [RescaledSpectrum],
    windows: &[DiaWindow],
) -> HashMap<(i32, OrderedFloat<f64>), Vec<&'a RescaledSpectrum>> {
    let mut groups: HashMap<(i32, OrderedFloat<f64>), Vec<&RescaledSpectrum>> = HashMap::new();

    for spectrum in spectra {
        for window in windows {
            if window.contains(spectrum.precursor_mz) {
                let key = (spectrum.target_cycle, OrderedFloat(window.center));
                groups.entry(key).or_default().push(spectrum);
                break;
            }
        }
    }

    groups
}

/// Merge peaks from multiple spectra
///
/// Peaks within `mz_tolerance` Da are merged by summing intensities.
pub fn merge_peaks(spectra: &[&RescaledSpectrum], mz_tolerance: f64) -> SimpleSpectrumData {
    let mut all_peaks: Vec<(f64, f32)> = Vec::new();

    for spectrum in spectra {
        for (mz, intensity) in spectrum
            .data
            .mz_array
            .iter()
            .zip(spectrum.data.intensity_array.iter())
        {
            all_peaks.push((*mz, *intensity));
        }
    }

    all_peaks.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap());

    let mut merged = SimpleSpectrumData::new();

    if all_peaks.is_empty() {
        return merged;
    }

    let mut current_mz = all_peaks[0].0;
    let mut current_intensity = all_peaks[0].1;

    for &(mz, intensity) in all_peaks.iter().skip(1) {
        if mz - current_mz <= mz_tolerance {
            let total_intensity = current_intensity + intensity;
            current_mz = (current_mz * current_intensity as f64 + mz * intensity as f64)
                / total_intensity as f64;
            current_intensity = total_intensity;
        } else {
            merged.mz_array.push(current_mz);
            merged.intensity_array.push(current_intensity);
            current_mz = mz;
            current_intensity = intensity;
        }
    }

    merged.mz_array.push(current_mz);
    merged.intensity_array.push(current_intensity);

    merged
}

/// Create merged DIA spectra from grouped rescaled spectra
pub fn create_merged_dia_spectra(
    groups: &HashMap<(i32, OrderedFloat<f64>), Vec<&RescaledSpectrum>>,
    windows: &[DiaWindow],
    cycle_times: &HashMap<i32, f32>,
    mz_tolerance: f64,
) -> Vec<MergedDiaSpectrum> {
    let mut merged_spectra = Vec::new();

    for ((cycle, window_center), spectra) in groups {
        let window = windows
            .iter()
            .find(|w| OrderedFloat(w.center) == *window_center)
            .expect("Window not found");

        let merged_data = merge_peaks(spectra, mz_tolerance);
        let time = cycle_times.get(cycle).copied().unwrap_or(0.0);

        merged_spectra.push(MergedDiaSpectrum {
            cycle: *cycle,
            window_center: window.center,
            window_min_mz: window.min_mz,
            window_max_mz: window.max_mz,
            time,
            data: merged_data,
            contributing_count: spectra.len(),
        });
    }

    merged_spectra.sort_by(|a, b| match a.cycle.cmp(&b.cycle) {
        std::cmp::Ordering::Equal => a.window_center.partial_cmp(&b.window_center).unwrap(),
        other => other,
    });

    merged_spectra
}

// ============================================================================
// DDA to DIA Converter
// ============================================================================

/// Main converter for DDA to DIA conversion
pub struct Dda2DiaConverter {
    mzdb_path: String,
    peakeldb_path: String,
    options: DiaConversionOptions,
}

impl Dda2DiaConverter {
    /// Create a new converter
    pub fn new(mzdb_path: &str, peakeldb_path: &str, options: DiaConversionOptions) -> Result<Self> {
        Ok(Self {
            mzdb_path: mzdb_path.to_string(),
            peakeldb_path: peakeldb_path.to_string(),
            options,
        })
    }

    /// Perform the DDA to DIA conversion
    pub fn convert(&self, output_path: &str) -> Result<DiaConversionStats> {
        log::info!("DDA to DIA Converter");
        log::info!("Input mzDB: {}", self.mzdb_path);
        log::info!("Input peakeldb: {}", self.peakeldb_path);
        log::info!("Output: {}", output_path);

        // Open input files
        log::info!("Opening input files...");
        let mzdb_conn =
            Connection::open(&self.mzdb_path).context("Failed to open mzDB file")?;
        let peakeldb =
            PeakelDbReader::open(&self.peakeldb_path).context("Failed to open peakeldb file")?;

        // Read peakels
        log::info!("Reading peakels...");
        let peakels = peakeldb.read_all_peakels()?;
        log::info!("Loaded {} peakels", peakels.len());

        // Read spectrum headers
        log::info!("Reading spectrum headers...");
        let ms1_headers = get_spectrum_headers_by_ms_level(&mzdb_conn, 1)?;
        let ms2_headers = get_spectrum_headers_by_ms_level(&mzdb_conn, 2)?;
        log::info!(
            "Found {} MS1 and {} MS2 spectra",
            ms1_headers.len(),
            ms2_headers.len()
        );

        // Build cycle to time mapping from MS1 spectra
        let cycle_times: HashMap<i32, f32> =
            ms1_headers.iter().map(|h| (h.cycle, h.time)).collect();

        // Build spectrum_id to cycle mapping for all spectra
        let all_headers = get_all_spectrum_headers(&mzdb_conn)?;
        let spectrum_to_cycle: HashMap<i64, i32> =
            all_headers.iter().map(|h| (h.id, h.cycle)).collect();

        // Generate DIA windows
        let dia_windows = generate_dia_windows(
            self.options.window_start,
            self.options.window_end,
            self.options.window_width,
        );
        log::info!(
            "Generated {} DIA windows ({}-{} Da, {} Da width)",
            dia_windows.len(),
            self.options.window_start,
            self.options.window_end,
            self.options.window_width
        );

        // Build peakel index
        log::info!("Building peakel index...");
        let peakel_index = build_peakel_index(&peakels);

        // Process MS2 spectra
        log::info!("Processing MS2 spectra...");
        let rescaled_spectra = process_ms2_spectra(
            &mzdb_conn,
            &ms2_headers,
            &peakels,
            &peakel_index,
            self.options.precursor_tolerance_ppm,
            &spectrum_to_cycle,
        )?;
        log::info!("Created {} rescaled spectra", rescaled_spectra.len());

        // Group and merge spectra
        log::info!("Grouping and merging spectra...");
        let groups = group_spectra_by_cycle_and_window(&rescaled_spectra, &dia_windows);
        log::info!("Created {} groups", groups.len());

        let merged_spectra = create_merged_dia_spectra(
            &groups,
            &dia_windows,
            &cycle_times,
            self.options.mz_tolerance,
        );
        log::info!("Created {} merged DIA spectra", merged_spectra.len());

        // Write output
        log::info!("Writing DIA mzDB file...");
        write_dia_mzdb(&mzdb_conn, &merged_spectra, &ms1_headers, Path::new(output_path))?;

        log::info!("Done!");

        Ok(DiaConversionStats {
            input_ms1_spectra: ms1_headers.len(),
            input_ms2_spectra: ms2_headers.len(),
            peakels_loaded: peakels.len(),
            dia_windows: dia_windows.len(),
            rescaled_spectra: rescaled_spectra.len(),
            merged_spectra: merged_spectra.len(),
        })
    }
}

/// Statistics from a DDA to DIA conversion
#[derive(Debug, Clone)]
pub struct DiaConversionStats {
    /// Number of input MS1 spectra
    pub input_ms1_spectra: usize,
    /// Number of input MS2 spectra
    pub input_ms2_spectra: usize,
    /// Number of peakels loaded
    pub peakels_loaded: usize,
    /// Number of DIA windows generated
    pub dia_windows: usize,
    /// Number of rescaled spectra created
    pub rescaled_spectra: usize,
    /// Number of merged DIA spectra written
    pub merged_spectra: usize,
}

// ============================================================================
// Internal Helper Functions
// ============================================================================

/// Get spectrum headers from mzDB connection by MS level
fn get_spectrum_headers_by_ms_level(
    conn: &Connection,
    ms_level: i32,
) -> Result<Vec<SimpleSpectrumHeader>> {
    let mut stmt = conn.prepare(
        "SELECT id, initial_id, title, cycle, time, ms_level, activation_type,
                tic, base_peak_mz, base_peak_intensity, main_precursor_mz,
                main_precursor_charge, data_points_count, param_tree,
                scan_list, precursor_list, product_list, data_encoding_id,
                bb_first_spectrum_id
         FROM spectrum
         WHERE ms_level = ?1
         ORDER BY id",
    )?;

    let headers = stmt.query_map([ms_level], |row| {
        Ok(SimpleSpectrumHeader {
            id: row.get(0)?,
            initial_id: row.get(1)?,
            title: row.get(2)?,
            cycle: row.get(3)?,
            time: row.get(4)?,
            ms_level: row.get(5)?,
            activation_type: row.get(6)?,
            tic: row.get(7)?,
            base_peak_mz: row.get(8)?,
            base_peak_intensity: row.get(9)?,
            main_precursor_mz: row.get(10)?,
            main_precursor_charge: row.get(11)?,
            data_points_count: row.get(12)?,
            param_tree: row.get(13)?,
            scan_list: row.get(14)?,
            precursor_list: row.get(15)?,
            product_list: row.get(16)?,
            data_encoding_id: row.get(17)?,
            bb_first_spectrum_id: row.get(18)?,
        })
    })?;

    headers
        .collect::<Result<Vec<_>, _>>()
        .context("Failed to read spectrum headers")
}

/// Get all spectrum headers
fn get_all_spectrum_headers(conn: &Connection) -> Result<Vec<SimpleSpectrumHeader>> {
    let mut stmt = conn.prepare(
        "SELECT id, initial_id, title, cycle, time, ms_level, activation_type,
                tic, base_peak_mz, base_peak_intensity, main_precursor_mz,
                main_precursor_charge, data_points_count, param_tree,
                scan_list, precursor_list, product_list, data_encoding_id,
                bb_first_spectrum_id
         FROM spectrum
         ORDER BY id",
    )?;

    let headers = stmt.query_map([], |row| {
        Ok(SimpleSpectrumHeader {
            id: row.get(0)?,
            initial_id: row.get(1)?,
            title: row.get(2)?,
            cycle: row.get(3)?,
            time: row.get(4)?,
            ms_level: row.get(5)?,
            activation_type: row.get(6)?,
            tic: row.get(7)?,
            base_peak_mz: row.get(8)?,
            base_peak_intensity: row.get(9)?,
            main_precursor_mz: row.get(10)?,
            main_precursor_charge: row.get(11)?,
            data_points_count: row.get(12)?,
            param_tree: row.get(13)?,
            scan_list: row.get(14)?,
            precursor_list: row.get(15)?,
            product_list: row.get(16)?,
            data_encoding_id: row.get(17)?,
            bb_first_spectrum_id: row.get(18)?,
        })
    })?;

    headers
        .collect::<Result<Vec<_>, _>>()
        .context("Failed to read spectrum headers")
}

/// Build an index of peakels by m/z for fast lookup
fn build_peakel_index(peakels: &[Peakel]) -> HashMap<i64, Vec<usize>> {
    let mut index: HashMap<i64, Vec<usize>> = HashMap::new();

    for (i, peakel) in peakels.iter().enumerate() {
        let bin = peakel.mz as i64;
        for b in (bin - 1)..=(bin + 1) {
            index.entry(b).or_default().push(i);
        }
    }

    index
}

/// Get spectrum data from a bounding box
fn get_spectrum_data(conn: &Connection, spectrum_id: i64) -> Result<SimpleSpectrumData> {
    // Get the bounding box containing this spectrum
    let bb_data: Option<Vec<u8>> = conn
        .query_row(
            "SELECT bb.data FROM bounding_box bb
             JOIN spectrum s ON s.bb_first_spectrum_id = bb.first_spectrum_id
             WHERE s.id = ?1",
            params![spectrum_id],
            |row| row.get(0),
        )
        .ok();

    let bb_data = match bb_data {
        Some(data) => data,
        None => {
            // Try direct lookup
            conn.query_row(
                "SELECT bb.data FROM bounding_box bb
                 WHERE bb.first_spectrum_id <= ?1
                 ORDER BY bb.first_spectrum_id DESC
                 LIMIT 1",
                params![spectrum_id],
                |row| row.get(0),
            )
            .ok()
            .unwrap_or_default()
        }
    };

    if bb_data.is_empty() {
        return Ok(SimpleSpectrumData::new());
    }

    parse_spectrum_from_bb(&bb_data, spectrum_id)
}

/// Parse spectrum data from a bounding box blob
fn parse_spectrum_from_bb(bb_data: &[u8], spectrum_id: i64) -> Result<SimpleSpectrumData> {
    let mut data = SimpleSpectrumData::new();
    let mut offset = 0;

    while offset + 8 <= bb_data.len() {
        let slice_spectrum_id = i32::from_le_bytes([
            bb_data[offset],
            bb_data[offset + 1],
            bb_data[offset + 2],
            bb_data[offset + 3],
        ]) as i64;
        offset += 4;

        let peaks_count = i32::from_le_bytes([
            bb_data[offset],
            bb_data[offset + 1],
            bb_data[offset + 2],
            bb_data[offset + 3],
        ]) as usize;
        offset += 4;

        if slice_spectrum_id == spectrum_id {
            for _ in 0..peaks_count {
                if offset + 12 > bb_data.len() {
                    break;
                }

                let mz = f64::from_le_bytes([
                    bb_data[offset],
                    bb_data[offset + 1],
                    bb_data[offset + 2],
                    bb_data[offset + 3],
                    bb_data[offset + 4],
                    bb_data[offset + 5],
                    bb_data[offset + 6],
                    bb_data[offset + 7],
                ]);
                offset += 8;

                let intensity = f32::from_le_bytes([
                    bb_data[offset],
                    bb_data[offset + 1],
                    bb_data[offset + 2],
                    bb_data[offset + 3],
                ]);
                offset += 4;

                data.mz_array.push(mz);
                data.intensity_array.push(intensity);
            }
            break;
        } else {
            offset += peaks_count * 12;
        }
    }

    Ok(data)
}

/// Process MS2 spectra and create rescaled spectra for each matching peakel
fn process_ms2_spectra(
    conn: &Connection,
    ms2_headers: &[SimpleSpectrumHeader],
    peakels: &[Peakel],
    peakel_index: &HashMap<i64, Vec<usize>>,
    tolerance_ppm: f64,
    spectrum_to_cycle: &HashMap<i64, i32>,
) -> Result<Vec<RescaledSpectrum>> {
    let mut rescaled_spectra = Vec::new();
    let mut best_spectra_for_peakel: HashMap<i32, (i32, SimpleSpectrumHeader, SimpleSpectrumData)> =
        HashMap::new();

    // First pass: find the best MS2 spectrum for each peakel (closest to apex)
    for header in ms2_headers {
        let precursor_mz = match header.main_precursor_mz {
            Some(mz) => mz,
            None => continue,
        };

        let bin = precursor_mz as i64;
        let candidate_indices = match peakel_index.get(&bin) {
            Some(indices) => indices,
            None => continue,
        };

        for &peakel_idx in candidate_indices {
            let peakel = &peakels[peakel_idx];

            if !peakel.contains_mz(precursor_mz, tolerance_ppm) {
                continue;
            }

            let ms2_time = header.time;
            let peakel_start_time = peakel.elution_times.first().copied().unwrap_or(0.0);
            let peakel_end_time = peakel.elution_times.last().copied().unwrap_or(0.0);

            if ms2_time < peakel_start_time || ms2_time > peakel_end_time {
                continue;
            }

            let apex_idx = peakel
                .apex_index()
                .unwrap_or(peakel.spectrum_ids.len() / 2);

            let closest_idx = peakel
                .elution_times
                .iter()
                .enumerate()
                .min_by(|(_, a), (_, b)| {
                    ((*a - ms2_time).abs())
                        .partial_cmp(&((*b - ms2_time).abs()))
                        .unwrap()
                })
                .map(|(i, _)| i)
                .unwrap_or(0);

            let distance_to_apex = (apex_idx as i32 - closest_idx as i32).abs();

            let dominated = best_spectra_for_peakel
                .get(&peakel.id)
                .map(|(best_dist, _, _)| distance_to_apex >= *best_dist)
                .unwrap_or(false);

            if !dominated {
                let spectrum_data = get_spectrum_data(conn, header.id)?;
                if spectrum_data.peaks_count() > 0 {
                    best_spectra_for_peakel.insert(
                        peakel.id,
                        (distance_to_apex, header.clone(), spectrum_data),
                    );
                }
            }
        }
    }

    log::info!(
        "Found {} peakels with matching MS2 spectra",
        best_spectra_for_peakel.len()
    );

    // Second pass: create rescaled spectra for each peakel data point
    for (peakel_id, (_, header, spectrum_data)) in &best_spectra_for_peakel {
        let peakel = peakels.iter().find(|p| p.id == *peakel_id).unwrap();
        let apex_intensity = peakel.apex_intensity;

        for (i, &target_spectrum_id) in peakel.spectrum_ids.iter().enumerate() {
            let point_intensity = peakel.intensities[i];
            let target_cycle = spectrum_to_cycle
                .get(&target_spectrum_id)
                .copied()
                .unwrap_or(1);

            let scale_factor = if apex_intensity > 0.0 {
                point_intensity / apex_intensity
            } else {
                1.0
            };

            let mut scaled_data = spectrum_data.clone();
            scaled_data.scale_intensities(scale_factor);

            rescaled_spectra.push(RescaledSpectrum {
                original_spectrum_id: header.id,
                peakel_id: *peakel_id,
                target_cycle,
                precursor_mz: peakel.mz,
                scale_factor,
                data: scaled_data,
            });
        }
    }

    Ok(rescaled_spectra)
}

// ============================================================================
// DIA Writer Functions
// ============================================================================

/// Write a DIA mzDB file from the original DDA file and merged spectra
fn write_dia_mzdb(
    source_conn: &Connection,
    merged_spectra: &[MergedDiaSpectrum],
    _ms1_headers: &[SimpleSpectrumHeader],
    output_path: &Path,
) -> Result<()> {
    // Get source path
    let source_path = source_conn.path().context("Database has no path")?;

    // Copy the original database as a starting point
    std::fs::copy(source_path, output_path).context("Failed to copy source database")?;

    // Open the copy for modification
    let conn = Connection::open(output_path).context("Failed to open output database")?;

    // Disable foreign key checks
    conn.execute_batch("PRAGMA foreign_keys = OFF;")?;

    // Begin transaction
    conn.execute_batch("BEGIN TRANSACTION;")?;

    // Delete all MS2 spectra from the original
    conn.execute("DELETE FROM spectrum WHERE ms_level = 2", [])?;

    // Delete MS2 bounding boxes
    conn.execute(
        "DELETE FROM bounding_box WHERE run_slice_id IN 
         (SELECT id FROM run_slice WHERE ms_level = 2)",
        [],
    )?;

    // Delete MS2 run slices
    conn.execute("DELETE FROM run_slice WHERE ms_level = 2", [])?;

    // Get the next spectrum ID
    let max_ms1_id: i64 = conn.query_row(
        "SELECT COALESCE(MAX(id), 0) FROM spectrum",
        [],
        |row| row.get(0),
    )?;

    let mut next_spectrum_id = max_ms1_id + 1;

    // Get or create data encoding for MS2
    let data_encoding_id = get_or_create_data_encoding(&conn)?;

    // Get run_id
    let run_id: i64 = conn.query_row("SELECT id FROM run LIMIT 1", [], |row| row.get(0))?;

    // Get reference metadata from MS1 spectra
    let (instr_config_id, source_file_id, data_proc_id): (i64, i64, i64) = conn
        .query_row(
            "SELECT instrument_configuration_id, source_file_id, data_processing_id 
         FROM spectrum WHERE ms_level = 1 LIMIT 1",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .unwrap_or((1, 1, 1));

    // Create DIA run slices for each window
    let mut window_to_run_slice: HashMap<OrderedFloat<f64>, i64> = HashMap::new();

    let mut run_slice_number = 1i64;
    for spectrum in merged_spectra {
        let key = OrderedFloat(spectrum.window_center);
        if !window_to_run_slice.contains_key(&key) {
            conn.execute(
                "INSERT INTO run_slice (ms_level, number, begin_mz, end_mz, run_id)
                 VALUES (2, ?1, ?2, ?3, ?4)",
                params![
                    run_slice_number,
                    spectrum.window_min_mz,
                    spectrum.window_max_mz,
                    run_id
                ],
            )?;
            let run_slice_id = conn.last_insert_rowid();
            window_to_run_slice.insert(key, run_slice_id);
            run_slice_number += 1;
        }
    }

    // Group merged spectra by bounding box
    let mut bb_to_spectra: HashMap<i64, Vec<&MergedDiaSpectrum>> = HashMap::new();

    for spectrum in merged_spectra {
        let key = OrderedFloat(spectrum.window_center);
        let run_slice_id = window_to_run_slice[&key];
        bb_to_spectra.entry(run_slice_id).or_default().push(spectrum);
    }

    // Check if MSn R-tree exists (do this once, not per bounding box)
    let has_msn_rtree: bool = conn.query_row(
        "SELECT COUNT(*) > 0 FROM sqlite_master WHERE type='table' AND name='bounding_box_msn_rtree'",
        [],
        |row| row.get(0),
    )?;

    // Insert bounding boxes and spectra
    for (run_slice_id, spectra) in &bb_to_spectra {
        if spectra.is_empty() {
            continue;
        }

        let first_spectrum_id = next_spectrum_id;
        let last_spectrum_id = next_spectrum_id + spectra.len() as i64 - 1;

        // Serialize using the generic writer utility (no data cloning needed)
        let bb_data = serialize_to_bounding_box(
            spectra.iter().enumerate().map(|(i, spectrum)| {
                (next_spectrum_id + i as i64, &spectrum.data)
            })
        );

        // Insert bounding box
        let bb_id = insert_bounding_box_data(&conn, &bb_data, *run_slice_id, first_spectrum_id, last_spectrum_id)?;

        // Insert spectra
        for (i, spectrum) in spectra.iter().enumerate() {
            let spectrum_id = next_spectrum_id + i as i64;

            let param_tree = generate_param_tree_xml(spectrum.time);
            let precursor_list = generate_precursor_list_xml(
                spectrum.window_center,
                spectrum.window_max_mz - spectrum.window_center,
            );

            let title = format!(
                "cycle={} msLevel=2 window={:.1}-{:.1}",
                spectrum.cycle, spectrum.window_min_mz, spectrum.window_max_mz
            );

            let tic: f64 = spectrum
                .data
                .intensity_array
                .iter()
                .map(|&i| i as f64)
                .sum();

            let (base_peak_mz, base_peak_intensity) = spectrum
                .data
                .mz_array
                .iter()
                .zip(spectrum.data.intensity_array.iter())
                .max_by(|a, b| a.1.partial_cmp(b.1).unwrap())
                .map(|(&mz, &int)| (mz, int))
                .unwrap_or((0.0, 0.0));

            conn.execute(
                "INSERT INTO spectrum (
                    id, initial_id, title, cycle, time, ms_level, activation_type,
                    tic, base_peak_mz, base_peak_intensity, main_precursor_mz,
                    main_precursor_charge, data_points_count, param_tree,
                    scan_list, precursor_list, product_list, 
                    shared_param_tree_id, instrument_configuration_id, source_file_id,
                    run_id, data_processing_id, data_encoding_id, bb_first_spectrum_id
                ) VALUES (
                    ?1, ?2, ?3, ?4, ?5, 2, 'HCD',
                    ?6, ?7, ?8, ?9,
                    NULL, ?10, ?11,
                    NULL, ?12, NULL,
                    NULL, ?13, ?14,
                    ?15, ?16, ?17, ?18
                )",
                params![
                    spectrum_id,
                    spectrum_id,
                    title,
                    spectrum.cycle,
                    spectrum.time,
                    tic,
                    base_peak_mz,
                    base_peak_intensity,
                    spectrum.window_center,
                    spectrum.data.mz_array.len() as i32,
                    param_tree,
                    precursor_list,
                    instr_config_id,
                    source_file_id,
                    run_id,
                    data_proc_id,
                    data_encoding_id,
                    first_spectrum_id,
                ],
            )?;
        }

        next_spectrum_id += spectra.len() as i64;

        // Insert R-tree entry if MSn R-tree exists (using writer utility)
        if has_msn_rtree {
            let min_time = spectra
                .iter()
                .map(|s| s.time)
                .fold(f32::INFINITY, f32::min);
            let max_time = spectra
                .iter()
                .map(|s| s.time)
                .fold(f32::NEG_INFINITY, f32::max);
            let min_mz = spectra
                .iter()
                .map(|s| s.window_min_mz)
                .fold(f64::INFINITY, f64::min);
            let max_mz = spectra
                .iter()
                .map(|s| s.window_max_mz)
                .fold(f64::NEG_INFINITY, f64::max);

            insert_msn_rtree_entry(
                &conn,
                bb_id,
                2, // ms_level
                min_mz, max_mz,  // parent m/z range (isolation window)
                min_mz, max_mz,  // m/z range
                min_time as f64, max_time as f64,
            )?;
        }
    }

    // Commit transaction
    conn.execute_batch("COMMIT;")?;
    conn.execute_batch("PRAGMA optimize;")?;

    log::info!("Successfully wrote DIA file: {:?}", output_path);

    Ok(())
}

/// Get or create a data encoding entry for 64-bit m/z, 32-bit intensity
fn get_or_create_data_encoding(conn: &Connection) -> Result<i64> {
    let existing: Option<i64> = conn
        .query_row(
            "SELECT id FROM data_encoding WHERE mode = 'centroid' LIMIT 1",
            [],
            |row| row.get(0),
        )
        .ok();

    if let Some(id) = existing {
        return Ok(id);
    }

    conn.execute(
        "INSERT INTO data_encoding (mode, compression, byte_order, mz_precision, intensity_precision)
         VALUES ('centroid', 'none', 'little_endian', 64, 32)",
        [],
    )?;

    Ok(conn.last_insert_rowid())
}

/// Generate param_tree XML for a spectrum
fn generate_param_tree_xml(time: f32) -> String {
    format!(
        r#"<params>
  <cvParam cvRef="MS" accession="MS:1000511" value="2" name="ms level"/>
  <cvParam cvRef="MS" accession="MS:1000580" value="" name="MSn spectrum"/>
  <cvParam cvRef="MS" accession="MS:1000127" value="" name="centroid spectrum"/>
  <cvParam cvRef="MS" accession="MS:1000016" value="{:.4}" name="scan start time" unitCvRef="UO" unitAccession="UO:0000031" unitName="minute"/>
</params>"#,
        time / 60.0
    )
}

/// Generate precursor_list XML for a DIA spectrum
fn generate_precursor_list_xml(target_mz: f64, half_width: f64) -> String {
    format!(
        r#"<precursorList count="1">
  <precursor>
    <isolationWindow>
      <cvParam cvRef="MS" accession="MS:1000827" value="{:.4}" name="isolation window target m/z" unitCvRef="MS" unitAccession="MS:1000040" unitName="m/z"/>
      <cvParam cvRef="MS" accession="MS:1000828" value="{:.4}" name="isolation window lower offset" unitCvRef="MS" unitAccession="MS:1000040" unitName="m/z"/>
      <cvParam cvRef="MS" accession="MS:1000829" value="{:.4}" name="isolation window upper offset" unitCvRef="MS" unitAccession="MS:1000040" unitName="m/z"/>
    </isolationWindow>
    <activation>
      <cvParam cvRef="MS" accession="MS:1000422" value="" name="beam-type collision-induced dissociation"/>
    </activation>
  </precursor>
</precursorList>"#,
        target_mz, half_width, half_width
    )
}

