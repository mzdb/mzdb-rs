//! DIA Leftover Mask — Generate a leftover mzDB by removing detected peakel peaks
//!
//! Given an original DIA mzDB file and its MS2 peakeldb, this module produces
//! a new mzDB file containing only the MS2 signal that was **not** part of any
//! detected peakel. MS1 spectra are copied as-is.
//!
//! The output preserves the original file structure exactly: same isolation windows,
//! same spectrum count, same staggered layout (if applicable). For each MS2 spectrum,
//! peaks whose m/z matches any data point in any peakel referencing that spectrum are
//! discarded. The remaining peaks form the leftover (residual) signal.
//!
//! This is independent from the DIA simplifier — the consumed set is built from the
//! **full extent** of each peakel (all data points, not just the apex ± N subset
//! retained by the simplifier).
//!
//! # Example
//!
//! ```no_run
//! use mzdb::processing::dia_leftover_mask::DiaLeftoverMask;
//!
//! let mzdb_path = "dia_file.mzDB";
//! let peakeldb_path = "peakels.peakeldb";
//! let leftover_mzdb_path = std::path::PathBuf::from("leftover.mzDB");
//!
//! let stats = DiaLeftoverMask::remove_detected_peakels(mzdb_path, peakeldb_path, &leftover_mzdb_path).unwrap();
//! println!("Removed {} peaks, {} residual peaks remain",
//!          stats.total_peaks_removed, stats.total_residual_peaks);
//! ```

use std::collections::{HashMap, HashSet};
use std::path::Path;

use anyhow_ext::{Context, Result};
use fallible_iterator::FallibleIterator;
use ordered_float::OrderedFloat;

use crate::MzDbReaderBuilder;
use crate::model::{
    SpectrumData, DataEncoding, DataMode, PeakEncoding, ByteOrder,
};
use crate::processing::peakeldb::{Ms2PeakelDbReader, ExtendedPeakel};
use crate::writer::{MzDbWriterBuilder, WriterMetadata};

// ============================================================================
// Public API
// ============================================================================

/// Statistics from leftover file generation
#[derive(Debug, Clone)]
pub struct LeftoverStats {
    /// Number of peakels used to build the consumed set
    pub peakel_count: usize,
    /// MS2 spectra with at least one residual peak
    pub ms2_with_residual: usize,
    /// MS2 spectra where all peaks were consumed (written as empty)
    pub ms2_fully_consumed: usize,
    /// MS2 spectra with no consumed peaks (written as-is)
    pub ms2_untouched: usize,
    /// Total residual peaks across all MS2 spectra
    pub total_residual_peaks: usize,
    /// Total peaks removed from original spectra
    pub total_peaks_removed: usize,
}

/// DIA Leftover Mask — removes detected peakel signal from a DIA mzDB file
pub struct DiaLeftoverMask;

impl DiaLeftoverMask {
    /// Generate a leftover mzDB file by removing all peakel peaks from the original.
    ///
    /// For each MS2 spectrum, any peak whose m/z appears in a peakel data point
    /// referencing that spectrum is discarded. The output preserves the original
    /// file structure (isolation windows, spectrum count, staggered layout).
    pub fn remove_detected_peakels(
        source_mzdb_path: &str,
        source_ms2_peakeldb_path: &str,
        leftover_mzdb_path: &Path,
    ) -> Result<LeftoverStats> {
        log::info!("DIA Leftover Mask");
        log::info!("Input mzDB: {}", source_mzdb_path);
        log::info!("Input peakeldb: {}", source_ms2_peakeldb_path);
        log::info!("Output: {:?}", leftover_mzdb_path);

        // Read peakels
        log::info!("Reading peakels...");
        let peakeldb = Ms2PeakelDbReader::open(source_ms2_peakeldb_path)
            .context("Failed to open peakeldb file")?;
        let peakels = peakeldb.read_all_peakels()?;
        let peakel_count = peakels.len();
        log::info!("Loaded {} peakels", peakel_count);

        // Build consumed m/z set per spectrum_id from the full peakel extent
        log::info!("Building consumed peaks index...");
        let consumed_mz_per_spectrum = build_consumed_index(&peakels);
        log::info!(
            "  {} MS2 spectra have consumed peaks",
            consumed_mz_per_spectrum.len()
        );

        // Open source mzDB
        log::info!("Opening source mzDB...");
        let source_reader = MzDbReaderBuilder::new(source_mzdb_path).build()?;
        let bb_sizes = source_reader.entity_cache().bb_sizes.clone();

        // Create writer
        log::info!("Creating leftover mzDB...");
        let metadata = WriterMetadata::with_defaults();

        let mut writer = MzDbWriterBuilder::new(leftover_mzdb_path)
            .metadata(metadata)
            .bb_sizes(bb_sizes)
            .is_dia(true)
            .build()?;

        writer.open()?;

        let encoding = DataEncoding {
            id: 1,
            mode: DataMode::Centroid,
            peak_encoding: PeakEncoding::LowRes,
            byte_order: ByteOrder::LittleEndian,
            compression: "none".to_string(),
        };

        // Iterate source spectra and write leftover
        log::info!("Writing leftover spectra...");
        let mut ms1_count = 0usize;
        let mut ms2_with_residual = 0usize;
        let mut ms2_fully_consumed = 0usize;
        let mut ms2_untouched = 0usize;
        let mut total_residual_peaks = 0usize;
        let mut total_peaks_removed = 0usize;
        let mut total_count = 0usize;

        let mut iter = source_reader.iter_spectra(None)?;
        while let Some(spectrum) = iter.next()? {
            total_count += 1;
            if total_count % 1000 == 0 {
                log::info!("  Progress: {} spectra", total_count);
            }

            if spectrum.header.ms_level == 1 {
                writer.insert_spectrum(&spectrum, &encoding)?;
                ms1_count += 1;
                continue;
            }

            if spectrum.header.ms_level != 2 {
                continue;
            }

            let consumed_set = consumed_mz_per_spectrum.get(&spectrum.header.id);

            if consumed_set.is_none() || consumed_set.unwrap().is_empty() {
                // No peaks consumed — write original spectrum unchanged
                writer.insert_spectrum(&spectrum, &encoding)?;
                ms2_untouched += 1;
                total_residual_peaks += spectrum.data.peaks_count;
                continue;
            }

            let consumed_set = consumed_set.unwrap();
            let original_count = spectrum.data.peaks_count;

            // Filter: keep peaks whose m/z is NOT in the consumed set
            let mut residual_mz = Vec::new();
            let mut residual_intensity = Vec::new();

            for i in 0..original_count {
                let mz = spectrum.data.mz_array[i];
                if !consumed_set.contains(&OrderedFloat(mz)) {
                    residual_mz.push(mz);
                    residual_intensity.push(spectrum.data.intensity_array[i]);
                }
            }

            let peaks_removed = original_count - residual_mz.len();
            total_peaks_removed += peaks_removed;

            if residual_mz.is_empty() {
                // All peaks consumed — write empty spectrum to preserve structure
                let mut empty = spectrum.clone();
                empty.header.tic = 0.0;
                empty.header.base_peak_mz = 0.0;
                empty.header.base_peak_intensity = 0.0;
                empty.header.peaks_count = 0;
                empty.data = SpectrumData {
                    data_encoding: encoding.clone(),
                    peaks_count: 0,
                    mz_array: vec![],
                    intensity_array: vec![],
                    lwhm_array: vec![],
                    rwhm_array: vec![],
                };
                writer.insert_spectrum_allow_empty(&empty, &encoding)?;
                ms2_fully_consumed += 1;
            } else {
                let tic: f32 = residual_intensity.iter().sum();
                let (bp_mz, bp_int) = residual_mz.iter()
                    .zip(&residual_intensity)
                    .max_by(|a, b| a.1.total_cmp(b.1))
                    .map(|(&mz, &int)| (mz as f64, int))
                    .unwrap_or((0.0, 0.0));

                let mut residual = spectrum.clone();
                residual.header.tic = tic;
                residual.header.base_peak_mz = bp_mz;
                residual.header.base_peak_intensity = bp_int;
                residual.header.peaks_count = residual_mz.len() as i64;
                residual.data = SpectrumData {
                    data_encoding: encoding.clone(),
                    peaks_count: residual_mz.len(),
                    mz_array: residual_mz,
                    intensity_array: residual_intensity,
                    lwhm_array: vec![],
                    rwhm_array: vec![],
                };
                writer.insert_spectrum(&residual, &encoding)?;
                ms2_with_residual += 1;
                total_residual_peaks += residual.data.peaks_count;
            }
        }

        log::info!("Leftover spectra:");
        log::info!("  MS1: {}", ms1_count);
        log::info!("  MS2 untouched (no peakel overlap): {}", ms2_untouched);
        log::info!("  MS2 with residual: {}", ms2_with_residual);
        log::info!("  MS2 fully consumed: {}", ms2_fully_consumed);
        log::info!("  Total peaks removed: {}", total_peaks_removed);
        log::info!("  Total residual peaks: {}", total_residual_peaks);

        log::info!("Finalizing leftover file...");
        writer.close()?;
        log::info!("Leftover file written successfully!");

        Ok(LeftoverStats {
            peakel_count,
            ms2_with_residual,
            ms2_fully_consumed,
            ms2_untouched,
            total_residual_peaks,
            total_peaks_removed,
        })
    }
}

// ============================================================================
// Consumed Index Builder
// ============================================================================

/// Build a map of spectrum_id → set of consumed m/z values from all peakels.
///
/// Iterates every data point of every peakel and records the (spectrum_id, m/z) pair.
/// Uses `OrderedFloat<f32>` for exact matching — the m/z values in the peakel were
/// read from the same spectra they'll be matched against, so no tolerance is needed.
fn build_consumed_index(
    peakels: &[ExtendedPeakel],
) -> HashMap<i64, HashSet<OrderedFloat<f32>>> {
    let mut consumed: HashMap<i64, HashSet<OrderedFloat<f32>>> = HashMap::new();

    for peakel in peakels {
        let spectrum_ids = peakel.data.spectrum_ids.as_slice();
        let mz_values = peakel.data.mz_values.as_slice();

        for (i, &spectrum_id) in spectrum_ids.iter().enumerate() {
            consumed
                .entry(spectrum_id)
                .or_default()
                .insert(OrderedFloat(mz_values[i]));
        }
    }

    consumed
}
