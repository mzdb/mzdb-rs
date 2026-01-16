//! Bounding Box utilities for reading spectrum slices from BB blobs
//!
//! This module provides functionality for:
//! - Indexing bounding boxes to extract spectrum slice metadata
//! - Converting bounding boxes to spectrum slices (like Java's BoundingBox.toSpectrumSlices())
//! - Reading spectrum slice data with optional m/z filtering
//!
//! # Java Reference
//! This module ports functionality from:
//! - `fr.profi.mzdb.model.BoundingBox.toSpectrumSlices()`
//! - `fr.profi.mzdb.io.reader.bb.BytesReader`
//! - `fr.profi.mzdb.io.reader.bb.IBlobReader`

use anyhow_ext::{anyhow, bail, Context, Result};
use rusqlite::Row;
use smallvec::SmallVec;

use crate::model::*;

// Re-export from queries for backward compatibility
pub use crate::queries::{read_spectrum_slice_data_at, read_spectrum_slice_into_buffer, SpectrumParseBuffer};

// ============================================================================
// BoundingBox Construction
// ============================================================================

/// Create a BoundingBox from a SQLite row
///
/// Ported from Java BoundingBoxBuilder.buildBB()
pub fn create_bbox(row: &Row) -> Result<BoundingBox> {
    let bb_id: i64 = row.get(0).context("Failed to get bb_id")?;
    let blob_data = row
        .get_ref(1)
        .context("Failed to get blob_data ref")?
        .as_blob()
        .context("blob_data is not a blob")?;
    let run_slice_id: i64 = row.get(2).context("Failed to get run_slice_id")?;
    let first_spectrum_id: i64 = row.get(3).context("Failed to get first_spectrum_id")?;
    let last_spectrum_id: i64 = row.get(4).context("Failed to get last_spectrum_id")?;

    Ok(BoundingBox {
        id: bb_id,
        blob_data: blob_data.to_vec(),
        run_slice_id,
        first_spectrum_id,
        last_spectrum_id,
    })
}

// ============================================================================
// BoundingBox Indexing
// ============================================================================

fn bytes_to_int(bytes: &[u8; 4]) -> i32 {
    i32::from_le_bytes(*bytes)
}

/// Index a bounding box to extract spectrum slice metadata
///
/// This scans the BB blob to build an index of:
/// - Byte offsets for each spectrum slice
/// - Spectrum IDs
/// - Peak counts
///
/// Ported from Java BytesReader constructor logic
pub fn index_bbox(bbox: &BoundingBox, cache: &DataEncodingsCache) -> Result<BoundingBoxIndex> {
    let mut slices_indexes: SmallVec<[usize; 16]> = SmallVec::new();
    let mut spectra_ids: SmallVec<[i64; 16]> = SmallVec::new();
    let mut peaks_counts: SmallVec<[usize; 16]> = SmallVec::new();

    let mut slices_count = 0;

    let blob_data = bbox.blob_data.as_slice();
    let n_bytes = blob_data.len();
    let mut int_as_bytes = [0u8; 4];

    let mut bytes_idx = 0;
    while bytes_idx < n_bytes {
        slices_indexes.push(bytes_idx);

        int_as_bytes.clone_from_slice(&blob_data[bytes_idx..=bytes_idx + 3]);
        let spectrum_id = bytes_to_int(&int_as_bytes) as i64;
        spectra_ids.push(spectrum_id);

        int_as_bytes.clone_from_slice(&blob_data[bytes_idx + 4..=bytes_idx + 7]);
        let peak_count = bytes_to_int(&int_as_bytes) as usize;
        peaks_counts.push(peak_count);

        let de = cache
            .get_data_encoding_by_spectrum_id(&spectrum_id)
            .ok_or(anyhow!("can't find data encoding for spectrum {}", spectrum_id))
            .context("index_bbox failed")?;

        let peak_size = de.get_peak_size();
        bytes_idx += 8 + peak_count * peak_size;

        slices_count += 1;
    }

    Ok(BoundingBoxIndex {
        bb_id: bbox.id,
        spectrum_slices_count: slices_count,
        spectra_ids,
        slices_indexes,
        peaks_counts,
    })
}

// ============================================================================
// Spectrum Slice Extraction
// ============================================================================

/// Convert a BoundingBox to an array of SpectrumSlices
///
/// This is the Rust port of Java's `BoundingBox.toSpectrumSlices()` method.
/// It reads all spectrum slices from the BB blob and returns them as a vector.
///
/// # Arguments
/// * `bbox` - The bounding box containing the blob data
/// * `bbox_index` - Pre-computed index of the BB structure
/// * `cache` - Entity cache with spectrum headers and data encodings
///
/// # Returns
/// Vector of SpectrumSlice objects, one per spectrum in the BB
///
/// # Java Reference
/// ```java
/// public SpectrumSlice[] toSpectrumSlices() {
///     SpectrumSlice[] spectrumSliceArray = _reader.readAllSpectrumSlices(this._runSliceId);
///     // ... validation logic ...
///     return spectrumSliceArray;
/// }
/// ```
pub fn to_spectrum_slices(
    bbox: &BoundingBox,
    bbox_index: &BoundingBoxIndex,
    cache: &EntityCache,
) -> Result<Vec<SpectrumSlice>> {
    let mut spectrum_slices = Vec::with_capacity(bbox_index.spectrum_slices_count);

    for slice_idx in 0..bbox_index.spectrum_slices_count {
        let spectrum_id = bbox_index.spectra_ids[slice_idx];
        
        // Get spectrum header
        let spectrum_header = cache
            .get_spectrum_header(spectrum_id)
            .ok_or_else(|| anyhow!("Spectrum header not found for id: {}", spectrum_id))?
            .clone();

        // Get data encoding
        let data_encoding = cache
            .data_encodings_cache
            .get_data_encoding_by_spectrum_id(&spectrum_id)
            .ok_or_else(|| anyhow!("Data encoding not found for spectrum id: {}", spectrum_id))?
            .clone();

        // Read spectrum slice data (no m/z filtering)
        let spectrum_data = read_spectrum_slice_data_at(
            bbox,
            bbox_index,
            &data_encoding,
            slice_idx,
            None, // min_mz
            None, // max_mz
        )?;

        spectrum_slices.push(SpectrumSlice {
            spectrum: Spectrum {
                header: spectrum_header,
                data: spectrum_data,
            },
            run_slice_id: bbox.run_slice_id,
        });
    }

    // Validate no duplicate spectrum IDs (ported from Java workaround)
    let mut seen_ids = std::collections::HashSet::new();
    for slice in &spectrum_slices {
        if !seen_ids.insert(slice.spectrum.header.id) {
            bail!("Duplicate spectrum id detected: {}", slice.spectrum.header.id);
        }
    }

    Ok(spectrum_slices)
}

/// Read all spectrum IDs from a bounding box
///
/// Ported from Java IBlobReader.getAllSpectrumIds()
pub fn get_all_spectrum_ids(bbox_index: &BoundingBoxIndex) -> Vec<i64> {
    bbox_index.spectra_ids.to_vec()
}

/// Get spectrum ID at a specific index
///
/// Ported from Java IBlobReader.getSpectrumIdAt()
pub fn get_spectrum_id_at(bbox_index: &BoundingBoxIndex, idx: usize) -> Result<i64> {
    bbox_index
        .spectra_ids
        .get(idx)
        .copied()
        .ok_or_else(|| anyhow!("Index {} out of bounds (max: {})", idx, bbox_index.spectra_ids.len()))
}

/// Get the number of spectra in a bounding box
///
/// Ported from Java IBlobReader.getSpectraCount()
pub fn get_spectra_count(bbox_index: &BoundingBoxIndex) -> usize {
    bbox_index.spectrum_slices_count
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bytes_to_int() {
        let bytes = [0x01, 0x00, 0x00, 0x00]; // Little endian 1
        assert_eq!(bytes_to_int(&bytes), 1);

        let bytes = [0xFF, 0xFF, 0xFF, 0xFF]; // Little endian -1
        assert_eq!(bytes_to_int(&bytes), -1);
    }
}
