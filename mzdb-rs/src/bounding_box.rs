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
use crate::model::DataMode::Fitted;

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
// Pre-allocated Buffer API for Hot-Path Spectrum Parsing
// ============================================================================

/// Reusable buffer for spectrum parsing to avoid repeated allocations
/// 
/// In hot loops that process many spectra, creating new `Vec`s for each
/// spectrum creates allocation pressure. This buffer allows reusing
/// memory across calls.
///
/// # Example
///
/// ```ignore
/// use mzdb::{MzDbReader, SpectrumParseBuffer};
///
/// let reader = MzDbReader::open("file.mzDB").unwrap();
/// let mut buffer = SpectrumParseBuffer::new();
///
/// // Process many spectra, reusing the buffer
/// for header in reader.get_spectrum_headers() {
///     // ... get bounding box and index ...
///     // buffer.clear() is called internally by read_spectrum_slice_into_buffer
/// }
/// ```
#[allow(dead_code)] // Public API for library consumers
#[derive(Clone, Debug, Default)]
pub struct SpectrumParseBuffer {
    /// Buffer for m/z values (32-bit for centroid data)
    pub mz_array: Vec<f32>,
    /// Buffer for intensity values
    pub intensity_array: Vec<f32>,
    /// Buffer for left half-width at half-maximum (fitted mode only)
    pub lwhm_array: Vec<f32>,
    /// Buffer for right half-width at half-maximum (fitted mode only)
    pub rwhm_array: Vec<f32>,
}

#[allow(dead_code)]
impl SpectrumParseBuffer {
    /// Create a new empty buffer
    pub fn new() -> Self {
        Self::default()
    }

    /// Create a buffer with pre-allocated capacity
    ///
    /// Use this when you know the typical peak count to avoid
    /// reallocation during initial parsing.
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            mz_array: Vec::with_capacity(capacity),
            intensity_array: Vec::with_capacity(capacity),
            lwhm_array: Vec::with_capacity(capacity),
            rwhm_array: Vec::with_capacity(capacity),
        }
    }

    /// Clear all arrays, keeping allocated capacity
    pub fn clear(&mut self) {
        self.mz_array.clear();
        self.intensity_array.clear();
        self.lwhm_array.clear();
        self.rwhm_array.clear();
    }

    /// Get the number of peaks currently stored
    pub fn len(&self) -> usize {
        self.mz_array.len()
    }

    /// Check if the buffer is empty
    pub fn is_empty(&self) -> bool {
        self.mz_array.is_empty()
    }

    /// Convert buffer contents to SpectrumData (clones the data)
    ///
    /// Use this when you need to store the data. For read-only access,
    /// use the buffer arrays directly.
    pub fn to_spectrum_data(&self, data_encoding: DataEncoding) -> SpectrumData {
        SpectrumData {
            data_encoding,
            peaks_count: self.mz_array.len(),
            mz_array: self.mz_array.clone(),
            intensity_array: self.intensity_array.clone(),
            lwhm_array: self.lwhm_array.clone(),
            rwhm_array: self.rwhm_array.clone(),
        }
    }

    /// Take ownership of the arrays, leaving the buffer empty
    ///
    /// This avoids cloning when you want to store the parsed data
    /// and then create a new buffer.
    pub fn take_as_spectrum_data(&mut self, data_encoding: DataEncoding) -> SpectrumData {
        let peaks_count = self.mz_array.len();
        SpectrumData {
            data_encoding,
            peaks_count,
            mz_array: std::mem::take(&mut self.mz_array),
            intensity_array: std::mem::take(&mut self.intensity_array),
            lwhm_array: std::mem::take(&mut self.lwhm_array),
            rwhm_array: std::mem::take(&mut self.rwhm_array),
        }
    }
}

// ============================================================================
// Spectrum Slice Data Parsing
// ============================================================================

fn read_spectrum_slice_data(
    bb_bytes: &[u8],
    peaks_start_pos: usize,
    peaks_count: usize,
    de: &DataEncoding,
    min_mz: Option<f32>,
    max_mz: Option<f32>,
) -> Result<SpectrumData> {
    let data_mode = de.mode;
    let pe = de.peak_encoding;
    let byte_order = de.byte_order;

    let peak_size = de.get_peak_size();

    let mut float_bytes = [0u8; 4];
    let mut double_bytes = [0u8; 8];

    let mut bytes_to_double = |offset: usize, decode_float: bool| -> (f64, usize) {
        if decode_float {
            float_bytes.clone_from_slice(&bb_bytes[offset..offset + 4]);
            let value = if byte_order == ByteOrder::BigEndian {
                f32::from_be_bytes(float_bytes) as f64
            } else {
                f32::from_le_bytes(float_bytes) as f64
            };
            (value, 4)
        } else {
            double_bytes.clone_from_slice(&bb_bytes[offset..offset + 8]);
            let value = if byte_order == ByteOrder::BigEndian {
                f64::from_be_bytes(double_bytes)
            } else {
                f64::from_le_bytes(double_bytes)
            };
            (value, 8)
        }
    };

    let mut filtered_peaks_count = 0;
    let mut filtered_peaks_start_idx = 0;

    if min_mz.is_none() && max_mz.is_none() {
        filtered_peaks_count = peaks_count;
        filtered_peaks_start_idx = peaks_start_pos;
    } else {
        let max_mz_threshold = max_mz.unwrap_or(f32::MAX);

        let mut i = 0;
        while i < peaks_count {
            let peak_start_pos: usize = peaks_start_pos + i * peak_size;
            let (mz, _offset) = bytes_to_double(peak_start_pos, pe == PeakEncoding::LowRes);
            let mz_f32 = mz as f32;

            if let Some(min) = min_mz
                && mz_f32 >= min && mz_f32 <= max_mz_threshold
            {
                filtered_peaks_count += 1;
                if filtered_peaks_start_idx == 0 {
                    filtered_peaks_start_idx = peak_start_pos;
                }
            }
            i += 1;
        }
    }

    let mut mz_array: Vec<f32> = Vec::with_capacity(filtered_peaks_count);
    let mut intensity_array: Vec<f32> = Vec::with_capacity(filtered_peaks_count);
    let mut lwhm_array: Vec<f32> = if data_mode == Fitted {
        Vec::with_capacity(filtered_peaks_count)
    } else {
        Vec::new()
    };
    let mut rwhm_array: Vec<f32> = if data_mode == Fitted {
        Vec::with_capacity(filtered_peaks_count)
    } else {
        Vec::new()
    };

    let mut float_bytes2 = [0u8; 4];
    let mut double_bytes2 = [0u8; 8];

    let mut bytes_to_float = |offset: usize, decode_float: bool| -> (f32, usize) {
        if decode_float {
            float_bytes2.clone_from_slice(&bb_bytes[offset..offset + 4]);
            let value = if byte_order == ByteOrder::BigEndian {
                f32::from_be_bytes(float_bytes2)
            } else {
                f32::from_le_bytes(float_bytes2)
            };
            (value, 4)
        } else {
            double_bytes2.clone_from_slice(&bb_bytes[offset..offset + 8]);
            let value = if byte_order == ByteOrder::BigEndian {
                f64::from_be_bytes(double_bytes2) as f32
            } else {
                f64::from_le_bytes(double_bytes2) as f32
            };
            (value, 8)
        }
    };

    let mut peak_idx = 0;
    while peak_idx < filtered_peaks_count {
        let peak_bytes_index = filtered_peaks_start_idx + peak_idx * peak_size;
        // Read m/z - convert to f32 regardless of source precision
        let (mz, offset) = bytes_to_double(peak_bytes_index, pe == PeakEncoding::LowRes);
        mz_array.push(mz as f32);

        let (intensity, _offset) =
            bytes_to_float(peak_bytes_index + offset, pe != PeakEncoding::NoLoss);
        intensity_array.push(intensity);

        if data_mode == Fitted {
            let mz_int_size = pe as usize;
            lwhm_array.push(bytes_to_float(peak_bytes_index + mz_int_size, true).0);
            rwhm_array.push(bytes_to_float(peak_bytes_index + mz_int_size + 4, true).0);
        }

        peak_idx += 1;
    }

    Ok(SpectrumData {
        data_encoding: de.clone(),
        peaks_count,
        mz_array,
        intensity_array,
        lwhm_array,
        rwhm_array,
    })
}

/// Parse spectrum slice data into a pre-allocated buffer
///
/// This is the buffer-based variant of `read_spectrum_slice_data_at`.
/// The buffer is cleared before parsing.
#[allow(dead_code)] // Public API for library consumers
pub fn read_spectrum_slice_into_buffer(
    bounding_box: &BoundingBox,
    bbox_index: &BoundingBoxIndex,
    data_encoding: &DataEncoding,
    spectrum_slice_idx: usize,
    min_mz: Option<f32>,
    max_mz: Option<f32>,
    buffer: &mut SpectrumParseBuffer,
) -> Result<()> {
    buffer.clear();

    let peaks_count = bbox_index.peaks_counts[spectrum_slice_idx];
    let peaks_start_pos = bbox_index.slices_indexes[spectrum_slice_idx] + 8;
    let bb_bytes = &bounding_box.blob_data;

    let data_mode = data_encoding.mode;
    let pe = data_encoding.peak_encoding;
    let byte_order = data_encoding.byte_order;
    let peak_size = data_encoding.get_peak_size();

    let mut float_bytes = [0u8; 4];
    let mut double_bytes = [0u8; 8];

    // Helper closures for byte conversion
    let bytes_to_double = |offset: usize, decode_float: bool,
                           float_buf: &mut [u8; 4], double_buf: &mut [u8; 8]| -> (f64, usize) {
        if decode_float {
            float_buf.clone_from_slice(&bb_bytes[offset..offset + 4]);
            let value = if byte_order == ByteOrder::BigEndian {
                f32::from_be_bytes(*float_buf) as f64
            } else {
                f32::from_le_bytes(*float_buf) as f64
            };
            (value, 4)
        } else {
            double_buf.clone_from_slice(&bb_bytes[offset..offset + 8]);
            let value = if byte_order == ByteOrder::BigEndian {
                f64::from_be_bytes(*double_buf)
            } else {
                f64::from_le_bytes(*double_buf)
            };
            (value, 8)
        }
    };

    let bytes_to_float = |offset: usize, float_buf: &mut [u8; 4]| -> f32 {
        float_buf.clone_from_slice(&bb_bytes[offset..offset + 4]);
        if byte_order == ByteOrder::BigEndian {
            f32::from_be_bytes(*float_buf)
        } else {
            f32::from_le_bytes(*float_buf)
        }
    };

    let is_low_res = pe == PeakEncoding::LowRes;
    let is_fitted = data_mode == Fitted;

    // Parse peaks
    for i in 0..peaks_count {
        let peak_start = peaks_start_pos + i * peak_size;

        let (mz, mz_size) = bytes_to_double(peak_start, is_low_res, &mut float_bytes, &mut double_bytes);
        let mz_f32 = mz as f32;

        // Apply m/z filter
        if let Some(min) = min_mz {
            if mz_f32 < min { continue; }
        }
        if let Some(max) = max_mz {
            if mz_f32 > max { continue; }
        }

        // Store m/z as f32
        buffer.mz_array.push(mz_f32);

        let intensity = bytes_to_float(peak_start + mz_size, &mut float_bytes);
        buffer.intensity_array.push(intensity);

        if is_fitted {
            let lwhm = bytes_to_float(peak_start + mz_size + 4, &mut float_bytes);
            let rwhm = bytes_to_float(peak_start + mz_size + 8, &mut float_bytes);
            buffer.lwhm_array.push(lwhm);
            buffer.rwhm_array.push(rwhm);
        }
    }

    Ok(())
}

/// Read spectrum slice data at a specific index within a bounding box
pub fn read_spectrum_slice_data_at(
    bounding_box: &BoundingBox,
    bbox_index: &BoundingBoxIndex,
    data_encoding: &DataEncoding,
    spectrum_slice_idx: usize,
    min_mz: Option<f32>,
    max_mz: Option<f32>,
) -> Result<SpectrumData> {
    let peaks_count = bbox_index.peaks_counts[spectrum_slice_idx];
    let peaks_start_pos = bbox_index.slices_indexes[spectrum_slice_idx] + 8;

    read_spectrum_slice_data(
        &bounding_box.blob_data,
        peaks_start_pos,
        peaks_count,
        data_encoding,
        min_mz,
        max_mz,
    )
}

/// Merge multiple spectrum slices into a single SpectrumData
pub fn merge_spectrum_slices(
    sd_slices: &mut Vec<SpectrumData>,
    peaks_count: usize,
) -> Result<SpectrumData> {
    if sd_slices.is_empty() {
        bail!("Cannot merge empty sd_slices");
    }

    let data_encoding = sd_slices
        .first()
        .map(|sd| sd.data_encoding.clone())
        .context("sd_slices is empty (should not happen)")?;

    let data_mode = data_encoding.mode;

    let mut mz_array: Vec<f32> = Vec::with_capacity(peaks_count);
    let mut intensity_array: Vec<f32> = Vec::with_capacity(peaks_count);
    let mut lwhm_array: Vec<f32> = if data_mode == Fitted {
        Vec::with_capacity(peaks_count)
    } else {
        Vec::new()
    };
    let mut rwhm_array: Vec<f32> = if data_mode == Fitted {
        Vec::with_capacity(peaks_count)
    } else {
        Vec::new()
    };

    for sd_slice in sd_slices {
        mz_array.append(&mut sd_slice.mz_array);
        intensity_array.append(&mut sd_slice.intensity_array);

        if data_mode == Fitted {
            lwhm_array.append(&mut sd_slice.lwhm_array);
            rwhm_array.append(&mut sd_slice.rwhm_array);
        }
    }

    Ok(SpectrumData {
        data_encoding,
        peaks_count,
        mz_array,
        intensity_array,
        lwhm_array,
        rwhm_array,
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
#[allow(dead_code)] // Used by RunSliceIterator
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

// ============================================================================
// Helper Functions (commented out - not currently used)
// ============================================================================

// /// Read all spectrum IDs from a bounding box
// ///
// /// Ported from Java IBlobReader.getAllSpectrumIds()
// pub fn get_all_spectrum_ids(bbox_index: &BoundingBoxIndex) -> Vec<i64> {
//     bbox_index.spectra_ids.to_vec()
// }

// /// Get spectrum ID at a specific index
// ///
// /// Ported from Java IBlobReader.getSpectrumIdAt()
// pub fn get_spectrum_id_at(bbox_index: &BoundingBoxIndex, idx: usize) -> Result<i64> {
//     bbox_index
//         .spectra_ids
//         .get(idx)
//         .copied()
//         .ok_or_else(|| anyhow!("Index {} out of bounds (max: {})", idx, bbox_index.spectra_ids.len()))
// }

// /// Get the number of spectra in a bounding box
// ///
// /// Ported from Java IBlobReader.getSpectraCount()
// pub fn get_spectra_count(bbox_index: &BoundingBoxIndex) -> usize {
//     bbox_index.spectrum_slices_count
// }

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