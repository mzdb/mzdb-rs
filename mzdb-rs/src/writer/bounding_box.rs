//! Bounding Box Cache and Serialization
//!
//! This module handles:
//! - Caching spectrum data in bounding boxes before writing to disk
//! - Binary serialization of peak data
//! - Flushing bounding box rows to the database

use anyhow::{Context, Result};
use std::collections::HashMap;

use crate::model::*;

/// Index into a spectrum's peak data
#[derive(Clone, Debug)]
pub struct SpectrumSliceIndex {
    /// Reference to the spectrum data
    pub spectrum_data: SpectrumData,
    
    /// Index of first peak in this slice
    pub first_peak_idx: usize,
    
    /// Index of last peak in this slice (inclusive)
    pub last_peak_idx: usize,
}

impl SpectrumSliceIndex {
    /// Get the number of peaks in this slice
    pub fn peaks_count(&self) -> usize {
        if self.last_peak_idx < self.first_peak_idx {
            0
        } else {
            1 + self.last_peak_idx - self.first_peak_idx
        }
    }
}

/// A bounding box accumulating spectrum slices
#[derive(Clone, Debug)]
pub struct BoundingBoxWriter {
    pub id: i64,
    pub first_time: f32,
    pub last_time: f32,
    pub run_slice_id: i64,
    pub ms_level: i64,
    pub data_encoding: DataEncoding,
    pub isolation_window: Option<MzRange>,
    pub spectrum_ids: Vec<i64>,
    pub spectrum_slices: Vec<Option<SpectrumSliceIndex>>,
}

/// Cache for accumulating bounding boxes before writing
pub struct BoundingBoxCache {
    /// BB dimensions
    bb_sizes: BBSizes,
    
    /// Map from (run_slice_id, isolation_window) to BoundingBox
    bounding_boxes: HashMap<(i64, Option<MzRange>), BoundingBoxWriter>,
    
    /// Next available bounding box ID
    next_bb_id: i64,
}

impl BoundingBoxCache {
    /// Create a new cache with the given bounding box sizes
    pub fn new(bb_sizes: BBSizes) -> Self {
        Self {
            bb_sizes,
            bounding_boxes: HashMap::new(),
            next_bb_id: 1,
        }
    }
    
    /// Check if it's time to start a new bounding box row
    ///
    /// A new row is needed when the retention time exceeds the BB RT width
    pub fn is_time_for_new_bb_row(
        &self,
        ms_level: i64,
        isolation_window: Option<&MzRange>,
        current_time: f32,
    ) -> bool {
        let first_time = self.find_bb_first_time(ms_level, isolation_window);
        
        if first_time.is_none() {
            return true;
        }
        
        let max_rt_width = if ms_level == 1 {
            self.bb_sizes.bb_rt_width_ms1
        } else {
            self.bb_sizes.bb_rt_width_msn
        };
        
        (current_time - first_time.unwrap()) > max_rt_width
    }
    
    /// Find the first retention time for a BB row
    fn find_bb_first_time(
        &self,
        ms_level: i64,
        isolation_window: Option<&MzRange>,
    ) -> Option<f32> {
        for ((_, cached_iso_win), bb) in &self.bounding_boxes {
            if bb.ms_level == ms_level && cached_iso_win.as_ref() == isolation_window {
                return Some(bb.first_time);
            }
        }
        None
    }
    
    /// Get a cached bounding box
    pub fn get_cached_bb(
        &mut self,
        run_slice_id: i64,
        isolation_window: Option<MzRange>,
    ) -> Option<&mut BoundingBoxWriter> {
        let key = (run_slice_id, isolation_window);
        self.bounding_boxes.get_mut(&key)
    }
    
    /// Create a new bounding box in the cache
    pub fn create_bb(
        &mut self,
        spectrum_time: f32,
        run_slice_id: i64,
        ms_level: i64,
        data_encoding: DataEncoding,
        isolation_window: Option<MzRange>,
        slices_count_hint: usize,
    ) -> &mut BoundingBoxWriter {
        let bb_id = self.next_bb_id;
        self.next_bb_id += 1;
        
        let bb = BoundingBoxWriter {
            id: bb_id,
            first_time: spectrum_time,
            last_time: spectrum_time,
            run_slice_id,
            ms_level,
            data_encoding,
            isolation_window,
            spectrum_ids: Vec::with_capacity(slices_count_hint),
            spectrum_slices: Vec::with_capacity(slices_count_hint),
        };
        
        let key = (run_slice_id, isolation_window);
        self.bounding_boxes.insert(key, bb);
        self.bounding_boxes.get_mut(&key).unwrap()
    }
    
    /// Execute a function for each cached BB matching the criteria
    pub fn for_each_cached_bb<F>(
        &self,
        ms_level: i64,
        isolation_window: Option<&MzRange>,
        mut f: F,
    ) where
        F: FnMut(&BoundingBoxWriter),
    {
        let mut bbs: Vec<_> = self.bounding_boxes
            .values()
            .filter(|bb| {
                bb.ms_level == ms_level && 
                bb.isolation_window.as_ref() == isolation_window
            })
            .collect();
        
        // Sort by run_slice_id for consistent ordering
        bbs.sort_by_key(|bb| bb.run_slice_id);
        
        for bb in bbs {
            f(bb);
        }
    }
    
    /// Remove all bounding boxes for a given MS level and isolation window
    pub fn remove_bb_row(
        &mut self,
        ms_level: i64,
        isolation_window: Option<&MzRange>,
    ) {
        let keys_to_remove: Vec<_> = self.bounding_boxes
            .iter()
            .filter(|(_, bb)| {
                bb.ms_level == ms_level &&
                bb.isolation_window.as_ref() == isolation_window
            })
            .map(|(key, _)| key.clone())
            .collect();
        
        for key in keys_to_remove {
            self.bounding_boxes.remove(&key);
        }
    }
    
    /// Get all unique (ms_level, isolation_window) pairs
    pub fn get_bb_row_keys(&self) -> Vec<(i64, Option<MzRange>)> {
        let mut keys: Vec<_> = self.bounding_boxes
            .values()
            .map(|bb| (bb.ms_level, bb.isolation_window))
            .collect();
        
        keys.sort_by_key(|(ms_level, iso_win_opt)| {
            (*ms_level, iso_win_opt.map(|w| OrderedFloat(w.min_mz)))
        });
        
        keys.into_iter()
            .collect::<std::collections::HashSet<_>>()
            .into_iter()
            .collect()
    }
}

/// Wrapper for f64 that implements Ord for sorting
#[derive(Copy, Clone, Debug, PartialEq, PartialOrd)]
struct OrderedFloat(f64);

impl Eq for OrderedFloat {}

impl Ord for OrderedFloat {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.0.partial_cmp(&other.0).unwrap_or(std::cmp::Ordering::Equal)
    }
}

/// Serialize a bounding box to binary format
///
/// Binary format:
/// - For each spectrum slice:
///   - spectrum_id (4 bytes, i32)
///   - peak_count (4 bytes, i32)
///   - For each peak:
///     - m/z (4 or 8 bytes depending on encoding)
///     - intensity (4 bytes, f32)
///     - [optional] left_hwhm (4 bytes, f32)
///     - [optional] right_hwhm (4 bytes, f32)
pub fn serialize_bounding_box(bb: &BoundingBoxWriter) -> Result<Vec<u8>> {
    let peak_struct_size = bb.data_encoding.get_peak_size();
    let slices_count = bb.spectrum_slices.len();
    
    // Calculate total peaks count
    let total_peaks_count: usize = bb.spectrum_slices
        .iter()
        .filter_map(|s| s.as_ref())
        .map(|s| s.peaks_count())
        .sum();
    
    // Calculate buffer size
    let bb_len = (8 * slices_count) + (peak_struct_size * total_peaks_count);
    let mut buffer = Vec::with_capacity(bb_len);
    
    // Determine if we need to swap bytes based on system endianness
    let swap_bytes = match bb.data_encoding.byte_order {
        ByteOrder::LittleEndian => cfg!(target_endian = "big"),
        ByteOrder::BigEndian => cfg!(target_endian = "little"),
    };
    
    // Serialize each spectrum slice
    for (slice_idx, spectrum_slice_opt) in bb.spectrum_slices.iter().enumerate() {
        let spectrum_id = bb.spectrum_ids[slice_idx];
        
        // Write spectrum ID
        let id_bytes = if swap_bytes {
            (spectrum_id as i32).swap_bytes().to_ne_bytes()
        } else {
            (spectrum_id as i32).to_ne_bytes()
        };
        buffer.extend_from_slice(&id_bytes);
        
        if let Some(spectrum_slice) = spectrum_slice_opt {
            let peaks_count = spectrum_slice.peaks_count();
            
            // Write peaks count
            let count_bytes = if swap_bytes {
                (peaks_count as i32).swap_bytes().to_ne_bytes()
            } else {
                (peaks_count as i32).to_ne_bytes()
            };
            buffer.extend_from_slice(&count_bytes);
            
            // Write peak data
            write_peak_data(
                &mut buffer,
                &spectrum_slice.spectrum_data,
                spectrum_slice.first_peak_idx,
                spectrum_slice.last_peak_idx,
                &bb.data_encoding,
                swap_bytes,
            )?;
        } else {
            // Empty slice - write 0 peaks count
            let zero_bytes = if swap_bytes {
                0i32.swap_bytes().to_ne_bytes()
            } else {
                0i32.to_ne_bytes()
            };
            buffer.extend_from_slice(&zero_bytes);
        }
    }
    
    Ok(buffer)
}

/// Write peak data to buffer
fn write_peak_data(
    buffer: &mut Vec<u8>,
    spectrum_data: &SpectrumData,
    first_idx: usize,
    last_idx: usize,
    encoding: &DataEncoding,
    swap_bytes: bool,
) -> Result<()> {
    for i in first_idx..=last_idx {
        // Write m/z
        match encoding.peak_encoding {
            PeakEncoding::HighRes | PeakEncoding::NoLoss => {
                let mz = spectrum_data.get_mz_at(i)?;
                let mz_bytes = if swap_bytes {
                    mz.swap_bytes().to_ne_bytes()
                } else {
                    mz.to_ne_bytes()
                };
                buffer.extend_from_slice(&mz_bytes);
            }
            PeakEncoding::LowRes => {
                let mz = spectrum_data.get_mz_at(i)? as f32;
                let mz_bytes = if swap_bytes {
                    mz.to_bits().swap_bytes().to_ne_bytes()
                } else {
                    mz.to_ne_bytes()
                };
                buffer.extend_from_slice(&mz_bytes);
            }
        }
        
        // Write intensity
        let intensity = spectrum_data.get_intensity_at(i)?;
        let int_bytes = if swap_bytes {
            intensity.to_bits().swap_bytes().to_ne_bytes()
        } else {
            intensity.to_ne_bytes()
        };
        buffer.extend_from_slice(&int_bytes);
        
        // Write HWHM if fitted mode
        if encoding.mode == DataMode::Fitted {
            let left_hwhm = spectrum_data.get_left_hwhm_at(i).unwrap_or(0.0);
            let right_hwhm = spectrum_data.get_right_hwhm_at(i).unwrap_or(0.0);
            
            let left_bytes = if swap_bytes {
                left_hwhm.to_bits().swap_bytes().to_ne_bytes()
            } else {
                left_hwhm.to_ne_bytes()
            };
            buffer.extend_from_slice(&left_bytes);
            
            let right_bytes = if swap_bytes {
                right_hwhm.to_bits().swap_bytes().to_ne_bytes()
            } else {
                right_hwhm.to_ne_bytes()
            };
            buffer.extend_from_slice(&right_bytes);
        }
    }
    
    Ok(())
}

/// Flush a bounding box row to the database
pub(crate) fn flush_bb_row(
    writer: &mut crate::writer::MzDbWriter,
    ms_level: i64,
    isolation_window: Option<MzRange>,
) -> Result<()> {
    // Collect all spectrum IDs across all BBs in this row
    let mut all_spectrum_ids = Vec::new();
    writer.bb_cache.for_each_cached_bb(
        ms_level,
        isolation_window.as_ref(),
        |bb| {
            all_spectrum_ids.extend(bb.spectrum_ids.iter().copied());
        }
    );
    
    // Get distinct, sorted spectrum IDs
    all_spectrum_ids.sort_unstable();
    all_spectrum_ids.dedup();
    
    // Insert each bounding box
    let conn = writer.connection.as_ref()
        .context("No active connection")?;
    
    // Clone the BBs we need to insert (to avoid borrow issues)
    let bbs_to_insert: Vec<_> = {
        let mut bbs = Vec::new();
        writer.bb_cache.for_each_cached_bb(
            ms_level,
            isolation_window.as_ref(),
            |bb| {
                bbs.push(bb.clone());
            }
        );
        bbs
    };
    
    for mut bb in bbs_to_insert {
        // Build spectrum slice map
        let mut slice_by_id: HashMap<i64, SpectrumSliceIndex> = bb.spectrum_ids
            .iter()
            .zip(bb.spectrum_slices.iter())
            .filter_map(|(id, slice_opt)| {
                slice_opt.as_ref().map(|s| (*id, s.clone()))
            })
            .collect();
        
        // Create complete spectrum slices array with Nones for missing
        let complete_slices: Vec<_> = all_spectrum_ids
            .iter()
            .map(|id| slice_by_id.remove(id))
            .collect();
        
        // Update BB with complete data
        bb.spectrum_ids = all_spectrum_ids.clone();
        bb.spectrum_slices = complete_slices;
        
        // Serialize and insert
        insert_bounding_box(conn, &bb, &writer.run_slice_factory)?;
        
        // Insert R-tree index
        insert_rtree_index(conn, &bb, &writer.run_slice_factory, writer.is_dia)?;
    }
    
    // Remove this BB row from cache
    writer.bb_cache.remove_bb_row(ms_level, isolation_window.as_ref());
    
    Ok(())
}

/// Insert a bounding box into the database
fn insert_bounding_box(
    conn: &rusqlite::Connection,
    bb: &BoundingBoxWriter,
    run_slice_factory: &crate::writer::RunSliceFactory,
) -> Result<i64> {
    let bb_data = serialize_bounding_box(bb)?;
    
    let first_spectrum_id = bb.spectrum_ids.first().copied().unwrap_or(0);
    let last_spectrum_id = bb.spectrum_ids.last().copied().unwrap_or(0);
    
    conn.execute(
        "INSERT INTO bounding_box VALUES (NULL, ?, ?, ?, ?)",
        rusqlite::params![
            &bb_data,
            bb.run_slice_id,
            first_spectrum_id,
            last_spectrum_id,
        ],
    )?;
    
    Ok(conn.last_insert_rowid())
}

/// Insert R-tree index for a bounding box
fn insert_rtree_index(
    conn: &rusqlite::Connection,
    bb: &BoundingBoxWriter,
    run_slice_factory: &crate::writer::RunSliceFactory,
    is_dia: bool,
) -> Result<()> {
    let run_slice = run_slice_factory.get_run_slice(bb.run_slice_id)
        .context("Run slice not found")?;
    
    let bb_id = conn.last_insert_rowid();
    
    if bb.ms_level == 1 {
        // MS1 - use simple R-tree
        conn.execute(
            "INSERT INTO bounding_box_rtree VALUES (?, ?, ?, ?, ?)",
            rusqlite::params![
                bb_id,
                run_slice.begin_mz,
                run_slice.end_mz,
                bb.first_time,
                bb.last_time,
            ],
        )?;
    } else if bb.ms_level == 2 && is_dia {
        // MS2 DIA - use MSn R-tree with isolation window
        if let Some(iso_win) = &bb.isolation_window {
            conn.execute(
                "INSERT INTO bounding_box_msn_rtree VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                rusqlite::params![
                    bb_id,
                    bb.ms_level,
                    bb.ms_level,
                    iso_win.min_mz,
                    iso_win.max_mz,
                    run_slice.begin_mz,
                    run_slice.end_mz,
                    bb.first_time,
                    bb.last_time,
                ],
            )?;
        }
    }
    
    Ok(())
}
