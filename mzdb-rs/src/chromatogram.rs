//! Chromatogram structures and queries for mzDB files
//!
//! This module provides support for chromatogram data as defined in the mzDB specification.
//! Chromatograms include Total Ion Current (TIC), Selected Ion Monitoring (SIM),
//! Selected Reaction Monitoring (SRM), and other types.
//!
//! # Chromatogram Types
//!
//! - **TIC**: Total Ion Current chromatogram
//! - **SRM/MRM**: Selected/Multiple Reaction Monitoring chromatograms
//! - **BPC**: Base Peak Chromatogram
//! - **XIC**: Extracted Ion Chromatogram
//!
//! # Example
//!
//! ```no_run
//! use mzdb::chromatogram::{list_chromatograms, get_chromatogram_data};
//! use rusqlite::Connection;
//!
//! let db = Connection::open("file.mzDB").unwrap();
//! let chromatograms = list_chromatograms(&db).unwrap();
//! for chrom in chromatograms {
//!     println!("Chromatogram: {} ({})", chrom.name, chrom.activation_type);
//! }
//! ```
#![allow(unused)]

use anyhow_ext::{anyhow, Result};
use rusqlite::{Connection, OptionalExtension};
use serde::{Deserialize, Serialize};

use crate::model::{ByteOrder, DataEncoding, DataMode, PeakEncoding};
use crate::query_utils::get_table_records_count;

// ============================================================================
// Chromatogram structures
// ============================================================================

/// Chromatogram header information from the chromatogram table
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ChromatogramHeader {
    /// Primary key
    pub id: i64,
    /// Unique name for this chromatogram
    pub name: String,
    /// Activation type (e.g., "CID", "HCD", or empty for TIC)
    pub activation_type: String,
    /// Parameter tree as XML string
    pub param_tree: String,
    /// Precursor information as XML (optional, for SRM)
    pub precursor: Option<String>,
    /// Product information as XML (optional, for SRM)
    pub product: Option<String>,
    /// Reference to shared parameter tree
    pub shared_param_tree_id: Option<i64>,
    /// Reference to run
    pub run_id: i64,
    /// Reference to data processing workflow
    pub data_processing_id: Option<i64>,
    /// Reference to data encoding
    pub data_encoding_id: i64,
}

/// Complete chromatogram with data points
#[derive(Clone, Debug, PartialEq)]
pub struct Chromatogram {
    /// Header information
    pub header: ChromatogramHeader,
    /// Chromatogram data points
    pub data: ChromatogramData,
}

/// Chromatogram data points
#[derive(Clone, Debug, PartialEq)]
pub struct ChromatogramData {
    /// Data encoding used for this chromatogram
    pub data_encoding: DataEncoding,
    /// Number of data points
    pub points_count: usize,
    /// Time values (retention time in minutes or seconds)
    pub time_array: Vec<f64>,
    /// Intensity values
    pub intensity_array: Vec<f32>,
}

impl ChromatogramData {
    /// Get the point with maximum intensity
    pub fn get_max_intensity_point(&self) -> Option<(f64, f32)> {
        if self.points_count == 0 {
            return None;
        }
        
        let max_idx = self.intensity_array
            .iter()
            .enumerate()
            .max_by(|(_, a), (_, b)| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal))
            .map(|(idx, _)| idx)?;
        
        Some((self.time_array[max_idx], self.intensity_array[max_idx]))
    }
    
    /// Get the total intensity (area under curve approximation)
    pub fn get_total_intensity(&self) -> f64 {
        self.intensity_array.iter().map(|&i| i as f64).sum()
    }
    
    /// Get points within a time range
    pub fn get_points_in_range(&self, min_time: f64, max_time: f64) -> Vec<(f64, f32)> {
        self.time_array
            .iter()
            .zip(self.intensity_array.iter())
            .filter(|(t, _)| **t >= min_time && **t <= max_time)
            .map(|(&t, &i)| (t, i))
            .collect()
    }
    
    /// Interpolate intensity at a specific time
    pub fn interpolate_at(&self, time: f64) -> Option<f32> {
        if self.points_count < 2 {
            return self.intensity_array.first().copied();
        }
        
        // Find the bracketing indices
        let idx = self.time_array
            .binary_search_by(|&t| t.partial_cmp(&time).unwrap_or(std::cmp::Ordering::Equal))
            .unwrap_or_else(|i| i);
        
        if idx == 0 {
            return Some(self.intensity_array[0]);
        }
        if idx >= self.points_count {
            return Some(self.intensity_array[self.points_count - 1]);
        }
        
        // Linear interpolation
        let t0 = self.time_array[idx - 1];
        let t1 = self.time_array[idx];
        let i0 = self.intensity_array[idx - 1];
        let i1 = self.intensity_array[idx];
        
        let fraction = (time - t0) / (t1 - t0);
        Some(i0 + (i1 - i0) * fraction as f32)
    }
}

/// Chromatogram type based on name pattern
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ChromatogramType {
    /// Total Ion Current
    TIC,
    /// Base Peak Chromatogram
    BPC,
    /// Selected Reaction Monitoring
    SRM,
    /// Selected Ion Monitoring
    SIM,
    /// Extracted Ion Chromatogram
    XIC,
    /// Unknown or custom type
    Other(String),
}

impl ChromatogramHeader {
    /// Infer the chromatogram type from its name
    pub fn chromatogram_type(&self) -> ChromatogramType {
        let name_lower = self.name.to_lowercase();
        if name_lower.contains("tic") || name_lower.contains("total ion") {
            ChromatogramType::TIC
        } else if name_lower.contains("bpc") || name_lower.contains("base peak") {
            ChromatogramType::BPC
        } else if name_lower.contains("srm") || name_lower.contains("mrm") {
            ChromatogramType::SRM
        } else if name_lower.contains("sim") {
            ChromatogramType::SIM
        } else if name_lower.contains("xic") || name_lower.contains("extracted") {
            ChromatogramType::XIC
        } else {
            ChromatogramType::Other(self.name.clone())
        }
    }
    
    /// Check if this is an SRM chromatogram (has precursor and product)
    pub fn is_srm(&self) -> bool {
        self.precursor.is_some() && self.product.is_some()
    }
}

// ============================================================================
// Query functions
// ============================================================================

/// Get all chromatogram headers from the database
pub fn list_chromatograms(db: &Connection) -> Result<Vec<ChromatogramHeader>> {
    let mut stmt = db.prepare(
        "SELECT id, name, activation_type, param_tree, precursor, product, \
         shared_param_tree_id, run_id, data_processing_id, data_encoding_id \
         FROM chromatogram"
    )?;
    
    let chroms = stmt.query_map([], |row| {
        Ok(ChromatogramHeader {
            id: row.get(0)?,
            name: row.get(1)?,
            activation_type: row.get(2)?,
            param_tree: row.get(3)?,
            precursor: row.get(4)?,
            product: row.get(5)?,
            shared_param_tree_id: row.get(6)?,
            run_id: row.get(7)?,
            data_processing_id: row.get(8)?,
            data_encoding_id: row.get(9)?,
        })
    })?;
    
    chroms.collect::<rusqlite::Result<Vec<_>>>().map_err(Into::into)
}

/// Get a specific chromatogram header by ID
pub fn get_chromatogram_header(db: &Connection, id: i64) -> Result<Option<ChromatogramHeader>> {
    let result = db
        .prepare(
            "SELECT id, name, activation_type, param_tree, precursor, product, \
             shared_param_tree_id, run_id, data_processing_id, data_encoding_id \
             FROM chromatogram WHERE id = ?1"
        )?
        .query_row([id], |row| {
            Ok(ChromatogramHeader {
                id: row.get(0)?,
                name: row.get(1)?,
                activation_type: row.get(2)?,
                param_tree: row.get(3)?,
                precursor: row.get(4)?,
                product: row.get(5)?,
                shared_param_tree_id: row.get(6)?,
                run_id: row.get(7)?,
                data_processing_id: row.get(8)?,
                data_encoding_id: row.get(9)?,
            })
        })
        .optional()?;
    Ok(result)
}

/// Get chromatogram header by name
pub fn get_chromatogram_by_name(db: &Connection, name: &str) -> Result<Option<ChromatogramHeader>> {
    let result = db
        .prepare(
            "SELECT id, name, activation_type, param_tree, precursor, product, \
             shared_param_tree_id, run_id, data_processing_id, data_encoding_id \
             FROM chromatogram WHERE name = ?1"
        )?
        .query_row([name], |row| {
            Ok(ChromatogramHeader {
                id: row.get(0)?,
                name: row.get(1)?,
                activation_type: row.get(2)?,
                param_tree: row.get(3)?,
                precursor: row.get(4)?,
                product: row.get(5)?,
                shared_param_tree_id: row.get(6)?,
                run_id: row.get(7)?,
                data_processing_id: row.get(8)?,
                data_encoding_id: row.get(9)?,
            })
        })
        .optional()?;
    Ok(result)
}

/// Get the number of chromatograms
pub fn get_chromatogram_count(db: &Connection) -> Result<i64> {
    Ok(get_table_records_count(db, "chromatogram")?.unwrap_or(0))
}

/// Get the TIC chromatogram if it exists
pub fn get_tic_chromatogram(db: &Connection) -> Result<Option<ChromatogramHeader>> {
    let result = db
        .prepare(
            "SELECT id, name, activation_type, param_tree, precursor, product, \
             shared_param_tree_id, run_id, data_processing_id, data_encoding_id \
             FROM chromatogram WHERE name LIKE '%TIC%' OR name LIKE '%total ion%' LIMIT 1"
        )?
        .query_row([], |row| {
            Ok(ChromatogramHeader {
                id: row.get(0)?,
                name: row.get(1)?,
                activation_type: row.get(2)?,
                param_tree: row.get(3)?,
                precursor: row.get(4)?,
                product: row.get(5)?,
                shared_param_tree_id: row.get(6)?,
                run_id: row.get(7)?,
                data_processing_id: row.get(8)?,
                data_encoding_id: row.get(9)?,
            })
        })
        .optional()?;
    Ok(result)
}

/// Get SRM chromatograms (those with precursor and product information)
pub fn list_srm_chromatograms(db: &Connection) -> Result<Vec<ChromatogramHeader>> {
    let mut stmt = db.prepare(
        "SELECT id, name, activation_type, param_tree, precursor, product, \
         shared_param_tree_id, run_id, data_processing_id, data_encoding_id \
         FROM chromatogram WHERE precursor IS NOT NULL AND product IS NOT NULL"
    )?;
    
    let chroms = stmt.query_map([], |row| {
        Ok(ChromatogramHeader {
            id: row.get(0)?,
            name: row.get(1)?,
            activation_type: row.get(2)?,
            param_tree: row.get(3)?,
            precursor: row.get(4)?,
            product: row.get(5)?,
            shared_param_tree_id: row.get(6)?,
            run_id: row.get(7)?,
            data_processing_id: row.get(8)?,
            data_encoding_id: row.get(9)?,
        })
    })?;
    
    chroms.collect::<rusqlite::Result<Vec<_>>>().map_err(Into::into)
}

// ============================================================================
// Data decoding
// ============================================================================

/// Get data encoding for a chromatogram
fn get_chromatogram_data_encoding(db: &Connection, data_encoding_id: i64) -> Result<DataEncoding> {
    let result = db
        .prepare(
            "SELECT id, mode, compression, byte_order, mz_precision, intensity_precision \
             FROM data_encoding WHERE id = ?1"
        )?
        .query_row([data_encoding_id], |row| {
            let mode_str: String = row.get(1)?;
            let byte_order_str: String = row.get(3)?;
            let mz_precision: u32 = row.get(4)?;
            let intensity_precision: u32 = row.get(5)?;
            
            let mode = match mode_str.as_str() {
                "fitted" => DataMode::Fitted,
                "centroid" => DataMode::Centroid,
                _ => DataMode::Profile,
            };
            
            let byte_order = if byte_order_str == "little_endian" {
                ByteOrder::LittleEndian
            } else {
                ByteOrder::BigEndian
            };
            
            let peak_encoding = if mz_precision == 32 {
                PeakEncoding::LowRes
            } else if intensity_precision == 32 {
                PeakEncoding::HighRes
            } else {
                PeakEncoding::NoLoss
            };
            
            Ok(DataEncoding {
                id: row.get(0)?,
                mode,
                peak_encoding,
                compression: row.get(2)?,
                byte_order,
            })
        })?;
    Ok(result)
}

/// Decode chromatogram data points from blob
fn decode_chromatogram_data(
    blob: &[u8],
    data_encoding: &DataEncoding,
) -> Result<ChromatogramData> {
    let byte_order = data_encoding.byte_order;
    let is_64bit_time = data_encoding.peak_encoding != PeakEncoding::LowRes;
    let is_64bit_intensity = data_encoding.peak_encoding == PeakEncoding::NoLoss;
    
    // Calculate point size
    let time_size = if is_64bit_time { 8 } else { 4 };
    let intensity_size = if is_64bit_intensity { 8 } else { 4 };
    let point_size = time_size + intensity_size;
    
    let points_count = blob.len() / point_size;
    
    let mut time_array = Vec::with_capacity(points_count);
    let mut intensity_array = Vec::with_capacity(points_count);
    
    for i in 0..points_count {
        let offset = i * point_size;
        
        // Read time value
        let time = if is_64bit_time {
            let mut bytes = [0u8; 8];
            bytes.copy_from_slice(&blob[offset..offset + 8]);
            if byte_order == ByteOrder::BigEndian {
                f64::from_be_bytes(bytes)
            } else {
                f64::from_le_bytes(bytes)
            }
        } else {
            let mut bytes = [0u8; 4];
            bytes.copy_from_slice(&blob[offset..offset + 4]);
            if byte_order == ByteOrder::BigEndian {
                f32::from_be_bytes(bytes) as f64
            } else {
                f32::from_le_bytes(bytes) as f64
            }
        };
        
        // Read intensity value
        let intensity = if is_64bit_intensity {
            let int_offset = offset + time_size;
            let mut bytes = [0u8; 8];
            bytes.copy_from_slice(&blob[int_offset..int_offset + 8]);
            if byte_order == ByteOrder::BigEndian {
                f64::from_be_bytes(bytes) as f32
            } else {
                f64::from_le_bytes(bytes) as f32
            }
        } else {
            let int_offset = offset + time_size;
            let mut bytes = [0u8; 4];
            bytes.copy_from_slice(&blob[int_offset..int_offset + 4]);
            if byte_order == ByteOrder::BigEndian {
                f32::from_be_bytes(bytes)
            } else {
                f32::from_le_bytes(bytes)
            }
        };
        
        time_array.push(time);
        intensity_array.push(intensity);
    }
    
    Ok(ChromatogramData {
        data_encoding: data_encoding.clone(),
        points_count,
        time_array,
        intensity_array,
    })
}

/// Get chromatogram data for a specific chromatogram ID
pub fn get_chromatogram_data(db: &Connection, chromatogram_id: i64) -> Result<ChromatogramData> {
    // Get the data encoding ID and blob
    let (data_encoding_id, blob): (i64, Vec<u8>) = db
        .prepare("SELECT data_encoding_id, data_points FROM chromatogram WHERE id = ?1")?
        .query_row([chromatogram_id], |row| {
            Ok((row.get(0)?, row.get(1)?))
        })?;
    
    let data_encoding = get_chromatogram_data_encoding(db, data_encoding_id)?;
    decode_chromatogram_data(&blob, &data_encoding)
}

/// Get a complete chromatogram (header + data) by ID
pub fn get_chromatogram(db: &Connection, chromatogram_id: i64) -> Result<Chromatogram> {
    let header = get_chromatogram_header(db, chromatogram_id)?
        .ok_or_else(|| anyhow!("Chromatogram with ID {} not found", chromatogram_id))?;
    let data = get_chromatogram_data(db, chromatogram_id)?;
    
    Ok(Chromatogram { header, data })
}

/// Iterate over all chromatograms, calling a function for each
pub fn for_each_chromatogram<F>(db: &Connection, mut callback: F) -> Result<()>
where
    F: FnMut(&Chromatogram) -> Result<()>,
{
    let headers = list_chromatograms(db)?;
    
    for header in headers {
        let data = get_chromatogram_data(db, header.id)?;
        let chromatogram = Chromatogram { header, data };
        callback(&chromatogram)?;
    }
    
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_chromatogram_type_inference() {
        let header = ChromatogramHeader {
            id: 1,
            name: "TIC MS1".to_string(),
            activation_type: String::new(),
            param_tree: String::new(),
            precursor: None,
            product: None,
            shared_param_tree_id: None,
            run_id: 1,
            data_processing_id: None,
            data_encoding_id: 1,
        };
        assert_eq!(header.chromatogram_type(), ChromatogramType::TIC);
        
        let header_srm = ChromatogramHeader {
            id: 2,
            name: "SRM Q1=500 Q3=200".to_string(),
            activation_type: "CID".to_string(),
            param_tree: String::new(),
            precursor: Some("<precursor/>".to_string()),
            product: Some("<product/>".to_string()),
            shared_param_tree_id: None,
            run_id: 1,
            data_processing_id: None,
            data_encoding_id: 1,
        };
        assert_eq!(header_srm.chromatogram_type(), ChromatogramType::SRM);
        assert!(header_srm.is_srm());
    }
    
    #[test]
    fn test_chromatogram_data_methods() {
        let data = ChromatogramData {
            data_encoding: DataEncoding {
                id: 1,
                mode: DataMode::Profile,
                peak_encoding: PeakEncoding::HighRes,
                compression: "none".to_string(),
                byte_order: ByteOrder::LittleEndian,
            },
            points_count: 3,
            time_array: vec![1.0, 2.0, 3.0],
            intensity_array: vec![100.0, 200.0, 150.0],
        };
        
        // Test max intensity
        let (time, intensity) = data.get_max_intensity_point().unwrap();
        assert_eq!(time, 2.0);
        assert_eq!(intensity, 200.0);
        
        // Test total intensity
        assert_eq!(data.get_total_intensity(), 450.0);
        
        // Test range query
        let points = data.get_points_in_range(1.5, 2.5);
        assert_eq!(points.len(), 1);
        assert_eq!(points[0], (2.0, 200.0));
        
        // Test interpolation
        let interp = data.interpolate_at(1.5).unwrap();
        assert_eq!(interp, 150.0); // Midpoint between 100 and 200
    }
}
