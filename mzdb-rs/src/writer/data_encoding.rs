//! Data Encoding Registry
//!
//! Manages unique data encodings used in the mzDB file and assigns IDs to them.

use anyhow::{Context, Result};
use rusqlite::Connection;
use std::collections::HashMap;

use crate::model::{DataEncoding, DataMode, PeakEncoding, ByteOrder};

/// Registry for tracking unique data encodings
pub struct DataEncodingRegistry {
    /// Map from (mode, peak_encoding) to DataEncoding
    encodings: HashMap<(DataMode, PeakEncoding), DataEncoding>,
    
    /// Next available ID
    next_id: i64,
}

impl DataEncodingRegistry {
    /// Create a new empty registry
    pub fn new() -> Self {
        Self {
            encodings: HashMap::new(),
            next_id: 1,
        }
    }
    
    /// Get or register a data encoding
    ///
    /// If the encoding already exists, returns the existing one.
    /// Otherwise, creates a new encoding with a unique ID.
    pub fn get_or_add(&mut self, encoding: &DataEncoding) -> DataEncoding {
        let key = (encoding.mode, encoding.peak_encoding);
        
        if let Some(existing) = self.encodings.get(&key) {
            existing.clone()
        } else {
            let new_encoding = DataEncoding {
                id: self.next_id,
                mode: encoding.mode,
                peak_encoding: encoding.peak_encoding,
                compression: encoding.compression.clone(),
                byte_order: encoding.byte_order,
            };
            self.next_id += 1;
            self.encodings.insert(key, new_encoding.clone());
            new_encoding
        }
    }
    
    /// Get all distinct data encodings, sorted by ID
    pub fn get_all(&self) -> Vec<&DataEncoding> {
        let mut encodings: Vec<_> = self.encodings.values().collect();
        encodings.sort_by_key(|e| e.id);
        encodings
    }
}

/// Insert all data encodings into the database
pub(crate) fn insert_data_encodings(
    conn: &Connection,
    registry: &DataEncodingRegistry,
) -> Result<()> {
    let mut stmt = conn.prepare(
        "INSERT INTO data_encoding VALUES (?, ?, ?, ?, ?, ?, NULL)"
    ).context("Failed to prepare data_encoding insert statement")?;
    
    for encoding in registry.get_all() {
        let (mz_precision, intensity_precision) = match encoding.peak_encoding {
            PeakEncoding::LowRes => (32, 32),
            PeakEncoding::HighRes => (64, 32),
            PeakEncoding::NoLoss => (64, 64),
        };
        
        let mode_str = match encoding.mode {
            DataMode::Profile => "PROFILE",
            DataMode::Centroid => "CENTROID",
            DataMode::Fitted => "FITTED",
        };
        
        let byte_order_str = match encoding.byte_order {
            ByteOrder::LittleEndian => "little_endian",
            ByteOrder::BigEndian => "big_endian",
        };
        
        stmt.execute(rusqlite::params![
            encoding.id,
            mode_str,
            &encoding.compression,
            byte_order_str,
            mz_precision,
            intensity_precision,
        ]).context("Failed to insert data encoding")?;
    }
    
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_data_encoding_registry() {
        let mut registry = DataEncodingRegistry::new();
        
        let enc1 = DataEncoding {
            id: 0, // ID will be assigned
            mode: DataMode::Centroid,
            peak_encoding: PeakEncoding::HighRes,
            compression: "none".to_string(),
            byte_order: ByteOrder::LittleEndian,
        };
        
        let registered1 = registry.get_or_add(&enc1);
        assert_eq!(registered1.id, 1);
        
        // Same encoding should return same ID
        let registered2 = registry.get_or_add(&enc1);
        assert_eq!(registered2.id, 1);
        
        // Different encoding should get new ID
        let enc2 = DataEncoding {
            id: 0,
            mode: DataMode::Profile,
            peak_encoding: PeakEncoding::HighRes,
            compression: "none".to_string(),
            byte_order: ByteOrder::LittleEndian,
        };
        let registered3 = registry.get_or_add(&enc2);
        assert_eq!(registered3.id, 2);
    }
}
