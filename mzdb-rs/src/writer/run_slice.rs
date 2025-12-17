//! Run Slice Factory
//!
//! Manages creation and tracking of run slices, which partition the m/z dimension
//! of the data for efficient spatial indexing.

use anyhow::{Context, Result};
use rusqlite::Connection;
use std::collections::HashMap;

use crate::model::RunSliceHeader;

/// Factory for creating and managing run slices
pub struct RunSliceFactory {
    /// Map from (ms_level, begin_mz, end_mz) to RunSliceHeader
    run_slices: HashMap<(i64, OrderedFloat, OrderedFloat), RunSliceHeader>,
    
    /// Map from ID to RunSliceHeader
    run_slices_by_id: HashMap<i64, RunSliceHeader>,
    
    /// Run ID these slices belong to
    run_id: i64,
    
    /// Next available run slice ID
    next_id: i64,
}

/// Wrapper for f64 that implements Eq and Hash for use in HashMap keys
#[derive(Copy, Clone, Debug)]
struct OrderedFloat(f64);

impl PartialEq for OrderedFloat {
    fn eq(&self, other: &Self) -> bool {
        self.0.to_bits() == other.0.to_bits()
    }
}

impl Eq for OrderedFloat {}

impl std::hash::Hash for OrderedFloat {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.0.to_bits().hash(state);
    }
}

impl RunSliceFactory {
    /// Create a new factory for the given run ID
    pub fn new(run_id: i64) -> Self {
        Self {
            run_slices: HashMap::new(),
            run_slices_by_id: HashMap::new(),
            run_id,
            next_id: 1,
        }
    }
    
    /// Add a run slice with the given boundaries
    ///
    /// If a run slice with these boundaries already exists, returns the existing one.
    pub fn add_run_slice(
        &mut self,
        ms_level: i64,
        begin_mz: f64,
        end_mz: f64,
    ) -> RunSliceHeader {
        let key = (ms_level, OrderedFloat(begin_mz), OrderedFloat(end_mz));
        
        if let Some(existing) = self.run_slices.get(&key) {
            existing.clone()
        } else {
            let run_slice = RunSliceHeader {
                id: self.next_id,
                ms_level,
                number: 0, // Will be assigned when finalizing
                begin_mz,
                end_mz,
                run_id: self.run_id,
            };
            
            self.run_slices.insert(key, run_slice.clone());
            self.run_slices_by_id.insert(self.next_id, run_slice.clone());
            self.next_id += 1;
            
            run_slice
        }
    }
    
    /// Check if a run slice with these boundaries exists
    pub fn has_run_slice(&self, ms_level: i64, begin_mz: f64, end_mz: f64) -> bool {
        let key = (ms_level, OrderedFloat(begin_mz), OrderedFloat(end_mz));
        self.run_slices.contains_key(&key)
    }
    
    /// Get the ID of a run slice with these boundaries
    pub fn get_run_slice_id(&self, ms_level: i64, begin_mz: f64, end_mz: f64) -> Option<i64> {
        let key = (ms_level, OrderedFloat(begin_mz), OrderedFloat(end_mz));
        self.run_slices.get(&key).map(|rs| rs.id)
    }
    
    /// Get a run slice by ID
    pub fn get_run_slice(&self, id: i64) -> Option<&RunSliceHeader> {
        self.run_slices_by_id.get(&id)
    }
    
    /// Get all run slices, sorted and with assigned numbers
    pub fn get_all(&self) -> Vec<RunSliceHeader> {
        // Group by MS level
        let mut by_ms_level: HashMap<i64, Vec<RunSliceHeader>> = HashMap::new();
        
        for run_slice in self.run_slices.values() {
            by_ms_level.entry(run_slice.ms_level)
                .or_insert_with(Vec::new)
                .push(run_slice.clone());
        }
        
        // Sort each group by ID and assign sequential numbers
        let mut all_slices = Vec::new();
        let mut global_number = 1;
        
        // Sort MS levels
        let mut ms_levels: Vec<_> = by_ms_level.keys().copied().collect();
        ms_levels.sort();
        
        for ms_level in ms_levels {
            if let Some(slices) = by_ms_level.get_mut(&ms_level) {
                slices.sort_by_key(|s| s.id);
                
                for slice in slices {
                    let mut numbered_slice = slice.clone();
                    numbered_slice.number = global_number;
                    all_slices.push(numbered_slice);
                    global_number += 1;
                }
            }
        }
        
        all_slices
    }
    
    /// Get the total number of run slices
    pub fn count(&self) -> usize {
        self.run_slices.len()
    }
}

/// Insert all run slices into the database
pub(crate) fn insert_run_slices(
    conn: &Connection,
    factory: &RunSliceFactory,
) -> Result<()> {
    let mut stmt = conn.prepare(
        "INSERT INTO run_slice VALUES (?, ?, ?, ?, ?, NULL, ?)"
    ).context("Failed to prepare run_slice insert statement")?;
    
    for run_slice in factory.get_all() {
        stmt.execute(rusqlite::params![
            run_slice.id,
            run_slice.ms_level,
            run_slice.number,
            run_slice.begin_mz,
            run_slice.end_mz,
            run_slice.run_id,
        ]).context("Failed to insert run slice")?;
    }
    
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_run_slice_factory() {
        let mut factory = RunSliceFactory::new(1);
        
        let slice1 = factory.add_run_slice(1, 100.0, 110.0);
        assert_eq!(slice1.id, 1);
        assert_eq!(slice1.ms_level, 1);
        assert_eq!(slice1.begin_mz, 100.0);
        
        // Same boundaries should return same slice
        let slice2 = factory.add_run_slice(1, 100.0, 110.0);
        assert_eq!(slice2.id, 1);
        
        // Different boundaries should create new slice
        let slice3 = factory.add_run_slice(1, 110.0, 120.0);
        assert_eq!(slice3.id, 2);
        
        assert_eq!(factory.count(), 2);
    }
    
    #[test]
    fn test_run_slice_numbering() {
        let mut factory = RunSliceFactory::new(1);
        
        factory.add_run_slice(1, 100.0, 110.0);
        factory.add_run_slice(1, 110.0, 120.0);
        factory.add_run_slice(2, 100.0, 10100.0);
        
        let all = factory.get_all();
        assert_eq!(all.len(), 3);
        
        // Numbers should be assigned sequentially
        assert_eq!(all[0].number, 1);
        assert_eq!(all[1].number, 2);
        assert_eq!(all[2].number, 3);
    }
}
