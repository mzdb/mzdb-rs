//! Staggered DIA Support
//!
//! This module provides support for processing staggered DIA (Data Independent Acquisition)
//! data, where consecutive cycles use different sets of isolation windows that are offset
//! by half their width.
//!
//! ## Key Features
//!
//! - Detection of staggered DIA acquisition mode
//! - Generation of unstaggered (half-width, non-overlapping) isolation windows
//! - Cycle parity separation for staggered window assignment
//!
//! ## Example
//!
//! ```no_run
//! use mzdb::processing::staggered::StaggeredDiaDetector;
//! use mzdb::MzDbReader;
//!
//! let reader = MzDbReader::open("staggered_dia.mzDB").unwrap();
//! let detector = StaggeredDiaDetector::new();
//! let stagger_info = detector.detect(reader.connection()).unwrap();
//!
//! if stagger_info.is_staggered {
//!     println!("Staggered DIA detected!");
//!     println!("Window offset: {:.2} Da", stagger_info.window_offset);
//!     println!("Unstaggered windows: {}", stagger_info.unstaggered_windows.len());
//! }
//! ```

use std::collections::{BTreeMap, HashSet};

use anyhow_ext::Result;
use roxmltree::Document;
use rusqlite::Connection;

use crate::processing::signal::ms2_detection::IsolationWindow;

// ============================================================================
// Helper Functions for Isolation Window Parsing
// ============================================================================

/// Parse isolation window lower and upper offsets from precursor_list XML
/// 
/// Looks for cvParam elements with accessions:
/// - MS:1000828 = "isolation window lower offset"
/// - MS:1000829 = "isolation window upper offset"
fn parse_isolation_window_offsets(xml: &str) -> (Option<f64>, Option<f64>) {
    let mut lower_offset = None;
    let mut upper_offset = None;
    
    // Parse XML using roxmltree
    let doc = match Document::parse(xml) {
        Ok(doc) => doc,
        Err(_) => return (None, None),
    };
    
    // Find all cvParam elements and look for our target accessions
    for node in doc.descendants() {
        if node.tag_name().name() == "cvParam" {
            if let Some(accession) = node.attribute("accession") {
                if let Some(value_str) = node.attribute("value") {
                    match accession {
                        "MS:1000828" => {
                            // isolation window lower offset
                            lower_offset = value_str.parse().ok();
                        }
                        "MS:1000829" => {
                            // isolation window upper offset
                            upper_offset = value_str.parse().ok();
                        }
                        _ => {}
                    }
                }
            }
        }
    }
    
    (lower_offset, upper_offset)
}

/// Calculate fallback half-width from window spacing
/// 
/// In staggered DIA, consecutive windows (across both cycle sets) are spaced 
/// by half the window width. In non-staggered DIA, consecutive windows are 
/// spaced by the full window width.
/// 
/// This function uses the median spacing as a reasonable estimate when
/// actual bounds are not available from the precursor_list XML.
fn calculate_fallback_half_width(target_mzs: &[f64]) -> f64 {
    if target_mzs.len() < 2 {
        log::warn!("Fewer than 2 isolation windows found, using default half-width of 4.0 Da");
        return 4.0; // Conservative default for modern DIA methods
    }
    
    let mut spacings: Vec<f64> = target_mzs.windows(2)
        .map(|w| w[1] - w[0])
        .filter(|&s| s > 0.0 && s < 100.0) // Filter reasonable spacings
        .collect();
    
    if spacings.is_empty() {
        log::warn!("Could not calculate window spacing, using default half-width of 4.0 Da");
        return 4.0;
    }
    
    spacings.sort_by(|a, b| a.total_cmp(b));
    let median_spacing = spacings[spacings.len() / 2];
    
    // In staggered DIA, spacing between consecutive windows equals half-width
    // In non-staggered DIA, spacing equals full width, so half-width = spacing/2
    // We use the spacing directly here since stagger detection will validate
    // whether the offset matches expected values
    median_spacing
}

/// Strategy for handling peakels observed in only one cycle's window
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum SingleObservationStrategy {
    /// Duplicate the peakel to all potential unstaggered windows
    Duplicate,
    /// Remove single-observation peakels entirely
    Remove,
    /// Keep in the original (wider) window
    KeepOriginal,
}

impl Default for SingleObservationStrategy {
    fn default() -> Self {
        SingleObservationStrategy::Duplicate
    }
}

// ============================================================================
// Unstaggered Window Types
// ============================================================================

/// Type of unstaggered window based on cycle coverage
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnstaggeredWindowType {
    /// Window derived from overlap of cycle A and cycle B windows
    Overlap,
    /// Window at the edge, only covered by cycle A (odd cycles)
    CycleAOnly,
    /// Window at the edge, only covered by cycle B (even cycles)
    CycleBOnly,
}

/// An unstaggered (half-width, non-overlapping) isolation window
#[derive(Debug, Clone)]
pub struct UnstaggeredWindow {
    /// Unique identifier
    pub id: i64,
    /// Lower m/z bound
    pub lower_mz: f64,
    /// Upper m/z bound
    pub upper_mz: f64,
    /// Center m/z (for reference)
    pub center_mz: f64,
    /// Source window ID from cycle A that overlaps with this window
    pub cycle_a_source_id: Option<i64>,
    /// Source window ID from cycle B that overlaps with this window
    pub cycle_b_source_id: Option<i64>,
    /// Type of window (overlap, cycle_a_only, cycle_b_only)
    pub window_type: UnstaggeredWindowType,
}

impl UnstaggeredWindow {
    /// Get the width of this window
    pub fn width(&self) -> f64 {
        self.upper_mz - self.lower_mz
    }

    /// Check if a given m/z falls within this window
    pub fn contains(&self, mz: f64) -> bool {
        mz >= self.lower_mz && mz <= self.upper_mz
    }
}

// ============================================================================
// Staggered DIA Detection
// ============================================================================

/// Result of staggered DIA detection
#[derive(Debug, Clone)]
pub struct StaggeredDiaInfo {
    /// Whether staggered mode was detected
    pub is_staggered: bool,
    /// Calculated offset between cycle sets (should be ~half window width)
    pub window_offset: f64,
    /// Average window width
    pub window_width: f64,
    /// Windows belonging to cycle set A (odd cycles)
    pub cycle_a_windows: Vec<IsolationWindow>,
    /// Windows belonging to cycle set B (even cycles)
    pub cycle_b_windows: Vec<IsolationWindow>,
    /// Calculated unstaggered (half-width) windows
    pub unstaggered_windows: Vec<UnstaggeredWindow>,
}

impl StaggeredDiaInfo {
    /// Create info for non-staggered DIA
    pub fn not_staggered() -> Self {
        Self {
            is_staggered: false,
            window_offset: 0.0,
            window_width: 0.0,
            cycle_a_windows: Vec::new(),
            cycle_b_windows: Vec::new(),
            unstaggered_windows: Vec::new(),
        }
    }
}

/// Detector for staggered DIA acquisition mode
pub struct StaggeredDiaDetector {
    /// Tolerance for matching window boundaries
    boundary_tolerance: f64,
    /// Tolerance for offset validation (as fraction of expected offset)
    offset_tolerance: f64,
}

impl StaggeredDiaDetector {
    /// Create a new detector with default tolerances
    pub fn new() -> Self {
        Self {
            boundary_tolerance: 0.1, // 0.1 Da
            offset_tolerance: 0.15,  // 15% of expected offset
        }
    }

    /// Create a detector with custom tolerances
    pub fn with_tolerances(boundary_tolerance: f64, offset_tolerance: f64) -> Self {
        Self {
            boundary_tolerance,
            offset_tolerance,
        }
    }

    /// Detect staggered DIA mode from mzDB connection
    pub fn detect(&self, conn: &Connection) -> Result<StaggeredDiaInfo> {
        // Get unique isolation windows with cycle information
        let windows_with_cycles = self.get_windows_with_cycles(conn)?;

        if windows_with_cycles.is_empty() {
            log::warn!("No isolation windows found");
            return Ok(StaggeredDiaInfo::not_staggered());
        }

        // Separate windows into two series using scan order (m/z drop detection)
        let (cycle_a_windows, cycle_b_windows) =
            self.separate_by_scan_order(conn, &windows_with_cycles);

        self.build_stagger_info(&windows_with_cycles, cycle_a_windows, cycle_b_windows)
    }

    /// Core stagger info construction shared by both detect paths.
    fn build_stagger_info(
        &self,
        windows_with_cycles: &[(IsolationWindow, Vec<i32>)],
        cycle_a_windows: Vec<IsolationWindow>,
        cycle_b_windows: Vec<IsolationWindow>,
    ) -> Result<StaggeredDiaInfo> {
        if cycle_a_windows.is_empty() || cycle_b_windows.is_empty() {
            log::info!("Only one cycle pattern found - not staggered");
            return Ok(StaggeredDiaInfo::not_staggered());
        }

        // Calculate window width (assume consistent)
        let avg_width = self.average_window_width(windows_with_cycles);

        // Calculate offset between the two window sets
        let offset = self.calculate_offset(&cycle_a_windows, &cycle_b_windows);

        // Verify staggered pattern: offset should be approximately half the width
        let expected_offset = avg_width / 2.0;
        let offset_diff = (offset - expected_offset).abs();
        let is_staggered = offset_diff <= expected_offset * self.offset_tolerance;

        if !is_staggered {
            log::info!(
                "Window offset {:.2} Da doesn't match expected {:.2} Da (±{:.0}%) - not staggered",
                offset, expected_offset, self.offset_tolerance * 100.0
            );
            return Ok(StaggeredDiaInfo::not_staggered());
        }

        log::info!(
            "Staggered DIA detected: offset={:.2} Da, width={:.2} Da, series A={} windows, series B={} windows",
            offset, avg_width, cycle_a_windows.len(), cycle_b_windows.len()
        );

        // Generate unstaggered windows
        let unstaggered_windows = self.generate_unstaggered_windows(&cycle_a_windows, &cycle_b_windows);

        Ok(StaggeredDiaInfo {
            is_staggered: true,
            window_offset: offset,
            window_width: avg_width,
            cycle_a_windows,
            cycle_b_windows,
            unstaggered_windows,
        })
    }

    /// Get isolation windows with their associated cycles
    fn get_windows_with_cycles(
        &self,
        conn: &Connection,
    ) -> Result<Vec<(IsolationWindow, Vec<i32>)>> {
        // Query to get precursor windows with their actual bounds from precursor_list XML
        let mut stmt = conn.prepare(
            "SELECT DISTINCT 
                main_precursor_mz,
                cycle,
                precursor_list
             FROM spectrum 
             WHERE ms_level = 2 AND main_precursor_mz IS NOT NULL
             ORDER BY main_precursor_mz, cycle",
        )?;

        // Collect windows grouped by target m/z, with actual bounds
        // Key: integer key for grouping (0.01 Da precision)
        // Value: (target_mz, lower_offset, upper_offset, cycles)
        let mut window_map: BTreeMap<i64, (f64, Option<f64>, Option<f64>, Vec<i32>)> = BTreeMap::new();

        let rows = stmt.query_map([], |row| {
            Ok((
                row.get::<_, f64>(0)?,           // main_precursor_mz
                row.get::<_, i32>(1)?,           // cycle
                row.get::<_, Option<String>>(2)?, // precursor_list (XML)
            ))
        })?;

        for row in rows {
            let (target_mz, cycle, precursor_list) = row?;
            let key = (target_mz * 100.0).round() as i64;

            // Parse isolation window bounds from XML if available
            let (lower_offset, upper_offset) = precursor_list
                .as_ref()
                .map(|xml| parse_isolation_window_offsets(xml))
                .unwrap_or((None, None));

            window_map
                .entry(key)
                .and_modify(|(_, existing_lower, existing_upper, cycles)| {
                    if !cycles.contains(&cycle) {
                        cycles.push(cycle);
                    }
                    // Update offsets if we found them and don't have them yet
                    if existing_lower.is_none() && lower_offset.is_some() {
                        *existing_lower = lower_offset;
                    }
                    if existing_upper.is_none() && upper_offset.is_some() {
                        *existing_upper = upper_offset;
                    }
                })
                .or_insert_with(|| {
                    (target_mz, lower_offset, upper_offset, vec![cycle])
                });
        }

        // Calculate fallback half-width from spacing (for cases where XML parsing fails)
        let target_mzs: Vec<f64> = window_map.values().map(|(mz, _, _, _)| *mz).collect();
        let fallback_half_width = calculate_fallback_half_width(&target_mzs);

        // Check if we have actual bounds from XML
        let has_xml_bounds = window_map.values()
            .any(|(_, lower, upper, _)| lower.is_some() && upper.is_some());
        
        if has_xml_bounds {
            log::debug!("Using isolation window bounds from precursor_list XML");
        } else {
            log::debug!("No XML bounds found, using calculated half-width: {:.2} Da", fallback_half_width);
        }

        // Build IsolationWindow objects
        let mut next_id = 1i64;
        let result: Vec<(IsolationWindow, Vec<i32>)> = window_map
            .into_values()
            .map(|(target_mz, lower_offset, upper_offset, cycles)| {
                // Use actual bounds from XML, or fall back to calculated spacing
                let half_width_lower = lower_offset.unwrap_or(fallback_half_width);
                let half_width_upper = upper_offset.unwrap_or(fallback_half_width);
                
                let window = IsolationWindow {
                    id: next_id,
                    target_mz,
                    lower_mz: target_mz - half_width_lower,
                    upper_mz: target_mz + half_width_upper,
                    spectrum_count: cycles.len(),
                };
                next_id += 1;
                (window, cycles)
            })
            .collect();

        Ok(result)
    }

    /// Separate windows into two staggered series by detecting DIA cycle boundaries
    /// from the actual scan order.
    ///
    /// A new DIA cycle starts whenever the precursor m/z drops below the previous
    /// MS2 spectrum's value. The first two distinct window sets found define
    /// series A and B. Each window is assigned to the series it appears in.
    fn separate_by_scan_order(
        &self,
        conn: &Connection,
        windows_with_cycles: &[(IsolationWindow, Vec<i32>)],
    ) -> (Vec<IsolationWindow>, Vec<IsolationWindow>) {
        // Query MS2 spectra in scan order to detect cycle boundaries
        let mut stmt = conn.prepare(
            "SELECT main_precursor_mz FROM spectrum \
             WHERE ms_level = 2 AND main_precursor_mz IS NOT NULL \
             ORDER BY id"
        ).unwrap();

        let precursor_mzs: Vec<f64> = stmt
            .query_map([], |row| row.get::<_, f64>(0))
            .unwrap()
            .filter_map(|r| r.ok())
            .collect();

        if precursor_mzs.is_empty() {
            return (Vec::new(), Vec::new());
        }

        // Detect cycle boundaries: m/z drops indicate new cycle start
        // Collect the first two distinct cycle patterns
        let mut pattern_a: Option<Vec<i64>> = None;
        let mut pattern_b: Option<Vec<i64>> = None;
        let mut current_cycle_keys: Vec<i64> = Vec::new();
        let mut prev_mz = 0.0f64;

        for &mz in &precursor_mzs {
            if mz < prev_mz && !current_cycle_keys.is_empty() {
                // Cycle boundary — identify this cycle's pattern
                let key_set: Vec<i64> = current_cycle_keys.clone();

                match (&pattern_a, &pattern_b) {
                    (None, _) => {
                        pattern_a = Some(key_set);
                    }
                    (Some(pa), None) => {
                        if key_set != *pa {
                            pattern_b = Some(key_set);
                            break; // We have both patterns
                        }
                    }
                    _ => break,
                }
                current_cycle_keys.clear();
            }
            current_cycle_keys.push((mz * 100.0).round() as i64);
            prev_mz = mz;
        }

        // Handle the last cycle if we still need pattern_b
        if pattern_b.is_none() && !current_cycle_keys.is_empty() {
            if let Some(pa) = &pattern_a {
                if current_cycle_keys != *pa {
                    pattern_b = Some(current_cycle_keys);
                }
            }
        }

        let (pat_a, pat_b) = match (pattern_a, pattern_b) {
            (Some(a), Some(b)) => (a, b),
            _ => {
                log::warn!("Could not detect two distinct cycle patterns from scan order");
                return (Vec::new(), Vec::new());
            }
        };

        // Convert patterns to HashSets for O(1) lookup
        let set_a: HashSet<i64> = pat_a.iter().copied().collect();
        let set_b: HashSet<i64> = pat_b.iter().copied().collect();

        log::info!(
            "Cycle patterns from scan order: series A = {} windows, series B = {} windows",
            set_a.len(), set_b.len()
        );

        // Assign each window to its series
        let mut cycle_a_windows = Vec::new();
        let mut cycle_b_windows = Vec::new();

        for (window, _) in windows_with_cycles {
            let key = (window.target_mz * 100.0).round() as i64;
            if set_a.contains(&key) {
                cycle_a_windows.push(window.clone());
            } else if set_b.contains(&key) {
                cycle_b_windows.push(window.clone());
            } else {
                // Window not in either pattern — assign to the closest series by m/z
                log::warn!(
                    "Window {:.2} not found in either cycle pattern, skipping",
                    window.target_mz
                );
            }
        }

        // Sort by target m/z
        cycle_a_windows.sort_by(|a, b| a.target_mz.total_cmp(&b.target_mz));
        cycle_b_windows.sort_by(|a, b| a.target_mz.total_cmp(&b.target_mz));

        (cycle_a_windows, cycle_b_windows)
    }

    /// Calculate average window width by looking at spacing within same-parity windows
    fn average_window_width(&self, windows: &[(IsolationWindow, Vec<i32>)]) -> f64 {
        if windows.len() < 2 {
            return 0.0;
        }
        
        // Separate into odd and even cycle windows
        let mut odd_targets: Vec<f64> = Vec::new();
        let mut even_targets: Vec<f64> = Vec::new();
        
        for (window, cycles) in windows {
            let has_odd = cycles.iter().any(|&c| c % 2 == 1);
            let has_even = cycles.iter().any(|&c| c % 2 == 0);
            
            if has_odd && !has_even {
                odd_targets.push(window.target_mz);
            } else if has_even && !has_odd {
                even_targets.push(window.target_mz);
            }
        }
        
        // Sort
        odd_targets.sort_by(|a, b| a.total_cmp(b));
        even_targets.sort_by(|a, b| a.total_cmp(b));
        
        // Calculate spacing within each set (this should be the actual window width)
        let mut spacings = Vec::new();
        
        for targets in [&odd_targets, &even_targets] {
            for i in 1..targets.len() {
                let spacing = targets[i] - targets[i - 1];
                if spacing > 0.0 && spacing < 100.0 {
                    spacings.push(spacing);
                }
            }
        }
        
        if spacings.is_empty() {
            // Fall back to old calculation
            let total: f64 = windows
                .iter()
                .map(|(w, _)| w.upper_mz - w.lower_mz)
                .sum();
            return total / windows.len() as f64;
        }
        
        // Return median spacing as window width
        spacings.sort_by(|a, b| a.total_cmp(b));
        spacings[spacings.len() / 2]
    }

    /// Calculate the offset between two window sets
    fn calculate_offset(&self, set_a: &[IsolationWindow], set_b: &[IsolationWindow]) -> f64 {
        if set_a.is_empty() || set_b.is_empty() {
            log::debug!("calculate_offset: one set is empty (a={}, b={})", set_a.len(), set_b.len());
            return 0.0;
        }

        log::debug!("calculate_offset: set_a has {} windows, set_b has {} windows", set_a.len(), set_b.len());
        
        // Log sample windows
        if let Some(win) = set_a.first() {
            log::debug!("  set_a first: target={:.2}, bounds={:.2}-{:.2}", win.target_mz, win.lower_mz, win.upper_mz);
        }
        if let Some(win) = set_b.first() {
            log::debug!("  set_b first: target={:.2}, bounds={:.2}-{:.2}", win.target_mz, win.lower_mz, win.upper_mz);
        }

        // Find pairs of windows that are adjacent in m/z space
        let mut offsets = Vec::new();

        for win_a in set_a {
            for win_b in set_b {
                let offset = (win_a.target_mz - win_b.target_mz).abs();
                // Only consider windows that are close (potential staggered pairs)
                let avg_width = (win_a.upper_mz - win_a.lower_mz + win_b.upper_mz - win_b.lower_mz) / 2.0;
                // Use <= and add small tolerance to handle edge cases
                let threshold = avg_width * 1.1;
                if offset <= threshold {
                    offsets.push(offset);
                }
            }
        }
        
        // Debug: log first comparison
        if let (Some(win_a), Some(win_b)) = (set_a.first(), set_b.first()) {
            let offset = (win_a.target_mz - win_b.target_mz).abs();
            let avg_width = (win_a.upper_mz - win_a.lower_mz + win_b.upper_mz - win_b.lower_mz) / 2.0;
            let threshold = avg_width * 1.1;
            log::debug!("  First pair: offset={:.4}, avg_width={:.4}, threshold={:.4}, passes={}", 
                offset, avg_width, threshold, offset <= threshold);
        }

        log::debug!("calculate_offset: found {} offset pairs", offsets.len());
        
        if offsets.is_empty() {
            return 0.0;
        }

        // Return median offset (more robust than mean)
        offsets.sort_by(|a: &f64, b: &f64| a.total_cmp(b));
        let median = offsets[offsets.len() / 2];
        log::debug!("calculate_offset: median offset = {:.2}", median);
        median
    }

    /// Generate unstaggered (half-width) windows from overlapping regions
    fn generate_unstaggered_windows(
        &self,
        cycle_a: &[IsolationWindow],
        cycle_b: &[IsolationWindow],
    ) -> Vec<UnstaggeredWindow> {
        // Boundary event for sweep line algorithm.
        // End events carry the window id they belong to, so we only clear state
        // when the *current* window ends (not a stale one from a same-series overlap).
        #[derive(Debug, Clone)]
        enum BoundaryEvent {
            CycleAStart(IsolationWindow),
            CycleAEnd(i64), // id of the window ending
            CycleBStart(IsolationWindow),
            CycleBEnd(i64), // id of the window ending
        }

        // Collect all boundary events
        let mut events: Vec<(f64, BoundaryEvent)> = Vec::new();

        for win in cycle_a {
            events.push((win.lower_mz, BoundaryEvent::CycleAStart(win.clone())));
            events.push((win.upper_mz, BoundaryEvent::CycleAEnd(win.id)));
        }
        for win in cycle_b {
            events.push((win.lower_mz, BoundaryEvent::CycleBStart(win.clone())));
            events.push((win.upper_mz, BoundaryEvent::CycleBEnd(win.id)));
        }

        // Sort by m/z position. For events at the same m/z, process End before Start
        // to avoid transient states where both old and new windows appear active.
        events.sort_by(|a, b| {
            a.0.total_cmp(&b.0).then_with(|| {
                let order = |e: &BoundaryEvent| -> u8 {
                    match e {
                        BoundaryEvent::CycleAEnd(_) | BoundaryEvent::CycleBEnd(_) => 0,
                        BoundaryEvent::CycleAStart(_) | BoundaryEvent::CycleBStart(_) => 1,
                    }
                };
                order(&a.1).cmp(&order(&b.1))
            })
        });

        // Sweep line to create non-overlapping windows
        let mut unstaggered = Vec::new();
        let mut current_cycle_a: Option<IsolationWindow> = None;
        let mut current_cycle_b: Option<IsolationWindow> = None;
        let mut last_mz = events.first().map(|(mz, _)| *mz).unwrap_or(0.0);
        let mut next_id = 1i64;

        // Minimum window width to avoid creating tiny segments from floating-point issues
        // Use boundary_tolerance as the minimum meaningful segment width
        let min_window_width = self.boundary_tolerance.max(0.1); // At least 0.1 Da

        for (mz, event) in &events {
            // Create window for the segment before this event (if we have coverage)
            let segment_width = *mz - last_mz;
            if segment_width > min_window_width {
                if current_cycle_a.is_some() || current_cycle_b.is_some() {
                    let window_type = match (&current_cycle_a, &current_cycle_b) {
                        (Some(_), Some(_)) => UnstaggeredWindowType::Overlap,
                        (Some(_), None) => UnstaggeredWindowType::CycleAOnly,
                        (None, Some(_)) => UnstaggeredWindowType::CycleBOnly,
                        (None, None) => continue,
                    };

                    unstaggered.push(UnstaggeredWindow {
                        id: next_id,
                        lower_mz: last_mz,
                        upper_mz: *mz,
                        center_mz: (last_mz + *mz) / 2.0,
                        cycle_a_source_id: current_cycle_a.as_ref().map(|w| w.id),
                        cycle_b_source_id: current_cycle_b.as_ref().map(|w| w.id),
                        window_type,
                    });
                    next_id += 1;
                }
            }

            // Update state based on event type.
            // End events only clear state if they match the currently active window,
            // preventing a stale End from clearing a newer same-series window.
            match event {
                BoundaryEvent::CycleAStart(win) => current_cycle_a = Some(win.clone()),
                BoundaryEvent::CycleAEnd(id) => {
                    if current_cycle_a.as_ref().is_some_and(|w| w.id == *id) {
                        current_cycle_a = None;
                    }
                }
                BoundaryEvent::CycleBStart(win) => current_cycle_b = Some(win.clone()),
                BoundaryEvent::CycleBEnd(id) => {
                    if current_cycle_b.as_ref().is_some_and(|w| w.id == *id) {
                        current_cycle_b = None;
                    }
                }
            }

            last_mz = *mz;
        }

        log::info!("Generated {} unstaggered windows", unstaggered.len());
        unstaggered
    }
}

impl Default for StaggeredDiaDetector {
    fn default() -> Self {
        Self::new()
    }
}
