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
//! - Peakel matching across overlapping windows from consecutive cycles
//! - Peakel merging with intensity profile alignment
//!
//! ## Example
//!
//! ```no_run
//! use mzdb::processing::staggered::{StaggeredDiaDetector, StaggeredPeakelConfig};
//! use mzdb::MzDbReader;
//!
//! let reader = MzDbReader::open("staggered_dia.mzDB").unwrap();
//! let detector = StaggeredDiaDetector::new();
//! let stagger_info = detector.detect(&reader).unwrap();
//!
//! if stagger_info.is_staggered {
//!     println!("Staggered DIA detected!");
//!     println!("Window offset: {:.2} Da", stagger_info.window_offset);
//!     println!("Unstaggered windows: {}", stagger_info.unstaggered_windows.len());
//! }
//! ```

use std::collections::{BTreeMap, HashMap};

use anyhow_ext::Result;
use rusqlite::Connection;

use crate::processing::dia::IsolationWindow;
use crate::processing::peakeldb::{ExtendedPeakel, PeakelData};
use crate::processing::model::{HasPeakelData, generate_peakel_id};

// ============================================================================
// Configuration Types
// ============================================================================

/// m/z tolerance specification for peakel matching
#[derive(Debug, Clone)]
pub enum MzTolerance {
    /// Tolerance in Daltons
    Da(f64),
    /// Tolerance in parts per million
    Ppm(f64),
}

impl Default for MzTolerance {
    fn default() -> Self {
        MzTolerance::Ppm(10.0)
    }
}

impl MzTolerance {
    /// Convert to absolute Dalton tolerance for a given m/z value
    pub fn to_da(&self, mz: f64) -> f64 {
        match self {
            MzTolerance::Da(da) => *da,
            MzTolerance::Ppm(ppm) => mz * ppm / 1_000_000.0,
        }
    }
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

/// Strategy for handling peakels with multiple potential matches
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum MultipleMatchStrategy {
    /// Create merged entries for all matching pairs
    MergeAll,
    /// Select the best match based on correlation
    SelectBest,
    /// Create separate entries for each potential pairing
    CreateSeparate,
}

impl Default for MultipleMatchStrategy {
    fn default() -> Self {
        MultipleMatchStrategy::SelectBest
    }
}

/// Configuration for staggered DIA peakel processing
#[derive(Debug, Clone)]
pub struct StaggeredPeakelConfig {
    /// m/z tolerance for matching peakels
    pub mz_tolerance: MzTolerance,
    /// Minimum correlation coefficient for accepting a match
    pub min_correlation: f64,
    /// Minimum RT overlap fraction (0.0-1.0) of the shorter peakel
    pub min_rt_overlap: f64,
    /// Strategy for single observations
    pub single_observation_strategy: SingleObservationStrategy,
    /// Strategy for multiple matches
    pub multiple_match_strategy: MultipleMatchStrategy,
    /// Enable intensity scaling during merge
    pub enable_intensity_scaling: bool,
}

impl Default for StaggeredPeakelConfig {
    fn default() -> Self {
        Self {
            mz_tolerance: MzTolerance::Ppm(10.0),
            min_correlation: 0.7,
            min_rt_overlap: 0.5,
            single_observation_strategy: SingleObservationStrategy::Duplicate,
            multiple_match_strategy: MultipleMatchStrategy::SelectBest,
            enable_intensity_scaling: true,
        }
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

        self.detect_from_windows_with_cycles(&windows_with_cycles)
    }

    /// Detect staggered mode from pre-loaded windows with cycle info
    pub fn detect_from_windows_with_cycles(
        &self,
        windows_with_cycles: &[(IsolationWindow, Vec<i32>)],
    ) -> Result<StaggeredDiaInfo> {
        // Separate windows by cycle parity
        let (odd_windows, even_windows) = self.separate_by_cycle_parity(windows_with_cycles);

        if odd_windows.is_empty() || even_windows.is_empty() {
            log::info!("Only one cycle pattern found - not staggered");
            return Ok(StaggeredDiaInfo::not_staggered());
        }

        // Calculate window width (assume consistent)
        let avg_width = self.average_window_width(windows_with_cycles);

        // Calculate offset between the two window sets
        let offset = self.calculate_offset(&odd_windows, &even_windows);

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
            "Staggered DIA detected: offset={:.2} Da, width={:.2} Da",
            offset, avg_width
        );

        // Generate unstaggered windows
        let unstaggered_windows = self.generate_unstaggered_windows(&odd_windows, &even_windows);

        Ok(StaggeredDiaInfo {
            is_staggered: true,
            window_offset: offset,
            window_width: avg_width,
            cycle_a_windows: odd_windows,
            cycle_b_windows: even_windows,
            unstaggered_windows,
        })
    }

    /// Get isolation windows with their associated cycles
    fn get_windows_with_cycles(
        &self,
        conn: &Connection,
    ) -> Result<Vec<(IsolationWindow, Vec<i32>)>> {
        // Query to get distinct precursor windows and their cycles
        // We don't parse precursor_list (which may be XML or JSON), instead we use
        // a default window width that will be validated against actual data
        let mut stmt = conn.prepare(
            "SELECT DISTINCT 
                main_precursor_mz,
                cycle
             FROM spectrum 
             WHERE ms_level = 2 AND main_precursor_mz IS NOT NULL
             ORDER BY main_precursor_mz, cycle",
        )?;

        // Collect windows grouped by target m/z
        let mut window_map: BTreeMap<i64, (f64, Vec<i32>)> = BTreeMap::new();

        let rows = stmt.query_map([], |row| {
            Ok((
                row.get::<_, f64>(0)?, // main_precursor_mz
                row.get::<_, i32>(1)?, // cycle
            ))
        })?;

        for row in rows {
            let (target_mz, cycle) = row?;
            let key = (target_mz * 100.0).round() as i64; // Use integer key for grouping (0.01 Da precision)

            window_map
                .entry(key)
                .and_modify(|(_, cycles)| {
                    if !cycles.contains(&cycle) {
                        cycles.push(cycle);
                    }
                })
                .or_insert_with(|| {
                    (target_mz, vec![cycle])
                });
        }

        // Calculate window width from spacing between consecutive windows
        let target_mzs: Vec<f64> = window_map.values().map(|(mz, _)| *mz).collect();
        let default_half_width = if target_mzs.len() >= 2 {
            let mut spacings: Vec<f64> = target_mzs.windows(2)
                .map(|w| w[1] - w[0])
                .filter(|&s| s > 0.0 && s < 100.0) // Filter reasonable spacings
                .collect();
            spacings.sort_by(|a, b| a.partial_cmp(b).unwrap());
            if !spacings.is_empty() {
                // Use median spacing as window width, half for offset
                spacings[spacings.len() / 2] / 2.0
            } else {
                12.5 // Default
            }
        } else {
            12.5 // Default
        };

        // Build IsolationWindow objects
        let mut next_id = 1i64;
        let result: Vec<(IsolationWindow, Vec<i32>)> = window_map
            .into_values()
            .map(|(target_mz, cycles)| {
                let window = IsolationWindow {
                    id: next_id,
                    target_mz,
                    lower_mz: target_mz - default_half_width,
                    upper_mz: target_mz + default_half_width,
                    spectrum_count: cycles.len(),
                };
                next_id += 1;
                (window, cycles)
            })
            .collect();

        Ok(result)
    }

    /// Separate windows by cycle parity (odd vs even cycles)
    fn separate_by_cycle_parity(
        &self,
        windows_with_cycles: &[(IsolationWindow, Vec<i32>)],
    ) -> (Vec<IsolationWindow>, Vec<IsolationWindow>) {
        let mut odd_windows = Vec::new();
        let mut even_windows = Vec::new();

        for (window, cycles) in windows_with_cycles {
            // Check which cycle types this window belongs to
            let has_odd = cycles.iter().any(|&c| c % 2 == 1);
            let has_even = cycles.iter().any(|&c| c % 2 == 0);

            // In staggered DIA, each window should belong to only odd OR even cycles
            if has_odd && !has_even {
                odd_windows.push(window.clone());
            } else if has_even && !has_odd {
                even_windows.push(window.clone());
            } else {
                // Window appears in both - might indicate non-staggered or mixed mode
                // For now, assign based on majority
                let odd_count = cycles.iter().filter(|&&c| c % 2 == 1).count();
                let even_count = cycles.iter().filter(|&&c| c % 2 == 0).count();
                if odd_count > even_count {
                    odd_windows.push(window.clone());
                } else {
                    even_windows.push(window.clone());
                }
            }
        }

        // Sort by target m/z
        odd_windows.sort_by(|a, b| a.target_mz.partial_cmp(&b.target_mz).unwrap());
        even_windows.sort_by(|a, b| a.target_mz.partial_cmp(&b.target_mz).unwrap());

        (odd_windows, even_windows)
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
        odd_targets.sort_by(|a, b| a.partial_cmp(b).unwrap());
        even_targets.sort_by(|a, b| a.partial_cmp(b).unwrap());
        
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
        spacings.sort_by(|a, b| a.partial_cmp(b).unwrap());
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
        offsets.sort_by(|a: &f64, b: &f64| a.partial_cmp(b).unwrap());
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
        // Boundary event for sweep line algorithm
        #[derive(Debug, Clone)]
        enum BoundaryEvent {
            CycleAStart(IsolationWindow),
            CycleAEnd,
            CycleBStart(IsolationWindow),
            CycleBEnd,
        }

        // Collect all boundary events
        let mut events: Vec<(f64, BoundaryEvent)> = Vec::new();

        for win in cycle_a {
            events.push((win.lower_mz, BoundaryEvent::CycleAStart(win.clone())));
            events.push((win.upper_mz, BoundaryEvent::CycleAEnd));
        }
        for win in cycle_b {
            events.push((win.lower_mz, BoundaryEvent::CycleBStart(win.clone())));
            events.push((win.upper_mz, BoundaryEvent::CycleBEnd));
        }

        // Sort by m/z position
        events.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap());

        // Sweep line to create non-overlapping windows
        let mut unstaggered = Vec::new();
        let mut current_cycle_a: Option<IsolationWindow> = None;
        let mut current_cycle_b: Option<IsolationWindow> = None;
        let mut last_mz = events.first().map(|(mz, _)| *mz).unwrap_or(0.0);
        let mut next_id = 1i64;

        for (mz, event) in &events {
            // Create window for the segment before this event (if we have coverage)
            if *mz > last_mz + self.boundary_tolerance {
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

            // Update state based on event type
            match event {
                BoundaryEvent::CycleAStart(win) => current_cycle_a = Some(win.clone()),
                BoundaryEvent::CycleAEnd => current_cycle_a = None,
                BoundaryEvent::CycleBStart(win) => current_cycle_b = Some(win.clone()),
                BoundaryEvent::CycleBEnd => current_cycle_b = None,
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

// ============================================================================
// Peakel Matching
// ============================================================================

/// Which cycle type a peakel belongs to
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CycleType {
    /// Odd cycles (1, 3, 5, ...)
    A,
    /// Even cycles (2, 4, 6, ...)
    B,
}

/// A matched pair of peakels from overlapping windows
#[derive(Debug, Clone)]
pub struct MatchedPeakelPair {
    /// Peakel from cycle A window
    pub cycle_a_peakel: ExtendedPeakel,
    /// Peakel from cycle B window
    pub cycle_b_peakel: ExtendedPeakel,
    /// The unstaggered window they map to
    pub target_window_id: i64,
    /// Pearson correlation between intensity profiles
    pub correlation: f64,
}

/// A peakel observed in only one cycle's window
#[derive(Debug, Clone)]
pub struct SingleObservationPeakel {
    /// The peakel
    pub peakel: ExtendedPeakel,
    /// Which cycle type it belongs to
    pub source_cycle: CycleType,
    /// Potential unstaggered windows it could map to
    pub potential_window_ids: Vec<i64>,
}

/// A peakel with multiple potential matches
#[derive(Debug, Clone)]
pub struct AmbiguousMatch {
    /// The primary peakel
    pub primary_peakel: ExtendedPeakel,
    /// All matching candidates from the other cycle
    pub candidates: Vec<ExtendedPeakel>,
    /// Target unstaggered window
    pub target_window_id: i64,
}

/// Result of matching peakels across staggered windows
#[derive(Debug)]
pub struct PeakelMatchResult {
    /// Peakels that matched exactly in overlapping windows
    pub matched_pairs: Vec<MatchedPeakelPair>,
    /// Peakels observed in only one window
    pub single_observations: Vec<SingleObservationPeakel>,
    /// Peakels with multiple potential matches
    pub ambiguous_matches: Vec<AmbiguousMatch>,
}

/// Matcher for peakels across staggered DIA windows
pub struct StaggeredPeakelMatcher {
    config: StaggeredPeakelConfig,
}

impl StaggeredPeakelMatcher {
    /// Create a new matcher with default configuration
    pub fn new() -> Self {
        Self {
            config: StaggeredPeakelConfig::default(),
        }
    }

    /// Create a matcher with custom configuration
    pub fn with_config(config: StaggeredPeakelConfig) -> Self {
        Self { config }
    }

    /// Match peakels across staggered windows
    pub fn match_peakels(
        &self,
        peakels: &[ExtendedPeakel],
        stagger_info: &StaggeredDiaInfo,
    ) -> PeakelMatchResult {
        let mut matched_pairs = Vec::new();
        let mut single_observations = Vec::new();
        let mut ambiguous_matches = Vec::new();

        // Build lookup maps
        let cycle_a_ids: std::collections::HashSet<i64> = stagger_info
            .cycle_a_windows
            .iter()
            .map(|w| w.id)
            .collect();

        // Group peakels by their isolation window
        let mut peakels_by_window: HashMap<i64, Vec<&ExtendedPeakel>> = HashMap::new();
        for peakel in peakels {
            if let Some(window_id) = peakel.isolation_window_id {
                peakels_by_window
                    .entry(window_id)
                    .or_default()
                    .push(peakel);
            }
        }

        // Track which peakels have been matched
        let mut matched_a: std::collections::HashSet<i64> = std::collections::HashSet::new();
        let mut matched_b: std::collections::HashSet<i64> = std::collections::HashSet::new();

        // Process each overlap window
        for window in stagger_info
            .unstaggered_windows
            .iter()
            .filter(|w| w.window_type == UnstaggeredWindowType::Overlap)
        {
            let cycle_a_source_id = match window.cycle_a_source_id {
                Some(id) => id,
                None => continue,
            };
            let cycle_b_source_id = match window.cycle_b_source_id {
                Some(id) => id,
                None => continue,
            };

            let cycle_a_peakels = peakels_by_window
                .get(&cycle_a_source_id)
                .map(|v| v.as_slice())
                .unwrap_or(&[]);
            let cycle_b_peakels = peakels_by_window
                .get(&cycle_b_source_id)
                .map(|v| v.as_slice())
                .unwrap_or(&[]);

            // Find matches
            for peakel_a in cycle_a_peakels {
                // Skip if already matched
                if matched_a.contains(&peakel_a.id) {
                    continue;
                }

                // Skip if m/z is outside this unstaggered window
                if peakel_a.mz < window.lower_mz || peakel_a.mz > window.upper_mz {
                    continue;
                }

                // Find matching peakels from cycle B
                let matches: Vec<_> = cycle_b_peakels
                    .iter()
                    .filter(|peakel_b| {
                        !matched_b.contains(&peakel_b.id)
                            && self.is_matching_peakel(peakel_a, peakel_b)
                    })
                    .cloned()
                    .collect();

                match matches.len() {
                    0 => {
                        // Single observation - will be handled later
                    }
                    1 => {
                        let peakel_b = matches[0];
                        let correlation = self.calculate_correlation(peakel_a, peakel_b);

                        if correlation >= self.config.min_correlation {
                            matched_a.insert(peakel_a.id);
                            matched_b.insert(peakel_b.id);

                            matched_pairs.push(MatchedPeakelPair {
                                cycle_a_peakel: (*peakel_a).clone(),
                                cycle_b_peakel: peakel_b.clone(),
                                target_window_id: window.id,
                                correlation,
                            });
                        }
                    }
                    _ => {
                        ambiguous_matches.push(AmbiguousMatch {
                            primary_peakel: (*peakel_a).clone(),
                            candidates: matches.iter().map(|p| (*p).clone()).collect(),
                            target_window_id: window.id,
                        });
                    }
                }
            }
        }

        // Collect single observations
        for peakel in peakels {
            let window_id = match peakel.isolation_window_id {
                Some(id) => id,
                None => continue,
            };

            let is_cycle_a = cycle_a_ids.contains(&window_id);
            let is_matched = if is_cycle_a {
                matched_a.contains(&peakel.id)
            } else {
                matched_b.contains(&peakel.id)
            };

            if !is_matched {
                // Find potential unstaggered windows
                let potential_windows: Vec<i64> = stagger_info
                    .unstaggered_windows
                    .iter()
                    .filter(|w| w.contains(peakel.mz))
                    .map(|w| w.id)
                    .collect();

                single_observations.push(SingleObservationPeakel {
                    peakel: peakel.clone(),
                    source_cycle: if is_cycle_a { CycleType::A } else { CycleType::B },
                    potential_window_ids: potential_windows,
                });
            }
        }

        PeakelMatchResult {
            matched_pairs,
            single_observations,
            ambiguous_matches,
        }
    }

    /// Check if two peakels from different cycles represent the same analyte
    fn is_matching_peakel(&self, peakel_a: &ExtendedPeakel, peakel_b: &ExtendedPeakel) -> bool {
        // Check m/z match
        let mz_tol = self.config.mz_tolerance.to_da(peakel_a.mz);
        let mz_diff = (peakel_a.mz - peakel_b.mz).abs();
        if mz_diff > mz_tol {
            return false;
        }

        // Check RT overlap
        let a_start = peakel_a.data.min_time();
        let a_end = peakel_a.data.max_time();
        let b_start = peakel_b.data.min_time();
        let b_end = peakel_b.data.max_time();

        let overlap_start = a_start.max(b_start);
        let overlap_end = a_end.min(b_end);

        if overlap_end <= overlap_start {
            return false;
        }

        // Check minimum overlap
        let overlap_duration = overlap_end - overlap_start;
        let min_duration = peakel_a.duration.min(peakel_b.duration);

        overlap_duration >= min_duration * self.config.min_rt_overlap as f32
    }

    /// Calculate Pearson correlation between intensity profiles
    fn calculate_correlation(&self, peakel_a: &ExtendedPeakel, peakel_b: &ExtendedPeakel) -> f64 {
        // Simplified correlation based on apex intensities and duration
        // A more sophisticated implementation would interpolate both profiles
        // to common time points and compute proper Pearson correlation

        let a_intensities = peakel_a.data.intensities();
        let b_intensities = peakel_b.data.intensities();

        if a_intensities.is_empty() || b_intensities.is_empty() {
            return 0.0;
        }

        // Use normalized intensity patterns
        let a_max = a_intensities.iter().cloned().fold(f32::NEG_INFINITY, f32::max);
        let b_max = b_intensities.iter().cloned().fold(f32::NEG_INFINITY, f32::max);

        if a_max <= 0.0 || b_max <= 0.0 {
            return 0.0;
        }

        // Compare apex intensity ratio similarity
        let apex_ratio = (peakel_a.apex_intensity / a_max) / (peakel_b.apex_intensity / b_max);
        let apex_score = 1.0 - (apex_ratio - 1.0).abs().min(1.0) as f64;

        // Compare duration similarity
        let duration_ratio = peakel_a.duration as f64 / peakel_b.duration as f64;
        let duration_score = 1.0 - (duration_ratio - 1.0).abs().min(1.0);

        // Combine scores
        (apex_score + duration_score) / 2.0
    }
}

impl Default for StaggeredPeakelMatcher {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// Peakel Merging
// ============================================================================

/// Statistics about a peakel merge operation
#[derive(Debug, Clone)]
pub struct MergeStats {
    /// Number of points from cycle A
    pub cycle_a_points: usize,
    /// Number of points from cycle B
    pub cycle_b_points: usize,
    /// Correlation coefficient before merging
    pub pre_merge_correlation: f64,
    /// Whether intensity scaling was applied
    pub intensity_scaled: bool,
    /// Scale factor applied (if any)
    pub scale_factor: Option<f64>,
}

/// A merged peakel from staggered DIA unstaggering
#[derive(Debug, Clone)]
pub struct MergedPeakel {
    /// New peakel ID
    pub id: i64,
    /// Weighted average m/z
    pub mz: f64,
    /// Elution time at apex
    pub elution_time: f32,
    /// Duration
    pub duration: f32,
    /// Apex intensity
    pub apex_intensity: f32,
    /// Area under curve
    pub area: f32,
    /// Combined data points
    pub data: PeakelData,
    /// Statistics about the merge
    pub merge_stats: MergeStats,
    /// Target unstaggered window ID
    pub target_window_id: i64,
}

/// Configuration for peakel merging
#[derive(Debug, Clone)]
pub struct MergeConfig {
    /// Enable intensity scaling to align profiles
    pub enable_scaling: bool,
    /// Minimum scale factor allowed
    pub min_scale: f64,
    /// Maximum scale factor allowed
    pub max_scale: f64,
}

impl Default for MergeConfig {
    fn default() -> Self {
        Self {
            enable_scaling: true,
            min_scale: 0.5,
            max_scale: 2.0,
        }
    }
}

/// Merger for peakels from overlapping staggered windows
pub struct PeakelMerger {
    config: MergeConfig,
}

impl PeakelMerger {
    /// Create a new merger with default configuration
    pub fn new() -> Self {
        Self {
            config: MergeConfig::default(),
        }
    }

    /// Create a merger with custom configuration
    pub fn with_config(config: MergeConfig) -> Self {
        Self { config }
    }

    /// Merge two matched peakels from overlapping windows
    pub fn merge_peakels(
        &self,
        matched_pair: &MatchedPeakelPair,
    ) -> Result<MergedPeakel> {
        let peakel_a = &matched_pair.cycle_a_peakel;
        let peakel_b = &matched_pair.cycle_b_peakel;

        // Calculate optional scaling factor
        let (scale_factor, intensity_scaled) = if self.config.enable_scaling {
            self.calculate_scaling(peakel_a, peakel_b)
        } else {
            (None, false)
        };

        // Collect all data points
        let mut all_points: Vec<(i64, f32, f64, f32)> = Vec::new();

        // Add cycle A points
        for i in 0..peakel_a.data.len() {
            all_points.push((
                peakel_a.data.spectrum_ids[i],
                peakel_a.data.elution_times[i],
                peakel_a.data.mz_values[i],
                peakel_a.data.intensities[i],
            ));
        }

        // Add cycle B points (with optional scaling)
        for i in 0..peakel_b.data.len() {
            let intensity = if let Some(factor) = scale_factor {
                peakel_b.data.intensities[i] * factor as f32
            } else {
                peakel_b.data.intensities[i]
            };
            all_points.push((
                peakel_b.data.spectrum_ids[i],
                peakel_b.data.elution_times[i],
                peakel_b.data.mz_values[i],
                intensity,
            ));
        }

        // Sort by elution time
        all_points.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap());

        // Build merged PeakelData
        let merged_data = PeakelData::from_vectors(
            all_points.iter().map(|p| p.0).collect(),
            all_points.iter().map(|p| p.1).collect(),
            all_points.iter().map(|p| p.2).collect(),
            all_points.iter().map(|p| p.3).collect(),
        );

        // Calculate summary statistics
        let merged_mz = merged_data.calc_weighted_mz();
        let apex_intensity = merged_data.apex_intensity().unwrap_or(0.0);
        let elution_time = merged_data.apex_elution_time().unwrap_or(0.0);
        let duration = merged_data.calc_duration();
        let area = merged_data.calc_area();

        Ok(MergedPeakel {
            id: generate_peakel_id(),
            mz: merged_mz,
            elution_time,
            duration,
            apex_intensity,
            area,
            data: merged_data,
            merge_stats: MergeStats {
                cycle_a_points: peakel_a.data.len(),
                cycle_b_points: peakel_b.data.len(),
                pre_merge_correlation: matched_pair.correlation,
                intensity_scaled,
                scale_factor,
            },
            target_window_id: matched_pair.target_window_id,
        })
    }

    /// Calculate scaling factor to align intensity profiles
    fn calculate_scaling(
        &self,
        peakel_a: &ExtendedPeakel,
        peakel_b: &ExtendedPeakel,
    ) -> (Option<f64>, bool) {
        if peakel_b.apex_intensity <= 0.0 || peakel_a.apex_intensity <= 0.0 {
            return (None, false);
        }

        let scale = peakel_a.apex_intensity as f64 / peakel_b.apex_intensity as f64;

        // Clamp scale factor to reasonable range
        if scale < self.config.min_scale || scale > self.config.max_scale {
            return (None, false);
        }

        (Some(scale), true)
    }
}

impl Default for PeakelMerger {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use smallvec::SmallVec;

    fn create_test_isolation_window(id: i64, target_mz: f64, half_width: f64) -> IsolationWindow {
        IsolationWindow {
            id,
            target_mz,
            lower_mz: target_mz - half_width,
            upper_mz: target_mz + half_width,
            spectrum_count: 100,
        }
    }

    fn create_test_peakel_data(
        spectrum_ids: Vec<i64>,
        times: Vec<f32>,
        intensities: Vec<f32>,
    ) -> PeakelData {
        let mz_values: Vec<f64> = vec![500.0; spectrum_ids.len()];
        PeakelData {
            spectrum_ids: SmallVec::from_vec(spectrum_ids),
            elution_times: SmallVec::from_vec(times),
            mz_values: SmallVec::from_vec(mz_values),
            intensities: SmallVec::from_vec(intensities),
        }
    }

    #[test]
    fn test_mz_tolerance_conversion() {
        let da_tol = MzTolerance::Da(0.01);
        assert!((da_tol.to_da(500.0) - 0.01).abs() < 1e-10);

        let ppm_tol = MzTolerance::Ppm(10.0);
        assert!((ppm_tol.to_da(500.0) - 0.005).abs() < 1e-10);
    }

    #[test]
    fn test_unstaggered_window_contains() {
        let window = UnstaggeredWindow {
            id: 1,
            lower_mz: 400.0,
            upper_mz: 412.5,
            center_mz: 406.25,
            cycle_a_source_id: Some(1),
            cycle_b_source_id: None,
            window_type: UnstaggeredWindowType::CycleAOnly,
        };

        assert!(window.contains(405.0));
        assert!(window.contains(400.0));
        assert!(window.contains(412.5));
        assert!(!window.contains(399.9));
        assert!(!window.contains(412.6));
    }

    #[test]
    fn test_generate_unstaggered_windows_overlap() {
        let detector = StaggeredDiaDetector::new();

        // Create staggered window sets
        let cycle_a = vec![
            create_test_isolation_window(1, 412.5, 12.5), // 400-425
            create_test_isolation_window(2, 437.5, 12.5), // 425-450
        ];
        let cycle_b = vec![
            create_test_isolation_window(3, 425.0, 12.5), // 412.5-437.5
        ];

        let unstaggered = detector.generate_unstaggered_windows(&cycle_a, &cycle_b);

        // Expected windows:
        // 400-412.5 (A only)
        // 412.5-425 (overlap A1+B1)
        // 425-437.5 (overlap A2+B1)
        // 437.5-450 (A only)
        assert_eq!(unstaggered.len(), 4);

        // Check overlap windows
        let overlaps: Vec<_> = unstaggered
            .iter()
            .filter(|w| w.window_type == UnstaggeredWindowType::Overlap)
            .collect();
        assert_eq!(overlaps.len(), 2);
    }

    #[test]
    fn test_calculate_offset() {
        let detector = StaggeredDiaDetector::new();

        let set_a = vec![
            create_test_isolation_window(1, 412.5, 12.5),
            create_test_isolation_window(2, 437.5, 12.5),
        ];
        let set_b = vec![
            create_test_isolation_window(3, 425.0, 12.5),
            create_test_isolation_window(4, 450.0, 12.5),
        ];

        let offset = detector.calculate_offset(&set_a, &set_b);
        assert!((offset - 12.5).abs() < 0.1);
    }

    #[test]
    fn test_peakel_matching() {
        let matcher = StaggeredPeakelMatcher::with_config(StaggeredPeakelConfig {
            mz_tolerance: MzTolerance::Da(0.01),
            min_correlation: 0.5,
            min_rt_overlap: 0.3,
            ..Default::default()
        });

        // Create test peakels with overlapping RT
        let data_a = create_test_peakel_data(
            vec![1, 3, 5, 7, 9],
            vec![100.0, 101.0, 102.0, 103.0, 104.0],
            vec![1000.0, 2000.0, 5000.0, 2000.0, 1000.0],
        );
        let peakel_a = ExtendedPeakel::new_ms2_dia(
            1, 500.0, 102.0, 4.0, 0, 5000.0, 11000.0, 5.0, 5,
            1, 5, 9, 1, 500.0, data_a,
        );

        let data_b = create_test_peakel_data(
            vec![2, 4, 6, 8, 10],
            vec![100.5, 101.5, 102.5, 103.5, 104.5],
            vec![1100.0, 2100.0, 5100.0, 2100.0, 1100.0],
        );
        let peakel_b = ExtendedPeakel::new_ms2_dia(
            2, 500.005, 102.5, 4.0, 0, 5100.0, 11500.0, 5.0, 5,
            2, 6, 10, 2, 500.0, data_b,
        );

        assert!(matcher.is_matching_peakel(&peakel_a, &peakel_b));
    }

    #[test]
    fn test_peakel_merging() {
        let merger = PeakelMerger::new();

        let data_a = create_test_peakel_data(
            vec![1, 3, 5],
            vec![100.0, 102.0, 104.0],
            vec![1000.0, 5000.0, 1000.0],
        );
        let peakel_a = ExtendedPeakel::new_ms2_dia(
            1, 500.0, 102.0, 4.0, 0, 5000.0, 7000.0, 5.0, 3,
            1, 3, 5, 1, 500.0, data_a,
        );

        let data_b = create_test_peakel_data(
            vec![2, 4, 6],
            vec![101.0, 103.0, 105.0],
            vec![2000.0, 4000.0, 2000.0],
        );
        let peakel_b = ExtendedPeakel::new_ms2_dia(
            2, 500.001, 103.0, 4.0, 0, 4000.0, 8000.0, 4.0, 3,
            2, 4, 6, 2, 500.0, data_b,
        );

        let matched_pair = MatchedPeakelPair {
            cycle_a_peakel: peakel_a,
            cycle_b_peakel: peakel_b,
            target_window_id: 1,
            correlation: 0.95,
        };

        let merged = merger.merge_peakels(&matched_pair).unwrap();

        // Verify merged data is chronologically ordered
        for i in 1..merged.data.len() {
            assert!(merged.data.elution_times[i] >= merged.data.elution_times[i - 1]);
        }

        // Verify all points are included
        assert_eq!(merged.data.len(), 6);
        assert_eq!(merged.merge_stats.cycle_a_points, 3);
        assert_eq!(merged.merge_stats.cycle_b_points, 3);
    }
}