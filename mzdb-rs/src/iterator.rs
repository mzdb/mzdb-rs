//! Iterator utilities for streaming through mzDB data
//!
//! This module provides efficient iteration over bounding boxes and spectra,
//! allowing processing of large mzDB files without loading everything into memory.
//!
//! # Example
//! ```no_run
//! use mzdb::iterator::for_each_spectrum;
//! use mzdb::cache::create_entity_cache;
//! use rusqlite::Connection;
//!
//! let db = Connection::open("file.mzDB").unwrap();
//! let cache = create_entity_cache(&db).unwrap();
//!
//! for_each_spectrum(&db, &cache, Some(1), |spectrum| {
//!     println!("MS1 spectrum: {}", spectrum.header.id);
//!     Ok(())
//! }).unwrap();
//! ```

use anyhow_ext::{anyhow, Context, Result};
use fallible_iterator::FallibleIterator;
use rusqlite::{Connection, Statement};
use std::collections::HashMap;

use crate::bounding_box::{create_bbox, index_bbox, to_spectrum_slices, read_spectrum_slice_data_at, merge_spectrum_slices};
use crate::model::*;
use crate::queries::*;

const SQL_QUERY_ALL_MS_LEVELS: &str = 
    "SELECT bounding_box.* FROM bounding_box, spectrum \
     WHERE spectrum.id = bounding_box.first_spectrum_id";

pub fn create_bb_iter_stmt_for_all_ms_levels(db: &Connection) -> Result<Statement<'_>> {
    let stmt = db.prepare(SQL_QUERY_ALL_MS_LEVELS).dot()?;
    Ok(stmt)
}

pub fn create_bb_iter_stmt_for_single_ms_level(db: &Connection, ms_level: u8) -> Result<Statement<'_>> {
    let stmt = db
        .prepare(&format!(
            "SELECT bounding_box.* FROM bounding_box, spectrum \
             WHERE spectrum.id = bounding_box.first_spectrum_id AND spectrum.ms_level={}",
            ms_level
        ))
        .dot()?;

    Ok(stmt)
}

fn iterate_bb<'stmt>(
    stmt: &'stmt mut Statement,
) -> Result<impl Iterator<Item = rusqlite::Result<BoundingBox>> + 'stmt> {
    let rows = stmt
        .query_map([], |row| {
            rusqlite::Result::Ok(BoundingBox {
                id: row.get(0)?,
                first_spectrum_id: row.get(3)?,
                last_spectrum_id: row.get(4)?,
                run_slice_id: row.get(2)?,
                blob_data: row.get(1)?,
            })
        })
        .dot()?;

    Ok(rows)
}

pub fn for_each_bb<F>(db: &Connection, ms_level: Option<u8>, mut on_each_bb: F) -> Result<()>
where
    F: FnMut(BoundingBox) -> Result<()>,
{
    let mut bb_iter_stmt = match ms_level {
        None => create_bb_iter_stmt_for_all_ms_levels(db).dot()?,
        Some(level) => create_bb_iter_stmt_for_single_ms_level(db, level).dot()?,
    };

    let bb_iter = iterate_bb(&mut bb_iter_stmt).dot()?;

    for bb_res in bb_iter {
        on_each_bb(bb_res?)?;
    }

    Ok(())
}

pub fn for_each_spectrum<F>(
    db: &Connection,
    entity_cache: &EntityCache,
    ms_level: Option<u8>,
    mut on_each_spectrum: F,
) -> Result<()>
where
    F: FnMut(&Spectrum) -> Result<()>,
{
    let mut bb_row_buffer = Vec::with_capacity(100);
    let mut spectrum_buffer = Vec::with_capacity(100);

    let mut prev_first_spectrum_id: Option<i64> = None;

    for_each_bb(db, ms_level, |bb: BoundingBox| {
        let bb_first_spectrum_header = entity_cache
            .get_spectrum_header(bb.first_spectrum_id)
            .ok_or_else(|| anyhow!("spectrum header not found for ID {}", bb.first_spectrum_id))?;

        let spec_ms_level = bb_first_spectrum_header.ms_level;

        // Process buffer when we encounter a new first_spectrum_id
        let is_new_spectrum = match prev_first_spectrum_id {
            None => false,
            Some(prev_id) => bb.first_spectrum_id != prev_id,
        };

        // the loop will stop if the next ms level is a ms level 1 and if a ms level 1 has already been processed
        // => will collect one ms level 1 and each ms level > 1 (before or after the ms level 1)
        // note: this is required to sort MS1 and MS2 spectra and thus iterate them in the right order
        if is_new_spectrum {
            bb_row_buffer_to_spectrum_buffer(&bb_row_buffer, &mut spectrum_buffer, entity_cache, None)
                .dot()?;
            bb_row_buffer.clear();

            // When encountering MS1, emit all buffered spectra in order
            if spec_ms_level == 1 {
                spectrum_buffer.sort_by(|s1, s2| s1.header.id.cmp(&s2.header.id));

                for s in spectrum_buffer.iter() {
                    on_each_spectrum(s).dot()?;
                }

                spectrum_buffer.clear();
            }
        }

        prev_first_spectrum_id = Some(bb.first_spectrum_id);
        bb_row_buffer.push(bb);

        Ok(())
    })?;

    // Process remaining bounding boxes
    bb_row_buffer_to_spectrum_buffer(&bb_row_buffer, &mut spectrum_buffer, entity_cache, None)
        .dot()?;

    // Emit remaining spectra
    spectrum_buffer.sort_by(|s1, s2| s1.header.id.cmp(&s2.header.id));

    for s in spectrum_buffer.iter() {
        on_each_spectrum(s)?;
    }

    Ok(())
}

/// Convert bounding boxes to spectra
/// 
/// # Arguments
/// * `bb_row_buffer` - Bounding boxes to process (must share the same first_spectrum_id)
/// * `spectrum_buffer` - Output buffer for decoded spectra
/// * `entity_cache` - Entity cache with headers and data encodings
/// * `spectrum_ids` - Optional slice of spectrum IDs to extract. If None, extracts all spectra.
///                    If Some, only extracts spectra whose ID is in this slice.
fn bb_row_buffer_to_spectrum_buffer(
    bb_row_buffer: &[BoundingBox],
    spectrum_buffer: &mut Vec<Spectrum>,
    entity_cache: &EntityCache,
    spectrum_ids: Option<&[i64]>,
) -> Result<()> {
    if bb_row_buffer.is_empty() {
        return Ok(());
    }

    let de_cache = &entity_cache.data_encodings_cache;
    let bb_count = bb_row_buffer.len();

    let indexed_bbs: Vec<_> = bb_row_buffer
        .iter()
        .map(|bb| index_bbox(bb, de_cache))
        .collect();

    let first_bb_index = indexed_bbs[0].as_ref().map_err(|e| anyhow!("{}", e))?;
    let n_spectra = first_bb_index.spectra_ids.len();

    for spectrum_slice_idx in 0..n_spectra {
        let spectrum_id = first_bb_index.spectra_ids[spectrum_slice_idx];
        
        // Skip if not in the requested spectrum IDs
        // Use binary_search for O(log n) lookup (spectrum_ids is sorted)
        if let Some(ids) = spectrum_ids {
            if ids.binary_search(&spectrum_id).is_err() {
                continue;
            }
        }

        let mut spectrum_peak_count = 0;
        let mut spectrum_slices = Vec::with_capacity(bb_count);

        let spectrum_header = entity_cache
            .get_spectrum_header(spectrum_id)
            .ok_or_else(|| anyhow!("spectrum header not found for ID {}", spectrum_id))?;

        let data_encoding = de_cache
            .get_data_encoding_by_spectrum_id(&spectrum_id)
            .ok_or_else(|| anyhow!("can't retrieve data encoding for spectrum ID={}", spectrum_id))?;

        for bb_idx in 0..bb_count {
            let bb = &bb_row_buffer[bb_idx];
            let bb_index = indexed_bbs[bb_idx].as_ref().map_err(|e| anyhow!("{}", e))?;

            let spectrum_slice_data = read_spectrum_slice_data_at(
                bb,
                bb_index,
                data_encoding,
                spectrum_slice_idx,
                None,
                None,
            )
            .dot()?;

            spectrum_peak_count += spectrum_slice_data.peaks_count;
            spectrum_slices.push(spectrum_slice_data);
        }

        let spectrum_data =
            merge_spectrum_slices(&mut spectrum_slices, spectrum_peak_count).dot()?;

        let spectrum = Spectrum {
            header: spectrum_header.clone(),
            data: spectrum_data,
        };

        spectrum_buffer.push(spectrum);
    }

    Ok(())
}

// ============================================================================
// Fallible Iterator API
// ============================================================================

/// Iterator that yields spectra from an mzDB file using fallible_iterator
///
/// This iterator provides a true streaming API that processes spectra on-demand
/// without loading them all into memory at once.
///
/// # Example
/// ```no_run
/// use mzdb::iterator::SpectrumIterator;
/// use mzdb::cache::create_entity_cache;
/// use rusqlite::Connection;
/// use fallible_iterator::FallibleIterator;
///
/// let db = Connection::open("file.mzDB").unwrap();
/// let cache = create_entity_cache(&db).unwrap();
///
/// let mut iter = SpectrumIterator::new(&db, &cache, Some(1)).unwrap();
/// while let Some(spectrum) = iter.next().unwrap() {
///     println!("Spectrum: {}", spectrum.header.id);
/// }
/// ```
pub struct SpectrumIterator<'a> {
    stmt: Statement<'a>,
    entity_cache: &'a EntityCache,
    bb_row_buffer: Vec<BoundingBox>,
    spectrum_buffer: Vec<Spectrum>,
    spectrum_buffer_idx: usize,
    prev_first_spectrum_id: Option<i64>,
    rows: Option<rusqlite::Rows<'a>>,
    finished: bool,
    /// Optional parameter for DIA queries (main_precursor_mz)
    dia_param: Option<f64>,
    /// Pre-computed spectrum IDs matching the DIA filter (sorted for binary search)
    dia_spectrum_ids: Option<Vec<i64>>,
}

impl<'a> SpectrumIterator<'a> {
    /// Create a new spectrum iterator
    ///
    /// # Arguments
    /// * `db` - Database connection
    /// * `entity_cache` - Pre-loaded entity cache
    /// * `ms_level` - Optional MS level filter (e.g., Some(1) for MS1 only, None for all levels)
    pub fn new(
        db: &'a Connection,
        entity_cache: &'a EntityCache,
        ms_level: Option<u8>,
    ) -> Result<Self> {
        let stmt = match ms_level {
            None => create_bb_iter_stmt_for_all_ms_levels(db).dot()?,
            Some(level) => create_bb_iter_stmt_for_single_ms_level(db, level).dot()?,
        };

        Ok(Self {
            stmt,
            entity_cache,
            bb_row_buffer: Vec::with_capacity(100),
            spectrum_buffer: Vec::with_capacity(100),
            spectrum_buffer_idx: 0,
            prev_first_spectrum_id: None,
            rows: None,
            finished: false,
            dia_param: None,
            dia_spectrum_ids: None,
        })
    }

    /// Create a new spectrum iterator for a specific DIA isolation window
    ///
    /// This efficiently iterates over MS2 spectra matching the given `main_precursor_mz`
    /// using tolerance-based filtering. Spectra are yielded one at a time without preloading.
    ///
    /// # Arguments
    /// * `db` - Database connection
    /// * `entity_cache` - Pre-loaded entity cache
    /// * `main_precursor_mz` - The target precursor m/z value for the isolation window
    /// * `precursor_mz_tol` - Optional m/z tolerance in Daltons (default: 0.1)
    ///
    /// # Tolerance
    /// The tolerance parameter allows matching spectra whose precursor m/z is within
    /// ±precursor_mz_tol of the target value. For example:
    /// - precursor_mz_tol = 0.0: Exact match only (may miss spectra due to rounding)
    /// - precursor_mz_tol = 0.1: Match within ±0.1 Da (recommended for rounded window values)
    /// - precursor_mz_tol = 0.5: Match within ±0.5 Da (wider window)
    ///
    /// When using `get_isolation_windows()` which returns rounded values (1 decimal place),
    /// a tolerance of 0.1 is recommended to capture all spectra in the window.
    pub fn new_dia(
        db: &'a Connection,
        entity_cache: &'a EntityCache,
        main_precursor_mz: f64,
        precursor_mz_tol: Option<f64>,
    ) -> Result<Self> {
        let precursor_mz_tol = precursor_mz_tol.unwrap_or(0.1);

        // Pre-compute spectrum IDs matching this isolation window
        // Use tolerance-based matching instead of exact equality
        let mut dia_spectrum_ids: Vec<i64> = entity_cache
            .spectrum_headers
            .iter()
            .filter(|h| {
                h.ms_level == 2 &&
                h.precursor_mz
                    .map(|mz| (mz - main_precursor_mz).abs() <= precursor_mz_tol)
                    .unwrap_or(false)
            })
            .map(|h| h.id)
            .collect();

        // Ensure sorted for efficient binary search during filtering
        dia_spectrum_ids.sort_unstable();

        // Check msn_bb_time_width to determine optimal query strategy
        let msn_bb_time_width = {
            use crate::metadata::{get_mzdb_metadata, parse_msn_bb_time_width};
            get_mzdb_metadata(db)
                .ok()
                .flatten()
                .and_then(|metadata| parse_msn_bb_time_width(&metadata.param_tree))
        };

        // Choose SQL query based on msn_bb_time_width:
        // - If 0: Each BB contains single spectrum, can filter by first_spectrum_id's precursor_mz
        // - If > 0: Multiple spectra per BB, must use run_slice join to filter by ms_level only
        let stmt = if msn_bb_time_width == Some(0.0) {
            // Optimization: BBs contain single spectra, filter by precursor_mz
            // Embed values directly in SQL to avoid storing them in struct
            let min_precursor_mz = main_precursor_mz - precursor_mz_tol;
            let max_precursor_mz = main_precursor_mz + precursor_mz_tol;

            let sql = format!(
                "SELECT bounding_box.* FROM bounding_box \
                 INNER JOIN spectrum ON bounding_box.first_spectrum_id = spectrum.id \
                 WHERE spectrum.ms_level = 2 \
                 AND spectrum.main_precursor_mz BETWEEN {} AND {}",
                min_precursor_mz, max_precursor_mz
            );

            db.prepare(&sql).dot()?
        } else {
            // General case: BBs may contain multiple spectra, filter by ms_level only
            db.prepare(
                "SELECT bounding_box.* FROM bounding_box \
                 INNER JOIN run_slice ON bounding_box.run_slice_id = run_slice.id \
                 WHERE run_slice.ms_level = 2"
            )
            .dot()?
        };

        Ok(Self {
            stmt,
            entity_cache,
            bb_row_buffer: Vec::with_capacity(100),
            spectrum_buffer: Vec::with_capacity(100),
            spectrum_buffer_idx: 0,
            prev_first_spectrum_id: None,
            rows: None,
            finished: false,
            dia_param: Some(main_precursor_mz),
            dia_spectrum_ids: Some(dia_spectrum_ids),
        })
    }

    fn ensure_rows(&mut self) -> Result<()> {
        if self.rows.is_none() {
            // Safety: We need to extend the lifetime of the rows iterator
            // The rows borrow from stmt, and stmt lives as long as self
            // This is safe because:
            // 1. stmt is owned by Self and lives for 'a
            // 2. rows will be dropped when Self is dropped
            // 3. rows will never outlive stmt
            let rows = unsafe {
                // No parameters needed - values are embedded in SQL
                std::mem::transmute::<rusqlite::Rows<'_>, rusqlite::Rows<'a>>(
                    self.stmt.query([]).dot()?
                )
            };
            self.rows = Some(rows);
        }
        Ok(())
    }

    fn read_next_bb(&mut self) -> Result<Option<BoundingBox>> {
        self.ensure_rows()?;

        if let Some(ref mut rows) = self.rows {
            if let Some(row) = rows.next().dot()? {
                return Ok(Some(BoundingBox {
                    id: row.get(0)?,
                    first_spectrum_id: row.get(3)?,
                    last_spectrum_id: row.get(4)?,
                    run_slice_id: row.get(2)?,
                    blob_data: row.get(1)?,
                }));
            }
        }

        Ok(None)
    }

    fn process_bb_buffer(&mut self) -> Result<()> {
        if self.bb_row_buffer.is_empty() {
            return Ok(());
        }

        let mut temp_buffer = Vec::with_capacity(100);

        // For DIA, filter to only the matching spectrum IDs
        let spectrum_ids = self.dia_spectrum_ids.as_deref();

        bb_row_buffer_to_spectrum_buffer(
            &self.bb_row_buffer,
            &mut temp_buffer,
            self.entity_cache,
            spectrum_ids,
        )
        .dot()?;

        self.bb_row_buffer.clear();

        // Only sort for non-DIA iteration (DIA spectra are already in correct order)
        if self.dia_param.is_none() {
            temp_buffer.sort_by(|s1, s2| s1.header.id.cmp(&s2.header.id));
        }

        self.spectrum_buffer.extend(temp_buffer);

        Ok(())
    }

    fn fill_spectrum_buffer(&mut self) -> Result<bool> {
        // Clear previous buffer
        self.spectrum_buffer.clear();
        self.spectrum_buffer_idx = 0;

        // Process bounding boxes until we have spectra to return
        while let Some(bb) = self.read_next_bb()? {
            let bb_first_spectrum_header = self
                .entity_cache
                .get_spectrum_header(bb.first_spectrum_id)
                .ok_or_else(|| anyhow!("spectrum header not found for ID {}", bb.first_spectrum_id))?;

            let spec_ms_level = bb_first_spectrum_header.ms_level;

            let is_new_spectrum = match self.prev_first_spectrum_id {
                None => false,
                Some(prev_id) => bb.first_spectrum_id != prev_id,
            };

            if is_new_spectrum {
                self.process_bb_buffer().dot()?;

                // When encountering MS1, we have collected one cycle
                if spec_ms_level == 1 && !self.spectrum_buffer.is_empty() {
                    self.prev_first_spectrum_id = Some(bb.first_spectrum_id);
                    self.bb_row_buffer.push(bb);
                    return Ok(true);
                }
            }

            self.prev_first_spectrum_id = Some(bb.first_spectrum_id);
            self.bb_row_buffer.push(bb);
        }

        // Process any remaining bounding boxes
        if !self.bb_row_buffer.is_empty() {
            self.process_bb_buffer().dot()?;
        }

        self.finished = true;
        Ok(!self.spectrum_buffer.is_empty())
    }
}

impl<'a> FallibleIterator for SpectrumIterator<'a> {
    type Item = Spectrum;
    type Error = anyhow_ext::Error;

    fn next(&mut self) -> Result<Option<Self::Item>> {
        // Return buffered spectra first
        if self.spectrum_buffer_idx < self.spectrum_buffer.len() {
            let spectrum = self.spectrum_buffer[self.spectrum_buffer_idx].clone();
            self.spectrum_buffer_idx += 1;
            return Ok(Some(spectrum));
        }

        // If we've exhausted the buffer and we're finished, return None
        if self.finished {
            return Ok(None);
        }

        // Fill the buffer with the next batch of spectra
        let has_spectra = self.fill_spectrum_buffer()?;

        if !has_spectra {
            return Ok(None);
        }

        // Return the first spectrum from the newly filled buffer
        if !self.spectrum_buffer.is_empty() {
            let spectrum = self.spectrum_buffer[0].clone();
            self.spectrum_buffer_idx = 1;
            Ok(Some(spectrum))
        } else {
            Ok(None)
        }
    }
}

// ============================================================================
// DIA Spectrum Iterator - efficient iteration over MS2 spectra by isolation window
// ============================================================================

/// Iterate over MS2 spectra for a specific DIA isolation window (by main_precursor_mz)
///
/// This is much more efficient than `for_each_spectrum` with post-filtering because
/// it uses SQL to filter directly on main_precursor_mz, avoiding loading unnecessary data.
///
/// Spectra are streamed one at a time without preloading all into memory.
/// Spectra are returned in retention time order.
///
/// # Arguments
/// * `precursor_mz_tol` - Optional m/z tolerance in Daltons (default: 0.1)
pub fn for_each_dia_spectrum<F>(
    db: &Connection,
    entity_cache: &EntityCache,
    main_precursor_mz: f64,
    precursor_mz_tol: Option<f64>,
    mut on_each_spectrum: F,
) -> Result<()>
where
    F: FnMut(&Spectrum) -> Result<()>,
{
    let mut iter = SpectrumIterator::new_dia(db, entity_cache, main_precursor_mz, precursor_mz_tol)?;

    while let Some(spectrum) = iter.next()? {
        on_each_spectrum(&spectrum)?;
    }

    Ok(())
}

/// Collect all MS2 spectra for a specific DIA isolation window
///
/// This is a convenience function that collects all spectra into a Vec.
/// Spectra are returned sorted by retention time.
///
/// # Arguments
/// * `precursor_mz_tol` - Optional m/z tolerance in Daltons (default: 0.1)
pub fn collect_dia_spectra(
    db: &Connection,
    entity_cache: &EntityCache,
    main_precursor_mz: f64,
    precursor_mz_tol: Option<f64>,
) -> Result<Vec<Spectrum>> {
    use fallible_iterator::FallibleIterator;

    let iter = SpectrumIterator::new_dia(db, entity_cache, main_precursor_mz, precursor_mz_tol)?;
    iter.collect()
}

// ============================================================================
// Run Slice Iterator
// ============================================================================

/// Iterator over RunSlices, grouped by run_slice_id
///
/// This is a port of Java's `AbstractRunSliceIterator` and `LcMsRunSliceIterator`.
/// The iterator loads bounding boxes ordered by run_slice.begin_mz, groups them
/// by run_slice_id, and returns complete RunSlice objects.
///
/// # Java Reference
/// From `fr.profi.mzdb.io.reader.iterator.AbstractRunSliceIterator`:
/// ```java
/// protected void initSpectrumSliceBuffer() {
///     this.spectrumSliceBuffer = this.firstBB.toSpectrumSlices();
///     ArrayList<SpectrumSlice> sl = new ArrayList<SpectrumSlice>(Arrays.asList(this.spectrumSliceBuffer));
///     
///     while (bbHasNext = boundingBoxIterator.hasNext()) {
///         BoundingBox bb = boundingBoxIterator.next();
///         if (bb.getRunSliceId() == this.firstBB.getRunSliceId()) {
///             sl.addAll(Arrays.asList(bb.toSpectrumSlices()));
///         } else {
///             this.firstBB = bb;
///             break;
///         }
///     }
///     this.spectrumSliceBuffer = sl.toArray(new SpectrumSlice[sl.size()]);
/// }
/// ```
#[allow(dead_code)] // Used by ms1_detection in processing module
pub struct RunSliceIterator<'a> {
    /// SQL statement for fetching bounding boxes
    stmt: Statement<'a>,
    /// Entity cache with headers and encodings
    entity_cache: &'a EntityCache,
    /// Rows iterator (initialized lazily)
    rows: Option<rusqlite::Rows<'a>>,
    /// First bounding box of next run slice (lookahead)
    first_bb: Option<BoundingBox>,
    /// Whether there are more bounding boxes
    bb_has_next: bool,
    /// Run slice headers by ID for metadata lookup
    run_slice_headers: HashMap<i64, RunSliceHeader>,
}

impl<'a> RunSliceIterator<'a> {
    /// Create a new RunSliceIterator for MS1 data
    ///
    /// Ported from Java's `LcMsRunSliceIterator` constructor
    ///
    /// # Arguments
    /// * `connection` - Database connection
    /// * `entity_cache` - Pre-loaded entity cache
    ///
    /// # SQL Query
    /// ```sql
    /// SELECT bounding_box.* FROM bounding_box, run_slice
    /// WHERE run_slice.ms_level = 1
    /// AND bounding_box.run_slice_id = run_slice.id
    /// ORDER BY run_slice.begin_mz
    /// ```
    pub fn new(
        connection: &'a Connection,
        entity_cache: &'a EntityCache,
    ) -> Result<Self> {
        Self::new_with_ms_level(connection, entity_cache, 1)
    }

    /// Create a new RunSliceIterator for a specific MS level
    ///
    /// # Arguments
    /// * `connection` - Database connection
    /// * `entity_cache` - Pre-loaded entity cache
    /// * `ms_level` - MS level to iterate (1 for MS1, 2 for MS2, etc.)
    pub fn new_with_ms_level(
        connection: &'a Connection,
        entity_cache: &'a EntityCache,
        ms_level: i64,
    ) -> Result<Self> {
        // Load run slice headers for this MS level
        let run_slice_headers = list_run_slices_by_ms_level(connection, ms_level)?
            .into_iter()
            .map(|header| (header.id, header))
            .collect();

        // Prepare SQL statement (matches Java query)
        let sql = format!(
            "SELECT bounding_box.* FROM bounding_box, run_slice \
             WHERE run_slice.ms_level = {} \
             AND bounding_box.run_slice_id = run_slice.id \
             ORDER BY run_slice.begin_mz",
            ms_level
        );

        let stmt = connection.prepare(&sql)?;

        Ok(Self {
            stmt,
            entity_cache,
            rows: None,
            first_bb: None,
            bb_has_next: true,
            run_slice_headers,
        })
    }

    /// Create a RunSliceIterator for a specific m/z range (MS1 only)
    ///
    /// Ported from Java's `LcMsRunSliceIterator(minRunSliceMz, maxRunSliceMz)` constructor
    ///
    /// # SQL Query
    /// ```sql
    /// SELECT bounding_box.* FROM bounding_box, run_slice
    /// WHERE run_slice.ms_level = 1
    /// AND bounding_box.run_slice_id = run_slice.id
    /// AND run_slice.end_mz >= ?
    /// AND run_slice.begin_mz <= ?
    /// ORDER BY run_slice.begin_mz
    /// ```
    #[allow(dead_code)]
    pub fn new_with_mz_range(
        connection: &'a Connection,
        entity_cache: &'a EntityCache,
        min_run_slice_mz: f64,
        max_run_slice_mz: f64,
    ) -> Result<Self> {
        // Load run slice headers for MS1
        let run_slice_headers = list_run_slices_by_ms_level(connection, 1)?
            .into_iter()
            .map(|header| (header.id, header))
            .collect();

        // Prepare SQL statement with m/z filter (embed params in SQL)
        let sql = format!(
            "SELECT bounding_box.* FROM bounding_box, run_slice \
             WHERE run_slice.ms_level = 1 \
             AND bounding_box.run_slice_id = run_slice.id \
             AND run_slice.end_mz >= {} \
             AND run_slice.begin_mz <= {} \
             ORDER BY run_slice.begin_mz",
            min_run_slice_mz, max_run_slice_mz
        );

        let stmt = connection.prepare(&sql)?;

        Ok(Self {
            stmt,
            entity_cache,
            rows: None,
            first_bb: None,
            bb_has_next: true,
            run_slice_headers,
        })
    }

    /// Ensure rows are initialized (lazy initialization)
    ///
    /// This matches SpectrumIterator's pattern for managing Statement/Rows lifetimes
    fn ensure_rows(&mut self) -> Result<()> {
        if self.rows.is_some() {
            return Ok(());
        }

        // Create Rows from Statement
        // Safety: The rows borrow from stmt, and stmt lives as long as self
        // This is safe because:
        // 1. stmt is owned by Self and lives for 'a
        // 2. rows will be dropped when Self is dropped
        // 3. rows will never outlive stmt
        let rows = unsafe {
            std::mem::transmute::<rusqlite::Rows<'_>, rusqlite::Rows<'a>>(
                self.stmt.query([])?
            )
        };
        self.rows = Some(rows);
        Ok(())
    }

    /// Read next bounding box from rows
    fn read_next_bb(&mut self) -> Result<Option<BoundingBox>> {
        self.ensure_rows()?;

        if let Some(ref mut rows) = self.rows {
            if let Some(row) = rows.next()? {
                return Ok(Some(create_bbox(row)?));
            }
        }

        Ok(None)
    }

    /// Initialize spectrum slice buffer for the current run slice
    ///
    /// This is the core algorithm ported from Java's `initSpectrumSliceBuffer()`.
    /// It groups all bounding boxes with the same run_slice_id and converts them
    /// to spectrum slices.
    ///
    /// # Returns
    /// Vector of SpectrumSlices for the current run slice
    fn init_spectrum_slice_buffer(&mut self) -> Result<Vec<SpectrumSlice>> {
        // Get first BB (either from lookahead or from iterator)
        let first_bb = if let Some(bb) = self.first_bb.take() {
            bb
        } else {
            // Get next BB from iterator
            match self.read_next_bb()? {
                Some(bb) => bb,
                None => {
                    self.bb_has_next = false;
                    return Ok(Vec::new());
                }
            }
        };

        let current_run_slice_id = first_bb.run_slice_id;

        // Index and convert first BB to spectrum slices
        let bbox_index = index_bbox(&first_bb, &self.entity_cache.data_encodings_cache)?;
        let mut spectrum_slices = to_spectrum_slices(
            &first_bb,
            &bbox_index,
            self.entity_cache,
        )?;

        // Collect all BBs with the same run_slice_id
        while let Some(bb) = self.read_next_bb()? {
            if bb.run_slice_id == current_run_slice_id {
                // Same run slice - add its spectrum slices
                let bbox_index = index_bbox(&bb, &self.entity_cache.data_encodings_cache)?;
                let mut bb_slices = to_spectrum_slices(
                    &bb,
                    &bbox_index,
                    self.entity_cache,
                )?;
                spectrum_slices.append(&mut bb_slices);
            } else {
                // Different run slice - save for next iteration
                self.first_bb = Some(bb);
                return Ok(spectrum_slices);
            }
        }

        // No more bounding boxes
        self.bb_has_next = false;
        self.first_bb = None;

        Ok(spectrum_slices)
    }
}

impl<'a> FallibleIterator for RunSliceIterator<'a> {
    type Item = RunSlice;
    type Error = anyhow_ext::Error;

    fn next(&mut self) -> Result<Option<Self::Item>> {
        if !self.bb_has_next && self.first_bb.is_none() {
            return Ok(None);
        }

        // Get spectrum slices for current run slice
        let spectrum_slices = self.init_spectrum_slice_buffer()?;

        if spectrum_slices.is_empty() {
            return Ok(None);
        }

        // Get run_slice_id from first spectrum slice
        let run_slice_id = spectrum_slices[0].run_slice_id;

        // Build RunSliceData
        let run_slice_data = RunSliceData::new(run_slice_id, spectrum_slices);

        // Get RunSliceHeader
        let run_slice_header = self.run_slice_headers
            .get(&run_slice_id)
            .ok_or_else(|| anyhow!("RunSliceHeader not found for id: {}", run_slice_id))?
            .clone();

        // Return complete RunSlice
        Ok(Some(RunSlice {
            header: run_slice_header,
            data: run_slice_data,
        }))
    }
}

/// Convenience function to collect all run slices for MS1
///
/// # Example
/// ```no_run
/// use mzdb::iterator::collect_run_slices;
/// use rusqlite::Connection;
///
/// let db = Connection::open("file.mzDB").unwrap();
/// let cache = mzdb::cache::create_entity_cache(&db).unwrap();
/// let run_slices = collect_run_slices(&db, &cache).unwrap();
/// println!("Found {} run slices", run_slices.len());
/// ```
#[allow(dead_code)] // Public API for library consumers
pub fn collect_run_slices(
    connection: &Connection,
    entity_cache: &EntityCache,
) -> Result<Vec<RunSlice>> {
    use fallible_iterator::FallibleIterator;
    
    let iter = RunSliceIterator::new(connection, entity_cache)?;
    iter.collect()
}