//! Spectrum Writer
//!
//! Handles the insertion of spectrum data into the mzDB file with automatic
//! bounding box assignment and caching.

use anyhow::{Context, Result};

use crate::model::*;
use crate::writer::MzDbWriter;
use crate::writer::bounding_box::SpectrumSliceIndex;

/// Insert a spectrum into the mzDB file
pub(crate) fn insert_spectrum(
    writer: &mut MzDbWriter,
    spectrum: &Spectrum,
    data_encoding: &DataEncoding,
) -> Result<()> {
    let sh = &spectrum.header;
    let sd = &spectrum.data;
    let peaks_count = sd.peaks_count;
    
    // Skip empty spectra
    if peaks_count == 0 {
        return Ok(());
    }
    
    writer.inserted_spectra_count += 1;
    
    let ms_level = sh.ms_level;
    let spectrum_id = writer.inserted_spectra_count;
    let spectrum_time = sh.time;
    
    // Get or register data encoding
    let data_enc = writer.data_encoding_registry.get_or_add(data_encoding);
    
    // Determine isolation window for DIA
    let isolation_window_opt = if writer.is_dia && ms_level == 2 {
        // Extract from precursor list XML
        sh.precursor_list_str
            .as_deref()
            .and_then(|xml| crate::xml::parse_isolation_window_from_xml(xml))
    } else {
        None
    };
    
    // Determine m/z increment based on MS level
    let mz_inc = if ms_level == 1 {
        writer.bb_sizes.bb_mz_height_ms1
    } else {
        writer.bb_sizes.bb_mz_height_msn
    };
    
    // Track first bounding box for this spectrum
    let mut bb_first_spectrum_id = 0i64;
    
    if peaks_count == 0 {
        // Handle empty spectrum (though we already returned above)
        let bb = get_bb_with_next_spectrum_slice(
            writer,
            spectrum,
            spectrum_id,
            spectrum_time,
            ms_level,
            &data_enc,
            isolation_window_opt,
            0,
            0.0,
            mz_inc as f32,
        )?;
        bb_first_spectrum_id = bb.spectrum_ids.first().copied().unwrap_or(0);
    } else {
        // Get first m/z value and round to BB boundary
        let first_mz = sd.get_mz_at(0)?;
        let mut cur_min_mz = ((first_mz / writer.bb_sizes.bb_mz_height_ms1).floor() as i64 as f64)
            * writer.bb_sizes.bb_mz_height_ms1;
        let mut cur_max_mz = cur_min_mz + mz_inc;
        
        // For MS2 non-DIA, use full m/z range
        if ms_level == 2 && !writer.is_dia {
            cur_min_mz = 0.0;
            cur_max_mz = writer.bb_sizes.bb_mz_height_msn;
        }
        
        // Check if we need to flush the current BB row
        let is_time_for_new_bb_row = writer.bb_cache.is_time_for_new_bb_row(
            ms_level,
            isolation_window_opt.as_ref(),
            spectrum_time,
        );
        
        if is_time_for_new_bb_row {
            writer.flush_bb_row(ms_level, isolation_window_opt.clone())?;
        }
        
        // Partition peaks into bounding boxes
        let mut i = 0;
        let mut current_bb = None;
        
        while i < peaks_count as usize {
            let mz = sd.get_mz_at(i)?;
            
            if i == 0 {
                let bb = get_bb_with_next_spectrum_slice(
                    writer,
                    spectrum,
                    spectrum_id,
                    spectrum_time,
                    ms_level,
                    &data_enc,
                    isolation_window_opt.clone(),
                    i,
                    cur_min_mz,
                    cur_max_mz,
                )?;
                bb_first_spectrum_id = bb.spectrum_ids.first().copied().unwrap_or(0);
                current_bb = Some((cur_min_mz, cur_max_mz, bb.spectrum_slices.len() - 1));
            } else if mz > cur_max_mz {
                // Need to move to next bounding box(es)
                while mz > cur_max_mz {
                    cur_min_mz += mz_inc;
                    cur_max_mz += mz_inc;
                    
                    // Ensure run slice exists
                    if !writer.run_slice_factory.has_run_slice(ms_level, cur_min_mz, cur_max_mz) {
                        writer.run_slice_factory.add_run_slice(ms_level, cur_min_mz, cur_max_mz);
                    }
                }
                
                let bb = get_bb_with_next_spectrum_slice(
                    writer,
                    spectrum,
                    spectrum_id,
                    spectrum_time,
                    ms_level,
                    &data_enc,
                    isolation_window_opt.clone(),
                    i,
                    cur_min_mz,
                    cur_max_mz,
                )?;
                current_bb = Some((cur_min_mz, cur_max_mz, bb.spectrum_slices.len() - 1));
            }
            
            // Update last peak index for current slice
            if let Some((min_mz, max_mz, slice_idx)) = current_bb {
                let run_slice_id = writer.run_slice_factory
                    .get_run_slice_id(ms_level, min_mz, max_mz)
                    .context("Run slice not found")?;
                
                if let Some(bb) = writer.bb_cache.get_cached_bb(run_slice_id, isolation_window_opt.clone()) {
                    if let Some(Some(slice)) = bb.spectrum_slices.get_mut(slice_idx) {
                        slice.last_peak_idx = i;
                    }
                }
            }
            
            i += 1;
        }
    }
    
    // Insert spectrum header into tmp_spectrum table
    insert_spectrum_header(
        writer,
        spectrum,
        spectrum_id,
        &data_enc,
        bb_first_spectrum_id,
    )?;
    
    Ok(())
}

/// Get or create a bounding box and add a new spectrum slice
fn get_bb_with_next_spectrum_slice(
    writer: &mut MzDbWriter,
    spectrum: &Spectrum,
    spectrum_id: i64,
    spectrum_time: f32,
    ms_level: i64,
    data_enc: &DataEncoding,
    isolation_window: Option<MzRange>,
    peak_idx: usize,
    min_mz: f64,
    max_mz: f64,
) -> Result<&mut crate::writer::bounding_box::BoundingBoxWriter> {
    let run_slice_id = if let Some(id) = writer.run_slice_factory.get_run_slice_id(ms_level, min_mz, max_mz) {
        id
    } else {
        writer.run_slice_factory.add_run_slice(ms_level, min_mz, max_mz).id
    };
    
    // Try to get existing BB from cache
    let bb_exists = writer.bb_cache.get_cached_bb(run_slice_id, isolation_window.clone()).is_some();
    
    if !bb_exists {
        // Estimate slices count
        let slices_count_hint = if ms_level == 2 {
            1
        } else {
            writer.run_slice_factory.count()
        };
        
        writer.bb_cache.create_bb(
            spectrum_time,
            run_slice_id,
            ms_level,
            data_enc.clone(),
            isolation_window.clone(),
            slices_count_hint,
        );
    }
    
    // Now we can safely get mutable reference
    let bb = writer.bb_cache.get_cached_bb(run_slice_id, isolation_window.clone())
        .context("Failed to get bounding box from cache")?;
    
    // Update last time
    bb.last_time = spectrum_time;
    
    // Add spectrum ID and slice
    bb.spectrum_ids.push(spectrum_id);
    bb.spectrum_slices.push(Some(SpectrumSliceIndex {
        spectrum_data: spectrum.data.clone(),
        first_peak_idx: peak_idx,
        last_peak_idx: peak_idx,
    }));
    
    // Return reference (we need to get it again because we can't return from the block above)
    writer.bb_cache.get_cached_bb(run_slice_id, isolation_window)
        .context("Failed to get bounding box from cache after insertion")
}

/// Insert spectrum header into tmp_spectrum table
fn insert_spectrum_header(
    writer: &mut MzDbWriter,
    spectrum: &Spectrum,
    spectrum_id: i64,
    data_enc: &DataEncoding,
    bb_first_spectrum_id: i64,
) -> Result<()> {
    let sh = &spectrum.header;
    let conn = writer.connection.as_ref()
        .context("No active connection")?;
    
    let activation_type = sh.activation_type.as_deref();
    let precursor_mz = sh.precursor_mz;
    let precursor_charge = sh.precursor_charge;
    
    conn.execute(
        "INSERT INTO tmp_spectrum VALUES (
            ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10,
            ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18, ?19, ?20,
            ?21, ?22, ?23, ?24
        )",
        rusqlite::params![
            spectrum_id,                          // 1: id
            spectrum_id,                          // 2: initial_id (same as id)
            &sh.title,                            // 3: title
            sh.cycle,                             // 4: cycle
            sh.time,                              // 5: time
            sh.ms_level,                          // 6: ms_level
            activation_type,                      // 7: activation_type
            sh.tic,                               // 8: tic
            sh.base_peak_mz,                      // 9: base_peak_mz
            sh.base_peak_intensity,               // 10: base_peak_intensity
            precursor_mz,                         // 11: main_precursor_mz
            precursor_charge,                     // 12: main_precursor_charge
            sh.peaks_count,                       // 13: data_points_count
            sh.param_tree_str.as_deref().unwrap_or(""), // 14: param_tree
            sh.scan_list_str.as_deref().unwrap_or(""),  // 15: scan_list
            sh.precursor_list_str.as_deref(),     // 16: precursor_list
            sh.product_list_str.as_deref(),       // 17: product_list
            1i64,                                 // 18: shared_param_tree_id
            1i64,                                 // 19: instrument_configuration_id
            1i64,                                 // 20: source_file_id
            1i64,                                 // 21: run_id
            1i64,                                 // 22: data_processing_id
            data_enc.id,                          // 23: data_encoding_id
            bb_first_spectrum_id,                 // 24: bb_first_spectrum_id
        ],
    ).context("Failed to insert spectrum header")?;
    
    Ok(())
}
