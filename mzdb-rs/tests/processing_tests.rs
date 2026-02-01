//! Integration tests for the processing module with mzdb-rs
//!
//! These tests verify that the signal processing algorithms work correctly
//! when applied to real mass spectrometry data from mzDB files.
//!
//! Run with: cargo test --features processing --test processing_integration_tests

#![cfg(feature = "processing")]

use mzdb::MzDbReader;
use mzdb::processing::model::{Peak, Peakel, PeakelBuilder, LcContext, HasPeakelData};
use mzdb::processing::signal::finder::{BasicPeakelFinder, SmartPeakelFinder, PeakelFinder};
use mzdb::processing::signal::filtering::{SavitzkyGolaySmoother, SignalSmoother, BaselineRemover};
use mzdb::processing::math;
use mzdb::processing::ms;

use anyhow_ext::*;

const TEST_MZDB_PATH: &str = "data/OVEMB150205_12.mzDB";

/// Standard m/z tolerance used across all tests (10 ppm)
const MZ_TOLERANCE_PPM: f32 = 10.0;

// ============================================================================
// mzDB Reader Integration Tests
// ============================================================================

#[test]
fn test_open_mzdb_file() {
    let reader = MzDbReader::open(TEST_MZDB_PATH);
    assert!(reader.is_ok(), "Failed to open mzDB file: {:?}", reader.err());
    
    let reader = reader.unwrap();
    let version = reader.get_version().unwrap();
    println!("mzDB version: {:?}", version);
    assert!(version.is_some(), "mzDB version should be present");
}

#[test]
fn test_spectrum_count() {
    let reader = MzDbReader::open(TEST_MZDB_PATH).unwrap();
    let count = reader.get_spectrum_count();
    println!("Total spectra: {}", count);
    assert!(count > 0, "Should have at least one spectrum");
}

#[test]
fn test_spectrum_headers() {
    let reader = MzDbReader::open(TEST_MZDB_PATH).unwrap();
    let headers = reader.get_spectrum_headers();
    
    println!("Spectrum headers count: {}", headers.len());
    assert!(!headers.is_empty(), "Should have spectrum headers");
    
    for (i, header) in headers.iter().take(5).enumerate() {
        println!("Spectrum {}: id={}, ms_level={}, time={:.2}s, peaks={}",
            i + 1, header.id, header.ms_level, header.time, header.peaks_count);
    }
}

#[test]
fn test_get_single_spectrum() {
    let reader = MzDbReader::open(TEST_MZDB_PATH).unwrap();
    let spectrum = reader.get_spectrum(1);
    assert!(spectrum.is_ok(), "Failed to get spectrum: {:?}", spectrum.err());
    
    let spectrum = spectrum.unwrap();
    println!("Spectrum 1: {} peaks, time={:.2}s, ms_level={}",
        spectrum.data.peaks_count, spectrum.header.time, spectrum.header.ms_level);
    
    assert!(spectrum.data.peaks_count > 0, "Spectrum should have peaks");
    assert_eq!(spectrum.data.mz_array.len(), spectrum.data.peaks_count);
    assert_eq!(spectrum.data.intensity_array.len(), spectrum.data.peaks_count);
}

// ============================================================================
// Signal Processing with Real Data Tests (using TIC, not XIC)
// ============================================================================

#[test]
fn test_savitzky_golay_on_tic() {
    let reader = MzDbReader::open(TEST_MZDB_PATH).unwrap();
    let headers = reader.get_spectrum_headers();
    
    let tic: Vec<(f32, f32)> = headers.iter()
        .filter(|h| h.ms_level == 1)
        .map(|h| (h.time, h.tic))
        .collect();
    
    assert!(!tic.is_empty(), "Should have TIC data");
    println!("TIC data points: {}", tic.len());
    
    let smoother = SavitzkyGolaySmoother::new(5, 4, 1);
    let smoothed = smoother.smooth_time_intensity_pairs(&tic);
    
    assert_eq!(smoothed.len(), tic.len(), "Smoothed length should match");
    
    for (orig, smooth) in tic.iter().zip(smoothed.iter()) {
        assert_eq!(orig.0, smooth.0, "RT should be preserved");
    }
    
    let orig_sum: f64 = tic.iter().map(|(_, i)| *i as f64).sum();
    let smooth_sum: f64 = smoothed.iter().map(|(_, i)| *i as f64).sum();
    let diff_pct = ((orig_sum - smooth_sum) / orig_sum).abs() * 100.0;
    println!("TIC sum: original={:.2e}, smoothed={:.2e}, diff={:.2}%", orig_sum, smooth_sum, diff_pct);
}

#[test]
fn test_baseline_remover_on_tic() {
    let reader = MzDbReader::open(TEST_MZDB_PATH).unwrap();
    let headers = reader.get_spectrum_headers();
    
    let tic: Vec<(f32, f32)> = headers.iter()
        .filter(|h| h.ms_level == 1)
        .map(|h| (h.time, h.tic))
        .collect();
    
    let remover = BaselineRemover::new(1);
    let threshold = remover.calc_noise_threshold(&tic);
    
    println!("Calculated noise threshold: {:.2e}", threshold);
    assert!(threshold > 0.0, "Threshold should be positive");
    
    let peak_groups = remover.find_noise_free_peak_groups_indices(&tic, threshold);
    println!("Peak groups above threshold: {}", peak_groups.len());
}

#[test]
fn test_basic_peakel_finder_on_tic() {
    let reader = MzDbReader::open(TEST_MZDB_PATH).unwrap();
    let headers = reader.get_spectrum_headers();
    
    let tic: Vec<(f32, f32)> = headers.iter()
        .filter(|h| h.ms_level == 1)
        .map(|h| (h.time, h.tic))
        .collect();
    
    let finder = BasicPeakelFinder::default_params();
    let peakels = finder.find_peakels_indices(&tic);
    
    println!("Basic peakel finder found {} peakels in TIC", peakels.len());
    
    for (i, (start, end)) in peakels.iter().take(5).enumerate() {
        let duration = tic[*end].0 - tic[*start].0;
        let max_int = tic[*start..=*end].iter().map(|(_, i)| *i).fold(0.0f32, f32::max);
        println!("Peakel {}: indices {}-{}, duration={:.2}s, max_int={:.2e}", i + 1, start, end, duration, max_int);
    }
}

#[test]
fn test_smart_peakel_finder_on_tic() {
    let reader = MzDbReader::open(TEST_MZDB_PATH).unwrap();
    let headers = reader.get_spectrum_headers();
    
    let tic: Vec<(f32, f32)> = headers.iter()
        .filter(|h| h.ms_level == 1)
        .map(|h| (h.time, h.tic))
        .collect();
    
    let finder = SmartPeakelFinder::new();
    let peakels = finder.find_peakels_indices(&tic);
    
    println!("Smart peakel finder found {} peakels in TIC", peakels.len());
    
    for (i, (start, end)) in peakels.iter().take(5).enumerate() {
        let duration = tic[*end].0 - tic[*start].0;
        let max_int = tic[*start..=*end].iter().map(|(_, i)| *i).fold(0.0f32, f32::max);
        println!("Peakel {}: indices {}-{}, duration={:.2}s, max_int={:.2e}", i + 1, start, end, duration, max_int);
    }
}

// ============================================================================
// MS Utility Tests with Real Data
// ============================================================================

#[test]
fn test_ppm_tolerance_calculations() {
    let reader = MzDbReader::open(TEST_MZDB_PATH).unwrap();
    let spectrum = reader.get_spectrum(1).unwrap();
    
    if !spectrum.data.mz_array.is_empty() {
        let test_mz = spectrum.data.mz_array[0] as f64;
        let da = ms::ppm_to_da(test_mz, 10.0);
        let ppm = ms::da_to_ppm(test_mz, da);
        
        println!("m/z {:.4}: 10 ppm = {:.6} Da", test_mz, da);
        assert!((ppm - 10.0).abs() < 0.001, "Round-trip should preserve ppm");
        
        let (min_mz, max_mz) = ms::mz_range_from_ppm(test_mz, 10.0);
        println!("m/z range: {:.6} - {:.6}", min_mz, max_mz);
        assert!(min_mz < test_mz && max_mz > test_mz);
    }
}

#[test]
fn test_isotope_pattern_generation() {
    let reader = MzDbReader::open(TEST_MZDB_PATH).unwrap();
    let spectrum = reader.get_spectrum(1).unwrap();
    
    if !spectrum.data.mz_array.is_empty() {
        let mono_mz = spectrum.data.mz_array[0] as f64;
        let pattern = ms::TheoreticalIsotopePattern::from_averagine(mono_mz, 2, 5);
        
        println!("Isotope pattern for m/z {:.4} @ +2:", mono_mz);
        let mz_values = pattern.mz_values();
        for (i, mz) in mz_values.iter().enumerate() {
            let rel_int = pattern.isotope_intensity(i).unwrap_or(0.0);
            println!("  M+{}: m/z {:.4}, rel_int {:.3}", i, mz, rel_int);
        }
        
        for i in 1..mz_values.len() {
            let spacing = mz_values[i] - mz_values[i-1];
            let expected = ms::NEUTRON_MASS / 2.0;
            assert!((spacing - expected).abs() < 0.001, "Isotope spacing should be ~0.5 Da for +2");
        }
    }
}

// ============================================================================
// Math Utility Tests with Real Data
// ============================================================================

#[test]
fn test_histogram_on_intensities() {
    let reader = MzDbReader::open(TEST_MZDB_PATH).unwrap();
    let spectrum = reader.get_spectrum(1).unwrap();
    
    let intensities: Vec<f64> = spectrum.data.intensity_array.iter().map(|&i| i as f64).collect();
    
    if intensities.len() > 10 {
        let bins = math::compute_histogram(&intensities, 10);
        
        println!("Intensity histogram ({} peaks):", intensities.len());
        for (i, bin) in bins.iter().enumerate() {
            println!("  Bin {}: {:.2e} - {:.2e}, count={}", i, bin.lower_bound, bin.upper_bound, bin.count);
        }
        
        let total_count: usize = bins.iter().map(|b| b.count).sum();
        assert_eq!(total_count, intensities.len());
    }
}

#[test]
fn test_local_extrema_detection() {
    let reader = MzDbReader::open(TEST_MZDB_PATH).unwrap();
    let headers = reader.get_spectrum_headers();
    
    let tic: Vec<f64> = headers.iter()
        .filter(|h| h.ms_level == 1)
        .map(|h| h.tic as f64)
        .collect();
    
    if tic.len() > 10 {
        let extrema = math::find_local_extrema(&tic);
        let maxima: Vec<_> = extrema.iter().filter(|e| e.is_maximum()).collect();
        let minima: Vec<_> = extrema.iter().filter(|e| e.is_minimum()).collect();
        
        println!("TIC extrema: {} maxima, {} minima", maxima.len(), minima.len());
        for (i, max) in maxima.iter().take(5).enumerate() {
            println!("  Max {}: index={}, value={:.2e}", i + 1, max.index(), max.value());
        }
    }
}

#[test]
fn test_statistical_functions() {
    let reader = MzDbReader::open(TEST_MZDB_PATH).unwrap();
    let spectrum = reader.get_spectrum(1).unwrap();
    
    let intensities: Vec<f64> = spectrum.data.intensity_array.iter().map(|&i| i as f64).collect();
    
    if !intensities.is_empty() {
        let median = math::median(&intensities);
        let mad = math::mad(&intensities);
        let threshold = math::robust_noise_threshold(&intensities, 3.0);
        
        println!("Intensity statistics:");
        println!("  Median: {:.2e}", median);
        println!("  MAD: {:.2e}", mad);
        println!("  Robust noise threshold: {:.2e}", threshold);
        
        assert!(median > 0.0);
        assert!(mad >= 0.0);
        assert!(threshold >= median);
    }
}

// ============================================================================
// Walking-based Peakel Detection Test (matching CLI behavior)
// ============================================================================

/// Indexed spectrum for fast m/z queries during walking
struct IndexedSpectrum {
    #[allow(dead_code)]
    spectrum_idx: usize,
    spectrum_id: i64,
    time: f32,
    peaks: Vec<(f32, f32, usize)>, // (mz, intensity, peak_idx) sorted by m/z
}

impl IndexedSpectrum {
    /// Find the nearest peak within m/z tolerance using binary search
    fn find_nearest_peak(&self, target_mz: f32, mz_tol_da: f32) -> Option<(f32, f32, usize)> {
        if self.peaks.is_empty() {
            return None;
        }
        
        let min_mz = target_mz - mz_tol_da;
        let max_mz = target_mz + mz_tol_da;
        
        // Binary search for the first peak >= min_mz
        let start = self.peaks.partition_point(|&(mz, _, _)| mz < min_mz);
        
        if start >= self.peaks.len() {
            return None;
        }
        
        // Find the nearest peak within the m/z range
        let mut best: Option<(f32, f32, usize)> = None;
        let mut best_diff = mz_tol_da;
        
        for i in start..self.peaks.len() {
            let (mz, intensity, peak_idx) = self.peaks[i];
            if mz > max_mz {
                break;
            }
            let diff = (mz - target_mz).abs();
            if diff < best_diff {
                best_diff = diff;
                best = Some((mz, intensity, peak_idx));
            }
        }
        
        best
    }
}

/// Test walking-based peakel detection - the same algorithm used by mzdb2peakeldb
/// 
/// This test implements the "walking" approach that matches the Scala implementation:
/// 1. Build indexed spectra for fast m/z lookup
/// 2. Sort all peaks by intensity (descending)  
/// 3. For each unvisited apex peak, walk left and right along the RT axis
/// 4. For each adjacent spectrum, find the nearest peak within m/z tolerance
/// 5. Stop walking when too many consecutive gaps or hitting a used peak
/// 6. Run peakel detection on the extracted XIC
#[test]
fn test_walking_peakel_detection() -> Result<()> {
    use std::collections::HashSet;
    
    let reader = MzDbReader::open(TEST_MZDB_PATH).unwrap();
    
    println!("=== Walking-based Peakel Detection Test ===");
    println!("Using {} ppm tolerance", MZ_TOLERANCE_PPM);
    
    const MAX_CONSECUTIVE_GAPS: usize = 3;
    const MIN_INTENSITY: f32 = 100.0;
    const MIN_PEAKS: usize = 5;
    
    // Get all MS1 spectra headers
    let headers = reader.get_spectrum_headers();
    let ms1_headers: Vec<_> = headers.iter().filter(|h| h.ms_level == 1).collect();
    let ms1_count = ms1_headers.len();
    println!("MS1 spectra: {}", ms1_count);
    
    // Build indexed spectra for fast m/z queries
    let mut indexed_spectra: Vec<IndexedSpectrum> = Vec::with_capacity(ms1_count);
    
    for (idx, header) in ms1_headers.iter().enumerate() {
        let spectrum = reader.get_spectrum(header.id).unwrap();
        let peaks: Vec<(f32, f32, usize)> = spectrum.data.mz_array.iter()
            .zip(spectrum.data.intensity_array.iter())
            .enumerate()
            .filter(|&(_, (ref _mz, ref intensity))| **intensity >= MIN_INTENSITY)
            .map(|(peak_idx, (&mz, &intensity))| (mz, intensity, peak_idx))
            .collect();
        
        indexed_spectra.push(IndexedSpectrum {
            spectrum_idx: idx,
            spectrum_id: header.id,
            time: header.time,
            peaks,
        });
    }
    
    // Collect all peaks: (mz, intensity, spectrum_idx, peak_idx)
    let mut all_peaks: Vec<(f32, f32, usize, usize)> = Vec::new();
    
    for (spec_idx, indexed_spec) in indexed_spectra.iter().enumerate() {
        for &(mz, intensity, peak_idx) in &indexed_spec.peaks {
            all_peaks.push((mz, intensity, spec_idx, peak_idx));
        }
    }
    
    println!("Total peaks above threshold: {}", all_peaks.len());
    
    // Sort peaks by intensity (descending)
    all_peaks.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
    
    // Track used peaks per spectrum
    let mut used_peaks: Vec<HashSet<usize>> = vec![HashSet::new(); ms1_count];
    
    // Create the peakel finder
    let finder = SmartPeakelFinder::new();
    
    let mut detected_peakels: Vec<Peakel> = Vec::new();
    
    // Process peaks in intensity order (highest first) - limit to top 5000 for test speed
    for &(apex_mz, _apex_intensity, apex_spectrum_idx, apex_peak_idx) in all_peaks.iter().take(5000) {
        // Skip if already used
        if used_peaks[apex_spectrum_idx].contains(&apex_peak_idx) {
            continue;
        }
        
        let apex_rt = indexed_spectra[apex_spectrum_idx].time;
        let mz_tol_da = apex_mz * MZ_TOLERANCE_PPM / 1_000_000.0_f32;
        
        // Walk left and right to build XIC
        let mut xic_peaks: Vec<(f32, f32, f32, usize, usize)> = Vec::new(); // (mz, int, rt, spec_idx, peak_idx)
        
        // Add apex
        xic_peaks.push((apex_mz, _apex_intensity, apex_rt, apex_spectrum_idx, apex_peak_idx));
        
        // Walk in both directions
        for direction in [1i32, -1i32] {
            let mut consecutive_gaps = 0usize;
            let mut offset = 1i32;
            
            loop {
                let cur_idx = apex_spectrum_idx as i32 + (offset * direction);
                
                if cur_idx < 0 || cur_idx >= ms1_count as i32 {
                    break;
                }
                
                let cur_spectrum = &indexed_spectra[cur_idx as usize];
                
                if let Some((mz, intensity, peak_idx)) = cur_spectrum.find_nearest_peak(apex_mz, mz_tol_da) {
                    // Stop at peakel boundary (used peak)
                    if used_peaks[cur_idx as usize].contains(&peak_idx) {
                        break;
                    }
                    
                    let peak_data = (mz, intensity, cur_spectrum.time, cur_idx as usize, peak_idx);
                    if direction > 0 {
                        xic_peaks.push(peak_data);
                    } else {
                        xic_peaks.insert(0, peak_data);
                    }
                    consecutive_gaps = 0;
                } else {
                    consecutive_gaps += 1;
                }
                
                if consecutive_gaps > MAX_CONSECUTIVE_GAPS {
                    break;
                }
                
                offset += 1;
            }
        }
        
        if xic_peaks.len() < MIN_PEAKS {
            continue;
        }
        
        // Convert to time-intensity pairs
        let xic_pairs: Vec<(f32, f32)> = xic_peaks.iter()
            .map(|(_, int, rt, _, _)| (*rt, *int))
            .collect();
        
        // Detect peakels
        let peakel_indices = finder.find_peakels_indices(&xic_pairs);
        
        for (start_idx, end_idx) in &peakel_indices {
            if end_idx - start_idx < 3 {
                continue;
            }
            
            // Build peakel
            let mut builder = PeakelBuilder::new();
            
            for i in *start_idx..=*end_idx {
                let (mz, intensity, rt, spec_idx, peak_idx) = xic_peaks[i];
                let spectrum_id = indexed_spectra[spec_idx].spectrum_id;
                
                builder.add_point(spectrum_id, rt, mz, intensity, 0.0, 0.0);
                
                // Mark as used
                used_peaks[spec_idx].insert(peak_idx);
            }
            
            let peakel = builder.build()?;
            
            // Filter by amplitude
            let min_int = peakel.intensity_values().iter().cloned().fold(f32::INFINITY, f32::min);
            let max_int = peakel.apex_intensity().unwrap_or(0.0);
            let amplitude = if min_int > 0.0 { max_int / min_int } else { 2.0 };
            
            if amplitude >= 1.5 && peakel.peaks_count() >= MIN_PEAKS {
                detected_peakels.push(peakel);
            }
        }
    }
    
    println!("\n=== Results ===");
    println!("Total peakels detected: {}", detected_peakels.len());
    
    if !detected_peakels.is_empty() {
        // Sort by intensity
        detected_peakels.sort_by(|a, b| 
            b.apex_intensity().partial_cmp(&a.apex_intensity()).unwrap_or(std::cmp::Ordering::Equal)
        );
        
        println!("\nTop 10 peakels by intensity:");
        for (i, peakel) in detected_peakels.iter().take(10).enumerate() {
            println!("  {:2}: m/z={:.4}, RT={:.2}s, intensity={:.2e}, peaks={}",
                i + 1,
                peakel.apex_mz().unwrap_or(0.0),
                peakel.apex_elution_time().unwrap_or(0.0),
                peakel.apex_intensity().unwrap_or(0.0),
                peakel.peaks_count()
            );
        }
        
        // Statistics
        let avg_peaks: f32 = detected_peakels.iter().map(|p| p.peaks_count() as f32).sum::<f32>() 
            / detected_peakels.len() as f32;
        println!("\nAverage peaks per peakel: {:.1}", avg_peaks);
    }
    
    // Should detect a reasonable number of peakels from top 5000 peaks
    assert!(detected_peakels.len() >= 50, 
        "Should detect at least 50 peakels from top 5000 peaks, found {}", detected_peakels.len());
    
    // Validate peakel properties
    for peakel in &detected_peakels {
        assert!(peakel.peaks_count() >= MIN_PEAKS, "Peakel should have at least {} peaks", MIN_PEAKS);
        assert!(peakel.apex_intensity().unwrap_or(0.0) > 0.0, "Apex intensity should be positive");
        assert!(peakel.calc_area() > 0.0, "Area should be positive");
    }
    
    Ok(())
}

// ============================================================================
// Additional Processing Module Tests
// ============================================================================

#[test]
fn test_savitzky_golay_coefficients() {
    // Test that SG coefficients sum to 1 (preservation of area under curve)
    use mzdb::processing::signal::filtering::compute_sg_coefficients;
    
    let coeffs_5_2 = compute_sg_coefficients(2, 2, 2);
    let sum: f64 = coeffs_5_2.iter().sum();
    println!("SG coefficients (5-point, order 2): {:?}", coeffs_5_2);
    println!("Sum: {:.10}", sum);
    assert!((sum - 1.0).abs() < 1e-10, "SG coefficients should sum to 1");
    
    let coeffs_7_4 = compute_sg_coefficients(3, 3, 4);
    let sum: f64 = coeffs_7_4.iter().sum();
    println!("SG coefficients (7-point, order 4): {:?}", coeffs_7_4);
    println!("Sum: {:.10}", sum);
    assert!((sum - 1.0).abs() < 1e-10, "SG coefficients should sum to 1");
}

#[test]
fn test_ternary_slopes() {
    // Test derivative analysis on simple signal
    let signal = vec![1.0, 2.0, 4.0, 3.0, 1.0, 2.0, 5.0, 3.0];
    
    let slopes = math::calc_ternary_slopes(&signal, 1);
    println!("Signal: {:?}", signal);
    println!("First derivative slopes: {:?}", slopes);
    
    assert_eq!(slopes.len(), signal.len() - 1);
    assert_eq!(slopes[0], 1.0);  // Rising from 1 to 2
    assert_eq!(slopes[1], 1.0);  // Rising from 2 to 4
    assert_eq!(slopes[2], -1.0); // Falling from 4 to 3
    
    let slopes_2nd = math::calc_ternary_slopes(&signal, 2);
    println!("Second derivative slopes: {:?}", slopes_2nd);
    assert_eq!(slopes_2nd.len(), signal.len() - 2);
}

#[test]
fn test_mass_conversions() {
    // Test m/z to mass and back conversions
    let mz = 500.0;
    let charge = 2;
    
    let mass = ms::mz_to_mass(mz, charge);
    let mz_back = ms::mass_to_mz(mass, charge);
    
    println!("m/z {} @ charge +{} -> mass {} -> m/z {}", mz, charge, mass, mz_back);
    assert!((mz - mz_back).abs() < 1e-6, "Round-trip should preserve m/z");
    
    // Test isotope m/z calculation
    let iso1_mz = ms::isotope_mz(mz, charge, 1);
    let expected_shift = ms::NEUTRON_MASS / charge as f64;
    println!("M+1 isotope: {} (shift: {})", iso1_mz, iso1_mz - mz);
    assert!((iso1_mz - mz - expected_shift).abs() < 1e-6, "Isotope shift should be ~neutron/charge");
}

#[test]
fn test_peakel_model() -> Result<()> {
    // Test Peakel creation and calculations
    let spectrum_ids = vec![1, 2, 3, 4, 5];
    let elution_times = vec![10.0f32, 11.0, 12.0, 13.0, 14.0];
    let mz_values = vec![500.0, 500.01, 500.02, 500.01, 500.0];
    let intensity_values = vec![100.0f32, 500.0, 1000.0, 500.0, 100.0];
    
    let peakel = Peakel::from_vectors(
        spectrum_ids,
        elution_times,
        mz_values,
        intensity_values,
        None,
        None,
        0,
    )?;
    
    println!("Peakel properties:");
    println!("  Peaks count: {}", peakel.peaks_count());
    println!("  Apex index: {:?}", peakel.apex_index());
    println!("  Apex m/z: {:.4}", peakel.apex_mz().unwrap_or(0.0));
    println!("  Apex RT: {:.2}s", peakel.apex_elution_time().unwrap_or(0.0));
    println!("  Apex intensity: {:.0}", peakel.apex_intensity().unwrap_or(0.0));
    println!("  Weighted m/z: {:.4}", peakel.calc_weighted_mz());
    println!("  Weighted RT: {:.2}s", peakel.calc_weighted_average_time());
    println!("  Duration: {:.2}s", peakel.calc_duration());
    println!("  Area: {:.0}", peakel.calc_area());
    println!("  Gap count: {}", peakel.gap_count);
    
    assert_eq!(peakel.peaks_count(), 5);
    assert_eq!(peakel.apex_index(), Some(2));  // Peak at intensity 1000
    assert_eq!(peakel.apex_intensity(), Some(1000.0));
    assert_eq!(peakel.apex_elution_time(), Some(12.0));
    assert_eq!(peakel.calc_duration(), 4.0);
    assert_eq!(peakel.calc_area(), 2100.0);
    assert_eq!(peakel.gap_count, 0);
    
    Ok(())
}

#[test]
fn test_peakel_builder() -> Result<()> {
    // Test PeakelBuilder for constructing peakels incrementally
    let mut builder = PeakelBuilder::new();
    
    builder.add_point(1, 10.0, 500.0, 100.0, 0.01, 0.02);
    builder.add_point(2, 11.0, 500.01, 500.0, 0.01, 0.02);
    builder.add_point(3, 12.0, 500.02, 1000.0, 0.01, 0.02);
    builder.add_point(4, 13.0, 500.01, 500.0, 0.01, 0.02);
    builder.add_point(5, 14.0, 500.0, 100.0, 0.01, 0.02);
    
    let peakel = builder.build()?;
    
    println!("Built peakel:");
    println!("  Peaks count: {}", peakel.peaks_count());
    println!("  Apex intensity: {:.0}", peakel.apex_intensity().unwrap_or(0.0));
    println!("  Left HWHM mean: {:.4}", peakel.left_hwhm_mean());
    println!("  Right HWHM mean: {:.4}", peakel.right_hwhm_mean());
    
    assert_eq!(peakel.peaks_count(), 5);
    assert_eq!(peakel.apex_intensity(), Some(1000.0));
    assert!(peakel.left_hwhm_mean() > 0.0);
    assert!(peakel.right_hwhm_mean() > 0.0);
    
    Ok(())
}

#[test]
fn test_peak_with_lc_context() {
    // Test Peak creation with LC context
    let lc_ctx = LcContext::new(42, 15.5);
    let peak = Peak::with_hwhm(500.25, 1234.5, 0.01, 0.02, Some(lc_ctx));
    
    println!("Peak: m/z={:.4}, intensity={:.1}", peak.mz, peak.intensity);
    
    assert_eq!(peak.mz, 500.25);
    assert_eq!(peak.intensity, 1234.5);
    assert!(peak.lc_context.is_some());
    
    let ctx = peak.lc_context.unwrap();
    assert_eq!(ctx.spectrum_id, 42);
    assert_eq!(ctx.elution_time, 15.5);
}
