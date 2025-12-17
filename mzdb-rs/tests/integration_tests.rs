//! Integration tests for mzdb-rs
//!
//! These tests require a test mzDB file at `./mzdb-rs/data/small.mzDB`

use mzdb::cache::create_entity_cache;
use mzdb::model::*;
use mzdb::queries::*;
use mzdb::iterator::*;
use mzdb::MzDbReader;
use fallible_iterator::FallibleIterator;
use rusqlite::Connection;
use std::path::PathBuf;

/// Get path to test fixture
fn test_db_path() -> PathBuf {
    let mut path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    path.push("data");
    path.push("small.mzDB");
    path
}

//const TEST_DB_PATH: &str = "./mzdb-rs/data/small.mzDB";

fn open_test_db() -> Connection {
    Connection::open(test_db_path()).expect("Failed to open test database")
}

#[test]
fn test_open_database() {
    let _db = open_test_db();
}

#[test]
fn test_get_mzdb_version() {
    let db = open_test_db();
    let version = get_mzdb_version(&db).expect("Failed to get version");
    assert!(version.is_some(), "mzDB version should be present");
}

#[test]
fn test_get_pwiz_mzdb_version() {
    let db = open_test_db();
    let version = get_pwiz_mzdb_version(&db).expect("Failed to get pwiz version");
    // Version may or may not be present depending on the file
    println!("pwiz-mzdb version: {:?}", version);
}

#[test]
fn test_create_entity_cache() {
    let db = open_test_db();
    let cache = create_entity_cache(&db).expect("Failed to create entity cache");
    
    // Check that we have some spectrum headers
    assert!(!cache.spectrum_headers.is_empty(), "Should have spectrum headers");
    
    // Check bounding box sizes are reasonable
    assert!(cache.bb_sizes.bb_mz_height_ms1 > 0.0, "MS1 BB mz height should be positive");
}

#[test]
fn test_get_max_ms_level() {
    let db = open_test_db();
    let max_level = get_max_ms_level(&db).expect("Failed to get max MS level");
    assert!(max_level.is_some(), "Max MS level should be present");
    let level = max_level.unwrap();
    assert!(level >= 1 && level <= 10, "MS level should be reasonable (1-10)");
}

#[test]
fn test_get_spectra_count_by_ms_level() {
    let db = open_test_db();
    
    // Check MS1 count
    let ms1_count = get_spectra_count_by_ms_level(&db, 1)
        .expect("Failed to get MS1 count");
    assert!(ms1_count.is_some(), "MS1 count should be present");
    assert!(ms1_count.unwrap() > 0, "Should have at least one MS1 spectrum");
}

#[test]
fn test_list_data_encodings() {
    let db = open_test_db();
    let encodings = list_data_encodings(&db).expect("Failed to list data encodings");
    
    assert!(!encodings.is_empty(), "Should have at least one data encoding");
    
    for enc in &encodings {
        assert!(enc.id > 0, "Encoding ID should be positive");
        println!("Encoding {}: mode={:?}, peak_encoding={:?}", 
                 enc.id, enc.mode, enc.peak_encoding);
    }
}

#[test]
fn test_get_spectrum() {
    let db = open_test_db();
    let cache = create_entity_cache(&db).expect("Failed to create cache");
    
    // Get first spectrum
    let spectrum = get_spectrum(&db, 1, &cache).expect("Failed to get spectrum 1");
    
    assert_eq!(spectrum.header.id, 1);
    assert!(spectrum.data.peaks_count > 0, "Spectrum should have peaks");
    assert_eq!(spectrum.data.mz_array.len(), spectrum.data.peaks_count);
    assert_eq!(spectrum.data.intensity_array.len(), spectrum.data.peaks_count);
    
    // Verify m/z values are sorted
    for i in 1..spectrum.data.mz_array.len() {
        assert!(spectrum.data.mz_array[i] >= spectrum.data.mz_array[i-1],
                "m/z values should be sorted");
    }
}

#[test]
fn test_for_each_bb() {
    let db = open_test_db();
    let mut bb_count = 0;
    
    for_each_bb(&db, None, |bb| {
        assert!(bb.id > 0, "BB ID should be positive");
        assert!(!bb.blob_data.is_empty(), "BB should have blob data");
        bb_count += 1;
        Ok(())
    }).expect("Failed to iterate bounding boxes");
    
    assert!(bb_count > 0, "Should have at least one bounding box");
    println!("Total bounding boxes: {}", bb_count);
}

#[test]
fn test_for_each_spectrum() {
    let db = open_test_db();
    let cache = create_entity_cache(&db).expect("Failed to create cache");
    let mut spectrum_count = 0;
    let mut ms1_count = 0;
    let mut ms2_count = 0;
    
    for_each_spectrum(&db, &cache, None, |spectrum| {
        spectrum_count += 1;
        match spectrum.header.ms_level {
            1 => ms1_count += 1,
            2 => ms2_count += 1,
            _ => {}
        }
        
        // Basic validation
        assert!(spectrum.header.id > 0);
        assert!(spectrum.header.time >= 0.0);
        assert!(spectrum.data.peaks_count == spectrum.data.mz_array.len());
        
        Ok(())
    }).expect("Failed to iterate spectra");
    
    assert!(spectrum_count > 0, "Should have spectra");
    println!("Total: {}, MS1: {}, MS2: {}", spectrum_count, ms1_count, ms2_count);
}

#[test]
fn test_for_each_spectrum_ms1_only() {
    let db = open_test_db();
    let cache = create_entity_cache(&db).expect("Failed to create cache");
    let mut count = 0;
    
    for_each_spectrum(&db, &cache, Some(1), |spectrum| {
        assert_eq!(spectrum.header.ms_level, 1, "Should only get MS1 spectra");
        count += 1;
        Ok(())
    }).expect("Failed to iterate MS1 spectra");
    
    assert!(count > 0, "Should have MS1 spectra");
}

#[test]
fn test_spectrum_data_get_nearest_peak() {
    let db = open_test_db();
    let cache = create_entity_cache(&db).expect("Failed to create cache");
    
    // Get first spectrum with some peaks
    let spectrum = get_spectrum(&db, 1, &cache).expect("Failed to get spectrum");
    
    if spectrum.data.peaks_count > 0 {
        let first_mz = spectrum.data.mz_array[0];
        let rt = spectrum.header.time;
        
        // Search for the first peak - should find it
        let peak = spectrum.data.get_nearest_peak(first_mz, 10.0, rt);
        assert!(peak.is_some(), "Should find the peak we're searching for");
        
        let found_peak = peak.unwrap();
        assert!((found_peak.mz - first_mz).abs() < 0.01, "Found peak should be close to searched m/z");
    }
}

#[test]
fn test_xic_generation() {
    let db = open_test_db();
    let cache = create_entity_cache(&db).expect("Failed to create cache");
    
    // Get a real m/z from the data to search for
    let spectrum = get_spectrum(&db, 1, &cache).expect("Failed to get spectrum");
    
    if spectrum.data.peaks_count > 0 && spectrum.header.ms_level == 1 {
        let search_mz = spectrum.data.mz_array[0];
        
        let xic = get_ms_xic(
            &db,
            search_mz,
            20.0, // 20 ppm tolerance
            None,
            None,
            XicMethod::Max,
            &cache,
        ).expect("Failed to generate XIC");
        
        // We should get at least one point (the one from our source spectrum)
        println!("XIC points for m/z {}: {}", search_mz, xic.len());
    }
}

// Unit tests for model types
mod model_tests {
    use super::*;

    #[test]
    fn test_data_encoding_peak_size() {
        let enc_lowres = DataEncoding {
            id: 1,
            mode: DataMode::Centroid,
            peak_encoding: PeakEncoding::LowRes,
            compression: "none".to_string(),
            byte_order: ByteOrder::LittleEndian,
        };
        assert_eq!(enc_lowres.get_peak_size(), 8);

        let enc_highres = DataEncoding {
            id: 2,
            mode: DataMode::Centroid,
            peak_encoding: PeakEncoding::HighRes,
            compression: "none".to_string(),
            byte_order: ByteOrder::LittleEndian,
        };
        assert_eq!(enc_highres.get_peak_size(), 12);

        let enc_fitted = DataEncoding {
            id: 3,
            mode: DataMode::Fitted,
            peak_encoding: PeakEncoding::HighRes,
            compression: "none".to_string(),
            byte_order: ByteOrder::LittleEndian,
        };
        // Fitted adds 8 bytes for HWHM values
        assert_eq!(enc_fitted.get_peak_size(), 20);
    }

    #[test]
    fn test_spectrum_data_new() {
        let enc = DataEncoding {
            id: 1,
            mode: DataMode::Centroid,
            peak_encoding: PeakEncoding::HighRes,
            compression: "none".to_string(),
            byte_order: ByteOrder::LittleEndian,
        };

        let mz = vec![100.0, 200.0, 300.0];
        let intensity = vec![1000.0, 2000.0, 1500.0];

        let data = SpectrumData::new(enc, mz.clone(), intensity.clone(), None, None);

        assert_eq!(data.peaks_count, 3);
        assert_eq!(data.mz_array, mz);
        assert_eq!(data.intensity_array, intensity);
        assert!(data.lwhm_array.is_empty());
        assert!(data.rwhm_array.is_empty());
    }
}

// Tests for the new fallible iterator API
#[test]
fn test_fallible_iterator_all_spectra() {
    let db = open_test_db();
    let cache = create_entity_cache(&db).expect("Failed to create cache");
    
    let mut iter = SpectrumIterator::new(&db, &cache, None)
        .expect("Failed to create iterator");
    
    let mut spectrum_count = 0;
    let mut ms1_count = 0;
    let mut ms2_count = 0;
    
    while let Some(spectrum) = iter.next().expect("Failed to get next spectrum") {
        spectrum_count += 1;
        match spectrum.header.ms_level {
            1 => ms1_count += 1,
            2 => ms2_count += 1,
            _ => {}
        }
        
        // Basic validation
        assert!(spectrum.header.id > 0);
        assert!(spectrum.header.time >= 0.0);
        assert_eq!(spectrum.data.peaks_count, spectrum.data.mz_array.len());
    }
    
    assert!(spectrum_count > 0, "Should have spectra");
    println!("Fallible iterator - Total: {}, MS1: {}, MS2: {}", spectrum_count, ms1_count, ms2_count);
}

#[test]
fn test_fallible_iterator_ms1_only() {
    let db = open_test_db();
    let cache = create_entity_cache(&db).expect("Failed to create cache");
    
    let mut iter = SpectrumIterator::new(&db, &cache, Some(1))
        .expect("Failed to create iterator");
    
    let mut count = 0;
    
    while let Some(spectrum) = iter.next().expect("Failed to get next spectrum") {
        assert_eq!(spectrum.header.ms_level, 1, "Should only get MS1 spectra");
        count += 1;
    }
    
    assert!(count > 0, "Should have MS1 spectra");
    println!("Fallible iterator MS1 - Count: {}", count);
}

#[test]
fn test_fallible_iterator_with_methods() {
    let db = open_test_db();
    let cache = create_entity_cache(&db).expect("Failed to create cache");
    
    let iter = SpectrumIterator::new(&db, &cache, None)
        .expect("Failed to create iterator");
    
    // Test count method
    let count = iter.count().expect("Failed to count spectra");
    assert!(count > 0, "Should have spectra");
    println!("Total spectra count: {}", count);
    
    // Create a new iterator for fold
    let iter2 = SpectrumIterator::new(&db, &cache, Some(1))
        .expect("Failed to create iterator");
    
    // Test fold to calculate total peaks
    let total_peaks = iter2.fold(0, |acc, spectrum| {
        Ok(acc + spectrum.data.peaks_count)
    }).expect("Failed to fold");
    
    println!("Total peaks in MS1 spectra: {}", total_peaks);
    assert!(total_peaks > 0, "Should have peaks");
}

#[test]
fn test_mzdb_reader_iter_spectra() {
    let reader = MzDbReader::open(test_db_path().to_str().unwrap())
        .expect("Failed to open MzDbReader");
    
    let mut iter = reader.iter_spectra(None).expect("Failed to create iterator");
    
    let mut count = 0;
    while let Some(spectrum) = iter.next().expect("Failed to get next spectrum") {
        count += 1;
        assert!(spectrum.header.id > 0);
    }
    
    assert!(count > 0, "Should have spectra");
    println!("MzDbReader iter_spectra - Count: {}", count);
}

#[test]
fn test_mzdb_reader_for_each_spectrum() {
    let reader = MzDbReader::open(test_db_path().to_str().unwrap())
        .expect("Failed to open MzDbReader");
    
    let mut count = 0;
    reader.for_each_spectrum(Some(1), |spectrum| {
        count += 1;
        assert_eq!(spectrum.header.ms_level, 1);
        Ok(())
    }).expect("Failed to iterate spectra");
    
    assert!(count > 0, "Should have MS1 spectra");
    println!("MzDbReader for_each_spectrum - Count: {}", count);
}

#[test]
fn test_fallible_iterator_collect() {
    let db = open_test_db();
    let cache = create_entity_cache(&db).expect("Failed to create cache");
    
    let iter = SpectrumIterator::new(&db, &cache, Some(1))
        .expect("Failed to create iterator");
    
    // Collect all MS1 spectra into a Vec
    let spectra: Vec<Spectrum> = iter.collect().expect("Failed to collect spectra");
    
    assert!(spectra.len() > 0, "Should have collected MS1 spectra");
    
    // Verify they're all MS1
    for spectrum in &spectra {
        assert_eq!(spectrum.header.ms_level, 1);
    }
    
    println!("Collected {} MS1 spectra", spectra.len());
}
