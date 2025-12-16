//! Integration tests for mzdb-rs extended functionality
//!
//! These tests verify the new metadata, chromatogram, and R-tree functionality.
//! Tests require a test mzDB file at `./data/small.mzDB`

use std::path::PathBuf;
use rusqlite::Connection;

/// Get path to test fixture
fn test_db_path() -> PathBuf {
    let mut path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    path.push("data");
    path.push("small.mzDB");
    path
}

fn open_test_db() -> Connection {
    Connection::open(test_db_path()).expect("Failed to open test database")
}

// ============================================================================
// MzDbReader high-level API tests
// ============================================================================

mod reader_tests {
    use super::*;
    use mzdb::MzDbReader;

    #[test]
    fn test_open_reader() {
        let path = test_db_path();
        if !path.exists() {
            eprintln!("Skipping test: test database not found");
            return;
        }
        
        let reader = MzDbReader::open(path.to_str().unwrap())
            .expect("Failed to open reader");
        
        assert!(reader.get_spectrum_count() > 0);
    }

    #[test]
    fn test_file_metadata() {
        let path = test_db_path();
        if !path.exists() { return; }
        
        let reader = MzDbReader::open(path.to_str().unwrap()).unwrap();
        
        // Version should be present
        let version = reader.get_version().unwrap();
        assert!(version.is_some());
        println!("mzDB version: {:?}", version);
        
        // BB sizes should be valid
        let bb_sizes = reader.get_bb_sizes();
        assert!(bb_sizes.bb_mz_height_ms1 > 0.0);
        assert!(bb_sizes.bb_rt_width_ms1 > 0.0);
    }

    #[test]
    fn test_spectrum_access() {
        let path = test_db_path();
        if !path.exists() { return; }
        
        let reader = MzDbReader::open(path.to_str().unwrap()).unwrap();
        
        // Get spectrum headers
        let headers = reader.get_spectrum_headers();
        assert!(!headers.is_empty());
        
        // Get first spectrum
        let spectrum = reader.get_spectrum(1).unwrap();
        assert_eq!(spectrum.header.id, 1);
        assert!(spectrum.data.peaks_count > 0 || spectrum.data.mz_array.is_empty());
        
        // Get max MS level
        let max_level = reader.get_max_ms_level().unwrap();
        assert!(max_level.is_some());
        assert!(max_level.unwrap() >= 1);
    }

    #[test]
    fn test_xic_generation() {
        let path = test_db_path();
        if !path.exists() { return; }
        
        let reader = MzDbReader::open(path.to_str().unwrap()).unwrap();
        
        // Get a real m/z from the data
        let spectrum = reader.get_spectrum(1).unwrap();
        if spectrum.data.peaks_count > 0 && spectrum.header.ms_level == 1 {
            let search_mz = spectrum.data.mz_array[0];
            
            let xic = reader.get_xic(
                search_mz,
                20.0, // 20 ppm
                None,
                None,
                mzdb::XicMethod::Max,
            ).unwrap();
            
            println!("XIC points for m/z {}: {}", search_mz, xic.len());
        }
    }
}

// ============================================================================
// Metadata tests
// ============================================================================

mod metadata_tests {
    use super::*;
    use mzdb::metadata::*;

    #[test]
    fn test_mzdb_metadata() {
        let db = open_test_db();
        
        let metadata = get_mzdb_metadata(&db);
        if let Ok(Some(meta)) = metadata {
            assert!(!meta.version.is_empty());
            println!("mzDB version: {}", meta.version);
            println!("Creation timestamp: {}", meta.creation_timestamp);
        }
    }

    #[test]
    fn test_runs() {
        let db = open_test_db();
        
        let runs = list_runs(&db);
        if let Ok(run_list) = runs {
            if !run_list.is_empty() {
                let run = &run_list[0];
                println!("Run: {} (ID: {})", run.name, run.id);
                
                // Get specific run
                if let Ok(Some(fetched)) = get_run(&db, run.id) {
                    assert_eq!(fetched.name, run.name);
                }
            }
        }
    }

    #[test]
    fn test_samples() {
        let db = open_test_db();
        
        let samples = list_samples(&db);
        if let Ok(sample_list) = samples {
            for sample in &sample_list {
                println!("Sample: {} (ID: {})", sample.name, sample.id);
            }
        }
    }

    #[test]
    fn test_software() {
        let db = open_test_db();
        
        let software = list_software(&db);
        if let Ok(sw_list) = software {
            for sw in &sw_list {
                println!("Software: {} v{}", sw.name, sw.version);
            }
            
            // Try to find mzDB writer
            if let Ok(Some(mzdb_sw)) = get_software_by_name(&db, "%mzDB%") {
                println!("mzDB writer: {} v{}", mzdb_sw.name, mzdb_sw.version);
            }
        }
    }

    #[test]
    fn test_source_files() {
        let db = open_test_db();
        
        let files = list_source_files(&db);
        if let Ok(file_list) = files {
            for file in &file_list {
                println!("Source file: {} at {}", file.name, file.location);
            }
        }
    }

    #[test]
    fn test_instrument_configurations() {
        let db = open_test_db();
        
        let configs = list_instrument_configurations(&db);
        if let Ok(config_list) = configs {
            for config in &config_list {
                println!("Instrument config: {}", config.name);
            }
        }
    }

    #[test]
    fn test_data_processing() {
        let db = open_test_db();
        
        let processings = list_data_processings(&db);
        if let Ok(proc_list) = processings {
            for proc in &proc_list {
                println!("Data processing: {}", proc.name);
                
                // Get methods for this processing
                if let Ok(methods) = get_processing_methods_for_workflow(&db, proc.id) {
                    for method in methods {
                        println!("  Method #{}", method.number);
                    }
                }
            }
        }
    }

    #[test]
    fn test_controlled_vocabularies() {
        let db = open_test_db();
        
        let cvs = list_controlled_vocabularies(&db);
        if let Ok(cv_list) = cvs {
            for cv in &cv_list {
                println!("CV: {} - {}", cv.id, cv.full_name);
            }
        }
    }

    #[test]
    fn test_cv_terms() {
        let db = open_test_db();
        
        let terms = list_cv_terms(&db);
        if let Ok(term_list) = terms {
            println!("Total CV terms: {}", term_list.len());
            
            // Search for specific terms
            if let Ok(scan_terms) = search_cv_terms(&db, "scan") {
                println!("Terms containing 'scan': {}", scan_terms.len());
            }
        }
    }

    #[test]
    fn test_cv_units() {
        let db = open_test_db();
        
        let units = list_cv_units(&db);
        if let Ok(unit_list) = units {
            for unit in &unit_list {
                println!("Unit: {} ({})", unit.name, unit.accession);
            }
        }
    }
}

// ============================================================================
// Chromatogram tests
// ============================================================================

mod chromatogram_tests {
    use super::*;
    use mzdb::chromatogram::*;

    #[test]
    fn test_list_chromatograms() {
        let db = open_test_db();
        
        let chroms = list_chromatograms(&db);
        if let Ok(chrom_list) = chroms {
            println!("Chromatograms: {}", chrom_list.len());
            
            for chrom in &chrom_list {
                println!("  {}: {} ({})", 
                    chrom.id, 
                    chrom.name, 
                    chrom.activation_type
                );
                println!("    Type: {:?}", chrom.chromatogram_type());
            }
        }
    }

    #[test]
    fn test_get_tic() {
        let db = open_test_db();
        
        if let Ok(Some(tic)) = get_tic_chromatogram(&db) {
            println!("TIC chromatogram: {}", tic.name);
            
            // Get data
            if let Ok(data) = get_chromatogram_data(&db, tic.id) {
                println!("  Points: {}", data.points_count);
                
                if let Some((time, intensity)) = data.get_max_intensity_point() {
                    println!("  Max intensity: {} at time {}", intensity, time);
                }
            }
        }
    }

    #[test]
    fn test_srm_chromatograms() {
        let db = open_test_db();
        
        let srm_chroms = list_srm_chromatograms(&db);
        if let Ok(chrom_list) = srm_chroms {
            println!("SRM chromatograms: {}", chrom_list.len());
            
            for chrom in &chrom_list {
                assert!(chrom.is_srm());
                println!("  {}: {}", chrom.id, chrom.name);
            }
        }
    }

    #[test]
    fn test_chromatogram_data_methods() {
        let db = open_test_db();
        
        let chroms = list_chromatograms(&db);
        if let Ok(chrom_list) = chroms {
            if let Some(chrom) = chrom_list.first() {
                if let Ok(data) = get_chromatogram_data(&db, chrom.id) {
                    if data.points_count > 0 {
                        // Test interpolation
                        let mid_time = (data.time_array[0] + data.time_array[data.points_count - 1]) / 2.0;
                        let interp = data.interpolate_at(mid_time);
                        assert!(interp.is_some());
                        
                        // Test range query
                        let points = data.get_points_in_range(
                            data.time_array[0],
                            data.time_array[data.points_count - 1]
                        );
                        assert_eq!(points.len(), data.points_count);
                        
                        // Test total intensity
                        let total = data.get_total_intensity();
                        assert!(total >= 0.0);
                    }
                }
            }
        }
    }
}

// ============================================================================
// R-tree tests
// ============================================================================

mod rtree_tests {
    use super::*;
    use mzdb::rtree::*;

    #[test]
    fn test_rtree_availability() {
        let db = open_test_db();
        
        let has_rt = has_rtree(&db);
        if let Ok(available) = has_rt {
            println!("R-tree available: {}", available);
        }
        
        let has_msn = has_msn_rtree(&db);
        if let Ok(available) = has_msn {
            println!("MSn R-tree available: {}", available);
        }
    }

    #[test]
    fn test_rtree_stats() {
        let db = open_test_db();
        
        if let Ok(Some(stats)) = get_rtree_stats(&db) {
            println!("R-tree stats:");
            println!("  Entries: {}", stats.entry_count);
            println!("  m/z range: {:.2} - {:.2}", stats.global_min_mz, stats.global_max_mz);
            println!("  Time range: {:.2} - {:.2}", stats.global_min_time, stats.global_max_time);
        }
    }

    #[test]
    fn test_rtree_mz_query() {
        let db = open_test_db();
        
        if let Ok(true) = has_rtree(&db) {
            // Query for common m/z range
            let entries = query_bounding_boxes_in_mz_range(&db, 400.0, 600.0);
            if let Ok(entry_list) = entries {
                println!("BBs in m/z 400-600: {}", entry_list.len());
            }
            
            // Query with ppm tolerance
            let entries_ppm = query_bounding_boxes_at_mz_ppm(&db, 500.0, 10.0);
            if let Ok(entry_list) = entries_ppm {
                println!("BBs at 500 m/z (10 ppm): {}", entry_list.len());
            }
        }
    }

    #[test]
    fn test_rtree_region_query() {
        let db = open_test_db();
        
        if let Ok(true) = has_rtree(&db) {
            // Query a 2D region
            let entries = query_bounding_boxes_in_region(&db, 400.0, 600.0, 0.0, 60.0);
            if let Ok(entry_list) = entries {
                println!("BBs in region (400-600 m/z, 0-60 min): {}", entry_list.len());
                
                for entry in entry_list.iter().take(3) {
                    println!("  BB {}: m/z {:.2}-{:.2}, time {:.2}-{:.2}",
                        entry.id, entry.min_mz, entry.max_mz, entry.min_time, entry.max_time);
                }
            }
        }
    }

    #[test]
    fn test_rtree_point_query() {
        let db = open_test_db();
        
        if let Ok(true) = has_rtree(&db) {
            // Query for a specific point
            let entries = query_bounding_boxes_containing_point(&db, 500.0, 30.0);
            if let Ok(entry_list) = entries {
                println!("BBs containing (500 m/z, 30 min): {}", entry_list.len());
            }
        }
    }

    #[test]
    fn test_entry_methods() {
        let entry = BoundingBoxRTreeEntry {
            id: 1,
            min_mz: 400.0,
            max_mz: 600.0,
            min_time: 10.0,
            max_time: 20.0,
        };
        
        assert!(entry.contains_mz(500.0));
        assert!(!entry.contains_mz(300.0));
        assert!(entry.contains_time(15.0));
        assert!(entry.contains_point(500.0, 15.0));
        assert_eq!(entry.mz_width(), 200.0);
        assert_eq!(entry.time_width(), 10.0);
        assert_eq!(entry.center_mz(), 500.0);
        assert_eq!(entry.center_time(), 15.0);
    }

    #[test]
    fn test_parent_mz_windows() {
        let db = open_test_db();
        
        if let Ok(true) = has_msn_rtree(&db) {
            if let Ok(windows) = get_parent_mz_windows(&db) {
                println!("Parent m/z windows: {}", windows.len());
                for (min_mz, max_mz) in windows.iter().take(5) {
                    println!("  {:.2} - {:.2}", min_mz, max_mz);
                }
            }
        }
    }
}

// ============================================================================
// Extended query tests
// ============================================================================

mod query_tests {
    use super::*;
    
    #[test]
    fn test_spectrum_headers_struct() {
        
        let db = open_test_db();
        let cache = mzdb::cache::create_entity_cache(&db).unwrap();
        
        for header in cache.spectrum_headers.iter().take(5) {
            println!("Spectrum {}: MS{}, time={:.2}, tic={:.0}",
                header.id, header.ms_level, header.time, header.tic);
            
            if let Some(prec_mz) = header.precursor_mz {
                println!("  Precursor: {:.4} m/z", prec_mz);
            }
        }
    }
}
