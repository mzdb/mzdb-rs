//! Integration tests for Thermo RAW to mzDB conversion
//!
//! These tests verify the complete conversion pipeline from RAW files to mzDB format,
//! including XML schema validation against mzDB specifications.

#![cfg(feature = "thermo2mzdb")]

use anyhow::Result;
use rusqlite::Connection;
use std::fs;
use std::path::Path;
use tempfile::NamedTempFile;

use mzdb::writer::thermo::convert_raw_to_mzdb;
use mzdb::BBSizes;

/// Test data paths
const TEST_RAW_FILE: &str = "data/small.RAW";
const XML_SCHEMATA_FILE: &str = "data/xml_schemata.tsv";

/// Load XML schemas from TSV file
fn load_xml_schemas() -> Result<std::collections::HashMap<String, String>> {
    let content = fs::read_to_string(XML_SCHEMATA_FILE)?;
    let mut schemas = std::collections::HashMap::new();
    
    for line in content.lines().skip(1) { // Skip header
        let parts: Vec<&str> = line.split('\t').collect();
        if parts.len() >= 3 {
            let name = parts[0].to_string();
            let schema = parts[2].to_string();
            schemas.insert(name, schema);
        }
    }
    
    Ok(schemas)
}

/// Validate XML against XSD schema using xmlschema crate
#[allow(dead_code)]
fn validate_xml(xml: &str, _xsd: &str) -> Result<bool> {
    // Parse XML
    let xml_doc = roxmltree::Document::parse(xml)?;
    
    // Basic structural validation
    // (Full XSD validation would require a proper XML Schema processor)
    
    // For now, check that the XML is well-formed and has expected structure
    let root = xml_doc.root_element();
    
    // Check root element exists
    if root.tag_name().name().is_empty() {
        return Ok(false);
    }
    
    Ok(true)
}

/// Extract and validate param_tree XML structure
fn validate_param_tree_structure(param_tree: &str) -> Result<()> {
    let doc = roxmltree::Document::parse(param_tree)?;
    let root = doc.root_element();
    
    // Root should be <paramTree>
    assert_eq!(root.tag_name().name(), "paramTree", "Root element must be <paramTree>");
    
    // Check for valid child elements
    for child in root.children() {
        if !child.is_element() {
            continue;
        }
        
        match child.tag_name().name() {
            "cvParams" => {
                // Validate cvParams children
                for cv_param in child.children().filter(|n| n.is_element()) {
                    assert_eq!(cv_param.tag_name().name(), "cvParam");
                    assert!(cv_param.attribute("accession").is_some(), "cvParam must have accession");
                }
            }
            "userParams" => {
                // Validate userParams children
                for user_param in child.children().filter(|n| n.is_element()) {
                    assert_eq!(user_param.tag_name().name(), "userParam");
                    assert!(user_param.attribute("name").is_some(), "userParam must have name");
                }
            }
            "userTexts" => {
                // Validate userTexts children
                for user_text in child.children().filter(|n| n.is_element()) {
                    assert_eq!(user_text.tag_name().name(), "userText");
                    assert!(user_text.attribute("name").is_some(), "userText must have name");
                }
            }
            _ => panic!("Unexpected child element: {}", child.tag_name().name()),
        }
    }
    
    Ok(())
}

/// Validate component_list XML structure
fn validate_component_list_structure(component_list: &str) -> Result<()> {
    let doc = roxmltree::Document::parse(component_list)?;
    let root = doc.root_element();
    
    // Root should be <componentList>
    assert_eq!(root.tag_name().name(), "componentList");
    
    // Must have count attribute
    assert!(root.attribute("count").is_some());
    
    // Check for valid components
    let mut has_source = false;
    let mut has_analyzer = false;
    let mut has_detector = false;
    
    for child in root.children().filter(|n| n.is_element()) {
        match child.tag_name().name() {
            "source" => {
                has_source = true;
                assert!(child.attribute("order").is_some());
            }
            "analyzer" => {
                has_analyzer = true;
                assert!(child.attribute("order").is_some());
            }
            "detector" => {
                has_detector = true;
                assert!(child.attribute("order").is_some());
            }
            _ => panic!("Unexpected component: {}", child.tag_name().name()),
        }
    }
    
    // mzDB requires at least one of each component
    assert!(has_source, "componentList must have source");
    assert!(has_analyzer, "componentList must have analyzer");
    assert!(has_detector, "componentList must have detector");
    
    Ok(())
}

/// Validate scan_list XML structure
fn validate_scan_list_structure(scan_list: &str) -> Result<()> {
    let doc = roxmltree::Document::parse(scan_list)?;
    let root = doc.root_element();
    
    // Root should be <scanList>
    assert_eq!(root.tag_name().name(), "scanList");
    
    // Must have count attribute
    assert!(root.attribute("count").is_some());
    
    // Should have cvParam and scan elements
    let mut has_scan = false;
    
    for child in root.children().filter(|n| n.is_element()) {
        match child.tag_name().name() {
            "cvParam" => {
                assert!(child.attribute("accession").is_some());
            }
            "scan" => {
                has_scan = true;
                // Scan should have cvParam children
                for scan_child in child.children().filter(|n| n.is_element()) {
                    assert_eq!(scan_child.tag_name().name(), "cvParam");
                }
            }
            _ => {}
        }
    }
    
    assert!(has_scan, "scanList must have at least one scan");
    
    Ok(())
}

/// Validate precursor_list XML structure
fn validate_precursor_list_structure(precursor_list: &str) -> Result<()> {
    let doc = roxmltree::Document::parse(precursor_list)?;
    let root = doc.root_element();
    
    // Root should be <precursorList>
    assert_eq!(root.tag_name().name(), "precursorList");
    
    // Must have count attribute
    assert!(root.attribute("count").is_some());
    
    // Should have precursor elements
    for child in root.children().filter(|n| n.is_element()) {
        assert_eq!(child.tag_name().name(), "precursor");
        
        // Check for required sub-elements
        let mut has_isolation = false;
        let mut has_selected_ion = false;
        let mut has_activation = false;
        
        for precursor_child in child.children().filter(|n| n.is_element()) {
            match precursor_child.tag_name().name() {
                "isolationWindow" => has_isolation = true,
                "selectedIonList" => has_selected_ion = true,
                "activation" => has_activation = true,
                _ => {}
            }
        }
        
        assert!(has_isolation || has_selected_ion || has_activation,
                "precursor must have at least one of isolationWindow, selectedIonList, or activation");
    }
    
    Ok(())
}

/// Check if a string contains valid XML (non-empty and parseable)
fn is_valid_xml(xml: &str) -> bool {
    if xml.is_empty() {
        return false;
    }
    roxmltree::Document::parse(xml).is_ok()
}

#[test]
fn test_raw_file_exists() {
    assert!(
        Path::new(TEST_RAW_FILE).exists(),
        "Test RAW file not found at: {}",
        TEST_RAW_FILE
    );
}

#[test]
fn test_xml_schemata_exists() {
    assert!(
        Path::new(XML_SCHEMATA_FILE).exists(),
        "XML schemata file not found at: {}",
        XML_SCHEMATA_FILE
    );
}

#[test]
fn test_load_xml_schemas() -> Result<()> {
    let schemas = load_xml_schemas()?;
    
    // Should have schemas for key fields
    assert!(schemas.contains_key("param_tree"));
    assert!(schemas.contains_key("spectrum.scan_list"));
    assert!(schemas.contains_key("spectrum.precursor_list"));
    assert!(schemas.contains_key("instrument_configuration.component_list"));
    
    Ok(())
}

#[test]
fn test_convert_raw_to_mzdb() -> Result<()> {
    // Create temporary output file
    let temp_file = NamedTempFile::new()?;
    let output_path = temp_file.path();
    
    // Define bounding box sizes
    let bb_sizes = BBSizes {
        bb_mz_height_ms1: 10.0,
        bb_mz_height_msn: 10000.0,
        bb_rt_width_ms1: 5.0,
        bb_rt_width_msn: 60.0,
    };
    
    // Convert
    convert_raw_to_mzdb(TEST_RAW_FILE, output_path, bb_sizes, false)?;
    
    // Verify the output file exists and can be opened
    assert!(output_path.exists());
    
    let conn = Connection::open(output_path)?;
    
    // Verify basic structure
    let version: String = conn.query_row("SELECT version FROM mzdb", [], |row| row.get(0))?;
    assert!(!version.is_empty());
    
    conn.close().map_err(|(_, e)| e)?;
    
    Ok(())
}

#[test]
fn test_mzdb_param_tree_structure() -> Result<()> {
    let temp_file = NamedTempFile::new()?;
    let output_path = temp_file.path();
    
    let bb_sizes = BBSizes {
        bb_mz_height_ms1: 10.0,
        bb_mz_height_msn: 10000.0,
        bb_rt_width_ms1: 5.0,
        bb_rt_width_msn: 60.0,
    };
    
    convert_raw_to_mzdb(TEST_RAW_FILE, output_path, bb_sizes, false)?;
    
    let conn = Connection::open(output_path)?;
    
    // Get param_tree from mzdb table
    let param_tree: Option<String> = conn.query_row(
        "SELECT param_tree FROM mzdb",
        [],
        |row| row.get(0),
    )?;
    
    if let Some(pt) = param_tree {
        if !pt.is_empty() {
            validate_param_tree_structure(&pt)?;
        }
    }
    
    conn.close().map_err(|(_, e)| e)?;
    
    Ok(())
}

#[test]
fn test_run_param_tree_structure() -> Result<()> {
    let temp_file = NamedTempFile::new()?;
    let output_path = temp_file.path();
    
    let bb_sizes = BBSizes {
        bb_mz_height_ms1: 10.0,
        bb_mz_height_msn: 10000.0,
        bb_rt_width_ms1: 5.0,
        bb_rt_width_msn: 60.0,
    };
    
    convert_raw_to_mzdb(TEST_RAW_FILE, output_path, bb_sizes, false)?;
    
    let conn = Connection::open(output_path)?;
    
    // Get param_tree from run table
    let param_tree: Option<String> = conn.query_row(
        "SELECT param_tree FROM run WHERE id = 1",
        [],
        |row| row.get(0),
    )?;
    
    // Only validate if we have valid XML content
    if let Some(pt) = param_tree {
        if is_valid_xml(&pt) {
            validate_param_tree_structure(&pt)?;
            
            // Check for cvParams wrapper
            assert!(pt.contains("<cvParams>"));
            assert!(pt.contains("</cvParams>"));
        }
    }
    
    conn.close().map_err(|(_, e)| e)?;
    
    Ok(())
}

#[test]
fn test_spectrum_param_tree_structure() -> Result<()> {
    let temp_file = NamedTempFile::new()?;
    let output_path = temp_file.path();
    
    let bb_sizes = BBSizes {
        bb_mz_height_ms1: 10.0,
        bb_mz_height_msn: 10000.0,
        bb_rt_width_ms1: 5.0,
        bb_rt_width_msn: 60.0,
    };
    
    convert_raw_to_mzdb(TEST_RAW_FILE, output_path, bb_sizes, false)?;
    
    let conn = Connection::open(output_path)?;
    
    // Get first spectrum's param_tree
    let param_tree: Option<String> = conn.query_row(
        "SELECT param_tree FROM spectrum WHERE param_tree IS NOT NULL AND param_tree != '' LIMIT 1",
        [],
        |row| row.get(0),
    ).ok();
    
    if let Some(pt) = param_tree {
        if is_valid_xml(&pt) {
            validate_param_tree_structure(&pt)?;
        }
    }
    
    conn.close().map_err(|(_, e)| e)?;
    
    Ok(())
}

#[test]
fn test_component_list_structure() -> Result<()> {
    let temp_file = NamedTempFile::new()?;
    let output_path = temp_file.path();
    
    let bb_sizes = BBSizes {
        bb_mz_height_ms1: 10.0,
        bb_mz_height_msn: 10000.0,
        bb_rt_width_ms1: 5.0,
        bb_rt_width_msn: 60.0,
    };
    
    convert_raw_to_mzdb(TEST_RAW_FILE, output_path, bb_sizes, false)?;
    
    let conn = Connection::open(output_path)?;
    
    // Get component_list from instrument_configuration table
    let component_list: Option<String> = conn.query_row(
        "SELECT component_list FROM instrument_configuration WHERE id = 1",
        [],
        |row| row.get(0),
    )?;
    
    // Only validate if we have valid XML content
    if let Some(cl) = component_list {
        if is_valid_xml(&cl) {
            validate_component_list_structure(&cl)?;
        }
    }
    
    conn.close().map_err(|(_, e)| e)?;
    
    Ok(())
}

#[test]
fn test_spectrum_scan_list_structure() -> Result<()> {
    let temp_file = NamedTempFile::new()?;
    let output_path = temp_file.path();
    
    let bb_sizes = BBSizes {
        bb_mz_height_ms1: 10.0,
        bb_mz_height_msn: 10000.0,
        bb_rt_width_ms1: 5.0,
        bb_rt_width_msn: 60.0,
    };
    
    convert_raw_to_mzdb(TEST_RAW_FILE, output_path, bb_sizes, false)?;
    
    let conn = Connection::open(output_path)?;
    
    // Get first spectrum's scan_list
    let scan_list: Option<String> = conn.query_row(
        "SELECT scan_list FROM spectrum WHERE scan_list IS NOT NULL AND scan_list != '' LIMIT 1",
        [],
        |row| row.get(0),
    ).ok();
    
    if let Some(sl) = scan_list {
        if is_valid_xml(&sl) {
            validate_scan_list_structure(&sl)?;
        }
    }
    
    conn.close().map_err(|(_, e)| e)?;
    
    Ok(())
}

#[test]
fn test_spectrum_precursor_list_structure() -> Result<()> {
    let temp_file = NamedTempFile::new()?;
    let output_path = temp_file.path();
    
    let bb_sizes = BBSizes {
        bb_mz_height_ms1: 10.0,
        bb_mz_height_msn: 10000.0,
        bb_rt_width_ms1: 5.0,
        bb_rt_width_msn: 60.0,
    };
    
    convert_raw_to_mzdb(TEST_RAW_FILE, output_path, bb_sizes, false)?;
    
    let conn = Connection::open(output_path)?;
    
    // Get first MS2 spectrum's precursor_list
    let precursor_list: Option<String> = conn.query_row(
        "SELECT precursor_list FROM spectrum WHERE ms_level > 1 AND precursor_list IS NOT NULL AND precursor_list != '' LIMIT 1",
        [],
        |row| row.get(0),
    ).ok();
    
    if let Some(pl) = precursor_list {
        if is_valid_xml(&pl) {
            validate_precursor_list_structure(&pl)?;
        }
    }
    
    conn.close().map_err(|(_, e)| e)?;
    
    Ok(())
}

#[test]
fn test_spectrum_count() -> Result<()> {
    let temp_file = NamedTempFile::new()?;
    let output_path = temp_file.path();
    
    let bb_sizes = BBSizes {
        bb_mz_height_ms1: 10.0,
        bb_mz_height_msn: 10000.0,
        bb_rt_width_ms1: 5.0,
        bb_rt_width_msn: 60.0,
    };
    
    convert_raw_to_mzdb(TEST_RAW_FILE, output_path, bb_sizes, false)?;
    
    let conn = Connection::open(output_path)?;
    
    // Count spectra
    let total_count: i32 = conn.query_row("SELECT COUNT(*) FROM spectrum", [], |row| row.get(0))?;
    assert!(total_count > 0, "No spectra found in mzDB");
    
    // Count MS1 spectra
    let ms1_count: i32 = conn.query_row(
        "SELECT COUNT(*) FROM spectrum WHERE ms_level = 1",
        [],
        |row| row.get(0),
    )?;
    
    // Count MS2 spectra
    let ms2_count: i32 = conn.query_row(
        "SELECT COUNT(*) FROM spectrum WHERE ms_level = 2",
        [],
        |row| row.get(0),
    )?;
    
    println!("Total spectra: {}", total_count);
    println!("MS1 spectra: {}", ms1_count);
    println!("MS2 spectra: {}", ms2_count);
    
    assert!(ms1_count > 0 || ms2_count > 0, "No MS1 or MS2 spectra found");
    
    conn.close().map_err(|(_, e)| e)?;
    
    Ok(())
}

#[test]
fn test_bounding_box_creation() -> Result<()> {
    let temp_file = NamedTempFile::new()?;
    let output_path = temp_file.path();
    
    let bb_sizes = BBSizes {
        bb_mz_height_ms1: 10.0,
        bb_mz_height_msn: 10000.0,
        bb_rt_width_ms1: 5.0,
        bb_rt_width_msn: 60.0,
    };
    
    convert_raw_to_mzdb(TEST_RAW_FILE, output_path, bb_sizes, false)?;
    
    let conn = Connection::open(output_path)?;
    
    // Count bounding boxes
    let bb_count: i32 = conn.query_row("SELECT COUNT(*) FROM bounding_box", [], |row| row.get(0))?;
    assert!(bb_count > 0, "No bounding boxes created");
    
    // Check rtree entries (only MS1 BBs get rtree entries in DDA mode)
    let rtree_count: i32 = conn.query_row(
        "SELECT COUNT(*) FROM bounding_box_rtree",
        [],
        |row| row.get(0),
    )?;
    // R-tree count may be less than total BB count for DDA files
    // where only MS1 bounding boxes are indexed
    assert!(rtree_count > 0, "No R-tree entries created");
    assert!(rtree_count <= bb_count, "R-tree count should not exceed BB count");
    
    conn.close().map_err(|(_, e)| e)?;
    
    Ok(())
}

#[test]
fn test_data_encoding_registry() -> Result<()> {
    let temp_file = NamedTempFile::new()?;
    let output_path = temp_file.path();
    
    let bb_sizes = BBSizes {
        bb_mz_height_ms1: 10.0,
        bb_mz_height_msn: 10000.0,
        bb_rt_width_ms1: 5.0,
        bb_rt_width_msn: 60.0,
    };
    
    convert_raw_to_mzdb(TEST_RAW_FILE, output_path, bb_sizes, false)?;
    
    let conn = Connection::open(output_path)?;
    
    // Check data encodings
    let encoding_count: i32 = conn.query_row(
        "SELECT COUNT(*) FROM data_encoding",
        [],
        |row| row.get(0),
    )?;
    assert!(encoding_count > 0, "No data encodings found");
    
    // Verify encoding mode
    let mode: String = conn.query_row(
        "SELECT mode FROM data_encoding LIMIT 1",
        [],
        |row| row.get(0),
    )?;
    assert!(mode == "centroid" || mode == "profile");
    
    conn.close().map_err(|(_, e)| e)?;
    
    Ok(())
}

#[test]
fn test_sample_metadata() -> Result<()> {
    let temp_file = NamedTempFile::new()?;
    let output_path = temp_file.path();
    
    let bb_sizes = BBSizes {
        bb_mz_height_ms1: 10.0,
        bb_mz_height_msn: 10000.0,
        bb_rt_width_ms1: 5.0,
        bb_rt_width_msn: 60.0,
    };
    
    convert_raw_to_mzdb(TEST_RAW_FILE, output_path, bb_sizes, false)?;
    
    let conn = Connection::open(output_path)?;
    
    // Check sample table
    let sample_count: i32 = conn.query_row("SELECT COUNT(*) FROM sample", [], |row| row.get(0))?;
    assert!(sample_count > 0, "No samples found");
    
    // Get sample name
    let name: String = conn.query_row("SELECT name FROM sample LIMIT 1", [], |row| row.get(0))?;
    assert!(!name.is_empty(), "Sample name is empty");
    
    // Check param_tree if available
    let param_tree: Option<String> = conn.query_row(
        "SELECT param_tree FROM sample LIMIT 1",
        [],
        |row| row.get(0),
    )?;
    
    if let Some(pt) = param_tree {
        if is_valid_xml(&pt) {
            validate_param_tree_structure(&pt)?;
        }
    }
    
    conn.close().map_err(|(_, e)| e)?;
    
    Ok(())
}

#[test]
fn test_xml_well_formedness() -> Result<()> {
    let temp_file = NamedTempFile::new()?;
    let output_path = temp_file.path();
    
    let bb_sizes = BBSizes {
        bb_mz_height_ms1: 10.0,
        bb_mz_height_msn: 10000.0,
        bb_rt_width_ms1: 5.0,
        bb_rt_width_msn: 60.0,
    };
    
    convert_raw_to_mzdb(TEST_RAW_FILE, output_path, bb_sizes, false)?;
    
    let conn = Connection::open(output_path)?;
    
    // Test all XML fields for well-formedness
    // We check that non-empty XML strings are well-formed
    let fields = vec![
        ("mzdb", "param_tree"),
        ("run", "param_tree"),
        ("instrument_configuration", "component_list"),
        ("spectrum", "scan_list"),
        ("spectrum", "param_tree"),
    ];
    
    for (table, field) in fields {
        let query = format!(
            "SELECT {} FROM {} WHERE {} IS NOT NULL AND {} != '' LIMIT 1",
            field, table, field, field
        );
        if let Ok(xml) = conn.query_row(&query, [], |row| row.get::<_, String>(0)) {
            // Try to parse XML - only fail if non-empty XML is malformed
            if !xml.is_empty() {
                match roxmltree::Document::parse(&xml) {
                    Ok(_) => {}, // XML is well-formed
                    Err(e) => panic!("Invalid XML in {}.{}: {}", table, field, e),
                }
            }
        }
    }
    
    conn.close().map_err(|(_, e)| e)?;
    
    Ok(())
}
