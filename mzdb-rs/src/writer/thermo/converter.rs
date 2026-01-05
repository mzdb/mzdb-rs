//! RAW to mzDB Conversion Logic

use anyhow::{Context, Result};
use std::path::Path;
use thernio::raw::RawFile;

use crate::metadata::*;
use crate::model::*;
use crate::writer::{MzDbWriterBuilder, WriterMetadata};

use super::xml_builder::*;

/// Statistics collected during conversion
#[derive(Debug, Default)]
struct ConversionStats {
    ms1_count: usize,
    ms2_count: usize,
    ms3_count: usize,
    min_rt: f64,
    max_rt: f64,
    min_precursor_mz: f64,
    max_precursor_mz: f64,
    activation_types: std::collections::HashSet<String>,
}

impl ConversionStats {
    fn new() -> Self {
        Self {
            min_rt: f64::MAX,
            max_rt: f64::MIN,
            min_precursor_mz: f64::MAX,
            max_precursor_mz: f64::MIN,
            ..Default::default()
        }
    }
    
    fn update_from_scan(&mut self, scan: &thernio::raw::Scan, activation_type: &str) {
        // Update MS level counts
        match scan.ms_level {
            1 => self.ms1_count += 1,
            2 => self.ms2_count += 1,
            3 => self.ms3_count += 1,
            _ => {}
        }
        
        // Update RT range
        if scan.retention_time < self.min_rt {
            self.min_rt = scan.retention_time;
        }
        if scan.retention_time > self.max_rt {
            self.max_rt = scan.retention_time;
        }
        
        // Update precursor m/z range for MS2+
        if scan.ms_level > 1 {
            for &mz in &scan.precursor_mzs {
                if mz < self.min_precursor_mz {
                    self.min_precursor_mz = mz;
                }
                if mz > self.max_precursor_mz {
                    self.max_precursor_mz = mz;
                }
            }
            
            // Track activation types
            if !activation_type.is_empty() {
                self.activation_types.insert(activation_type.to_string());
            }
        }
    }
}

/// Convert a Thermo RAW file to mzDB format
///
/// # Arguments
///
/// * `raw_path` - Path to the input .raw file
/// * `mzdb_path` - Path to the output .mzDB file  
/// * `bb_sizes` - Bounding box dimensions for spatial partitioning
/// * `is_dia` - Whether this is DIA/SWATH data
///
/// # Example
///
/// ```no_run
/// use mzdb::writer::thermo::convert_raw_to_mzdb;
/// use mzdb::BBSizes;
///
/// let bb_sizes = BBSizes {
///     bb_mz_height_ms1: 10.0,
///     bb_mz_height_msn: 10000.0,
///     bb_rt_width_ms1: 5.0,
///     bb_rt_width_msn: 60.0,
/// };
///
/// convert_raw_to_mzdb(
///     "input.raw",
///     "output.mzDB",
///     bb_sizes,
///     false,
/// ).unwrap();
/// ```
pub fn convert_raw_to_mzdb(
    raw_path: impl AsRef<Path>,
    mzdb_path: impl AsRef<Path>,
    bb_sizes: BBSizes,
    is_dia: bool,
) -> Result<()> {
    let raw_path = raw_path.as_ref();
    let mzdb_path = mzdb_path.as_ref();
    
    // Open RAW file
    let mut raw = RawFile::open(raw_path)
        .with_context(|| format!("Failed to open RAW file: {}", raw_path.display()))?;
    
    println!("Opened RAW file: {}", raw_path.display());
    println!("  Version: {}", raw.version());
    println!("  Model: {}", raw.model());
    println!("  Scans: {}", raw.num_scans());
    
    // Display embedded method info if available
    if let Some(method) = raw.embedded_method() {
        println!("  Embedded method:");
        println!("    Size: {} bytes", method.size);
        if !method.instruments.is_empty() {
            println!("    Instruments: {}", method.instruments.join(", "));
        }
        if method.text_content.is_some() {
            println!("    Has text content: yes");
        }
        if method.xml_content.is_some() {
            println!("    Has XML content: yes");
        }
    }
    
    // Display autosampler info if available
    let autosampler = raw.autosampler_info();
    if autosampler.vials_per_tray > 0 {
        println!("  Autosampler:");
        if !autosampler.tray_name.is_empty() {
            println!("    Tray: {}", autosampler.tray_name);
        }
        println!("    Position: {}:{} ({}x{} tray)", 
                 autosampler.tray_index, 
                 autosampler.vial_index,
                 autosampler.vials_per_tray_x,
                 autosampler.vials_per_tray_y);
    }
    
    // Display sample info
    let seq_row = raw.sequencer_row();
    println!("  Sample:");
    println!("    ID: {}", seq_row.sample_id);
    if !seq_row.comment.is_empty() {
        println!("    Comment: {}", seq_row.comment);
    }
    if seq_row.injection.injection_volume > 0.0 {
        println!("    Injection volume: {:.2} µL", seq_row.injection.injection_volume);
    }
    if !seq_row.vial.is_empty() {
        println!("    Vial: {}", seq_row.vial);
    }
    
    // First pass: collect statistics from all scans
    let mut stats = ConversionStats::new();
    let num_scans = raw.num_scans();
    
    // Clone scan events to avoid borrow conflict with raw.scan()
    let scan_events: Vec<_> = raw.scan_events().to_vec();
    
    for scan_num in 1..=num_scans {
        let scan = raw.scan(scan_num)
            .with_context(|| format!("Failed to read scan {} for statistics", scan_num))?;
        
        // Get activation type from scan event (0-indexed)
        let activation_type = if scan.ms_level > 1 && scan_num <= scan_events.len() {
            activation_type_to_string(scan_events[scan_num - 1].activation)
        } else {
            ""
        };
        
        // Update statistics
        stats.update_from_scan(&scan, activation_type);
    }
    
    // Build metadata from RAW file (returns metadata + method texts)
    let (metadata, ms_method_text, _lc_method_text) = build_metadata(&mut raw, &stats)?;
    
    // Build mzdb param_tree with legacy instrumentMethods for backward compatibility
    let mzdb_param_tree = if ms_method_text.is_some() {
        build_mzdb_user_texts(ms_method_text.as_deref())?
    } else {
        String::new()
    };
    
    // Create writer
    let mut writer = MzDbWriterBuilder::new(mzdb_path)
        .metadata(metadata)
        .bb_sizes(bb_sizes)
        .is_dia(is_dia)
        .mzdb_param_tree(mzdb_param_tree)
        .build()?;
    
    writer.open()
        .context("Failed to open mzDB writer")?;
    
    // Convert all scans (scan_events already retrieved above)
    let mut current_cycle: i64 = 0;
    let mut last_ms_level: u8 = 0;
    
    for scan_num in 1..=num_scans {
        if scan_num % 100 == 0 {
            println!("  Processing scan {}/{}", scan_num, num_scans);
        }
        
        let scan = raw.scan(scan_num)
            .with_context(|| format!("Failed to read scan {}", scan_num))?;
        
        // Track cycle number: increment when we see an MS1 scan
        // (a cycle is typically MS1 followed by all its MS2/MS3 scans)
        if scan.ms_level == 1 && (last_ms_level != 1 || scan_num == 1) {
            current_cycle += 1;
        }
        last_ms_level = scan.ms_level;
        
        // Get scan event for this scan (0-indexed)
        let scan_event = if scan_num <= scan_events.len() {
            Some(&scan_events[scan_num - 1])
        } else {
            None
        };
        
        // Convert to mzDB spectrum
        let spectrum = convert_scan_to_spectrum(scan_num, &scan, scan_event, current_cycle, &raw)?;
        
        // Determine data encoding based on scan mode
        let mode = if let Some(event) = scan_event {
            match event.scan_mode {
                thernio::raw::ScanMode::Centroid => DataMode::Centroid,
                thernio::raw::ScanMode::Profile => DataMode::Profile,
            }
        } else {
            DataMode::Centroid
        };
        
        let encoding = DataEncoding {
            id: 0, // Will be assigned by registry
            mode,
            peak_encoding: PeakEncoding::HighRes, // 64-bit m/z precision
            compression: "none".to_string(),
            byte_order: ByteOrder::LittleEndian,
        };
        
        // Insert spectrum
        writer.insert_spectrum(&spectrum, &encoding)
            .with_context(|| format!("Failed to insert spectrum {}", scan_num))?;
    }
    
    println!("Finalizing mzDB file...");
    println!("  MS1 spectra: {}", stats.ms1_count);
    println!("  MS2 spectra: {}", stats.ms2_count);
    if stats.ms3_count > 0 {
        println!("  MS3 spectra: {}", stats.ms3_count);
    }
    println!("  RT range: {:.2} - {:.2} min", stats.min_rt, stats.max_rt);
    if stats.max_precursor_mz > 0.0 {
        println!("  Precursor m/z range: {:.2} - {:.2}", stats.min_precursor_mz, stats.max_precursor_mz);
    }
    
    // Report method text extraction
    if let Some(method) = raw.embedded_method() {
        let (ms_text, lc_text) = extract_method_texts(method);
        if ms_text.is_some() || lc_text.is_some() {
            println!("  Method texts extracted:");
            if let Some(ms) = ms_text {
                println!("    MS instrument method: {} chars", ms.len());
            }
            if let Some(lc) = lc_text {
                println!("    LC method: {} chars", lc.len());
            }
        }
    }
    
    writer.close()
        .context("Failed to close mzDB writer")?;
    
    println!("Conversion complete: {}", mzdb_path.display());
    
    Ok(())
}

/// Extract MS instrument and LC method text from embedded method
fn extract_method_texts(method: &thernio::raw::EmbeddedMethod) -> (Option<String>, Option<String>) {
    let parsed = match &method.parsed {
        Some(p) => p,
        None => return (None, None),
    };
    
    let mut ms_text: Option<String> = None;
    let mut lc_text: Option<String> = None;
    
    for inst in &parsed.instruments {
        let name_lower = inst.instrument_name.to_lowercase();
        
        // MS instrument patterns (LTQ, FTMS, TNG-Merkur, SiiXcalibur, etc.)
        if name_lower.contains("ltq") 
            || name_lower.contains("ftms")
            || name_lower.contains("tng")
            || name_lower.contains("merkur")
            || name_lower.contains("xcalibur")
            || name_lower.contains("orbitrap")
            || name_lower.contains("exactive")
            || name_lower.contains("exploris")
            || name_lower.contains("fusion")
            || name_lower.contains("lumos")
        {
            // Take the first/longest MS instrument text
            if ms_text.is_none() || inst.text_content.len() > ms_text.as_ref().unwrap().len() {
                if !inst.text_content.trim().is_empty() {
                    ms_text = Some(inst.text_content.clone());
                }
            }
        }
        
        // LC method patterns (Surveyor, pump, autosampler, etc.)
        if name_lower.contains("surveyor")
            || name_lower.contains("pump")
            || name_lower.contains("autosampler")
            || name_lower.contains("as ")
            || name_lower.contains("micro as")
            || name_lower.contains("accela")
            || name_lower.contains("vanquish")
            || name_lower.contains("ultimate")
        {
            // Combine all LC-related texts
            if !inst.text_content.trim().is_empty() {
                let current = lc_text.get_or_insert_with(String::new);
                if !current.is_empty() {
                    current.push_str("\n\n");
                }
                current.push_str(&format!("=== {} ===\n", inst.instrument_name));
                current.push_str(&inst.text_content);
            }
        }
    }
    
    (ms_text, lc_text)
}

/// Build user texts XML section for run param_tree (ms_method and lc_method)
fn build_run_user_texts(ms_method: Option<&str>, lc_method: Option<&str>) -> Result<String> {
    let mut user_texts = xmltree::Element::new("userTexts");
    
    if let Some(ms_text) = ms_method {
        let mut user_text = xmltree::Element::new("userText");
        user_text.attributes.insert("cvRef".to_string(), "MS".to_string());
        user_text.attributes.insert("accession".to_string(), "MS:-1".to_string());
        user_text.attributes.insert("name".to_string(), "ms_method".to_string());
        user_text.attributes.insert("type".to_string(), "xsd:string".to_string());
        user_text.children.push(xmltree::XMLNode::Text(ms_text.to_string()));
        user_texts.children.push(xmltree::XMLNode::Element(user_text));
    }
    
    if let Some(lc_text) = lc_method {
        let mut user_text = xmltree::Element::new("userText");
        user_text.attributes.insert("cvRef".to_string(), "MS".to_string());
        user_text.attributes.insert("accession".to_string(), "MS:-1".to_string());
        user_text.attributes.insert("name".to_string(), "lc_method".to_string());
        user_text.attributes.insert("type".to_string(), "xsd:string".to_string());
        user_text.children.push(xmltree::XMLNode::Text(lc_text.to_string()));
        user_texts.children.push(xmltree::XMLNode::Element(user_text));
    }
    
    let mut output = Vec::new();
    // Use write_with_config to avoid XML declaration
    let config = xmltree::EmitterConfig::new()
        .write_document_declaration(false);
    user_texts.write_with_config(&mut output, config)?;
    Ok(String::from_utf8(output)?)
}

/// Build user texts XML for mzdb table (legacy instrumentMethods for backward compatibility)
fn build_mzdb_user_texts(ms_method: Option<&str>) -> Result<String> {
    if ms_method.is_none() {
        return Ok(String::new());
    }
    
    let mut user_texts = xmltree::Element::new("userTexts");
    
    if let Some(ms_text) = ms_method {
        let mut user_text = xmltree::Element::new("userText");
        user_text.attributes.insert("cvRef".to_string(), "MS".to_string());
        user_text.attributes.insert("accession".to_string(), "MS:-1".to_string());
        user_text.attributes.insert("name".to_string(), "instrumentMethods".to_string());
        user_text.attributes.insert("type".to_string(), "xsd:string".to_string());
        user_text.children.push(xmltree::XMLNode::Text(ms_text.to_string()));
        user_texts.children.push(xmltree::XMLNode::Element(user_text));
    }
    
    let mut output = Vec::new();
    // Use write_inner to avoid XML declaration
    let config = xmltree::EmitterConfig::new()
        .write_document_declaration(false);
    user_texts.write_with_config(&mut output, config)?;
    Ok(String::from_utf8(output)?)
}

/// Convert Windows FILETIME to ISO-8601 string
fn filetime_to_iso8601(filetime: u64) -> String {
    // FILETIME is 100-nanosecond intervals since January 1, 1601 UTC
    const FILETIME_EPOCH_DIFF: u64 = 11644473600; // seconds between 1601 and 1970
    
    let seconds_since_1601 = filetime / 10_000_000;
    if seconds_since_1601 < FILETIME_EPOCH_DIFF {
        return String::new();
    }
    
    let unix_timestamp = seconds_since_1601 - FILETIME_EPOCH_DIFF;
    
    // Simple ISO-8601 formatting (basic version without full datetime library)
    let seconds_in_day = 86400;
    let days = unix_timestamp / seconds_in_day;
    let remaining_seconds = unix_timestamp % seconds_in_day;
    
    let hours = remaining_seconds / 3600;
    let minutes = (remaining_seconds % 3600) / 60;
    let seconds = remaining_seconds % 60;
    
    // Simple date calculation (approximate, good enough for display)
    let year = 1970 + (days / 365) as i32;
    let day_of_year = days % 365;
    let month = (day_of_year / 30) + 1;
    let day = (day_of_year % 30) + 1;
    
    format!("{:04}-{:02}-{:02}T{:02}:{:02}:{:02}Z", 
            year, month, day, hours, minutes, seconds)
}

/// Build WriterMetadata from RAW file information
/// Returns (metadata, ms_method_text, lc_method_text)
fn build_metadata(raw: &mut RawFile, stats: &ConversionStats) -> Result<(WriterMetadata, Option<String>, Option<String>)> {
    // First, build the component list which requires mutable borrow
    let component_list = build_component_list(raw)?;
    
    // Now get the other data (immutable borrows)
    let seq_row = raw.sequencer_row();
    let header = raw.header();
    let autosampler = raw.autosampler_info();
    let (low_mz, high_mz) = raw.mz_range();
    let model_name = raw.model().to_string();
    
    // Convert creation date
    let creation_date = filetime_to_iso8601(header.audit_start.time);
    
    // Pre-declare all formatted strings to ensure they live long enough
    let version_str = header.version.to_string();
    let sample_type_str = format!("{:?}", seq_row.injection.sample_type);
    let vol_str = format!("{:.2}", seq_row.injection.injection_volume);
    let weight_str = format!("{:.3}", seq_row.injection.sample_weight);
    let sample_vol_str = format!("{:.2}", seq_row.injection.sample_volume);
    let dilution_str = format!("{:.3}", seq_row.injection.dilution_factor);
    let tray_str = format!("{}:{}", autosampler.tray_index, autosampler.vial_index);
    let start_time_str = format!("{:.2}", raw.start_time());
    let _end_time_str = format!("{:.2}", raw.end_time());
    let _low_mz_str = format!("{:.4}", low_mz);
    let _high_mz_str = format!("{:.4}", high_mz);
    let ms1_str = stats.ms1_count.to_string();
    let ms2_str = stats.ms2_count.to_string();
    let ms3_str = stats.ms3_count.to_string();
    let min_rt_str = format!("{:.4}", stats.min_rt);
    let max_rt_str = format!("{:.4}", stats.max_rt);
    let min_mz_str = format!("{:.4}", stats.min_precursor_mz);
    let max_mz_str = format!("{:.4}", stats.max_precursor_mz);
    
    // Software - include embedded method info if available
    let mut software_params = Vec::new();
    let instruments_str: String;
    if let Some(method) = raw.embedded_method() {
        if !method.instruments.is_empty() {
            instruments_str = method.instruments.join(", ");
            software_params.push(("MS", "MS:1000531", "software", instruments_str.as_str()));
        }
    }
    
    let software = Software {
        id: 1,
        name: "any2mzdb".to_string(),
        version: "0.9.10".to_string(), // Matching pwiz-mzdb version for compatibility
        param_tree: if !software_params.is_empty() {
            build_param_tree(&software_params)?
        } else {
            "".to_string()
        },
        shared_param_tree_id: None,
    };
    
    // Instrument configuration with more detailed param tree
    let mut inst_params = vec![
        ("MS", "MS:1000494", "Thermo Scientific instrument model", model_name.as_str()),
    ];
    
    // Add file version
    if header.version > 0 {
        inst_params.push(("MS", "MS:1000569", "RAW file version", version_str.as_str()));
    }
    
    // Add instrument method path if available
    if !seq_row.instrument_method.is_empty() {
        inst_params.push(("MS", "MS:1000004", "instrument method", &seq_row.instrument_method));
    }
    
    let inst_param_tree = build_param_tree(&inst_params)?;
    
    let inst_config = InstrumentConfiguration {
        id: 1,
        name: model_name.clone(),
        param_tree: Some(inst_param_tree),
        component_list,
        shared_param_tree_id: None,
        software_id: 1,
    };
    
    // Sample with comprehensive details
    let mut sample_params = vec![
        ("MS", "MS:1000002", "sample name", seq_row.sample_id.as_str()),
    ];
    
    // Add sample type
    if !sample_type_str.is_empty() && sample_type_str != "Unknown" {
        sample_params.push(("MS", "MS:1000001", "sample type", sample_type_str.as_str()));
    }
    
    // Add comment if available
    if !seq_row.comment.is_empty() {
        sample_params.push(("MS", "MS:1000003", "sample comment", &seq_row.comment));
    }
    
    // Add injection volume
    if seq_row.injection.injection_volume > 0.0 {
        sample_params.push(("MS", "MS:1000005", "injection volume", vol_str.as_str()));
    }
    
    // Add sample weight
    if seq_row.injection.sample_weight > 0.0 {
        sample_params.push(("MS", "MS:1000006", "sample weight", weight_str.as_str()));
    }
    
    // Add sample volume
    if seq_row.injection.sample_volume > 0.0 {
        sample_params.push(("MS", "MS:1000007", "sample volume", sample_vol_str.as_str()));
    }
    
    // Add dilution factor
    if seq_row.injection.dilution_factor > 0.0 && seq_row.injection.dilution_factor != 1.0 {
        sample_params.push(("MS", "MS:1000008", "dilution factor", dilution_str.as_str()));
    }
    
    // Add vial position
    if !seq_row.vial.is_empty() {
        sample_params.push(("MS", "MS:1000009", "vial position", &seq_row.vial));
    }
    
    // Add autosampler tray info
    if autosampler.vials_per_tray > 0 {
        sample_params.push(("MS", "MS:1000010", "autosampler position", tray_str.as_str()));
    }
    
    if !autosampler.tray_name.is_empty() {
        sample_params.push(("MS", "MS:1000011", "autosampler tray", &autosampler.tray_name));
    }
    
    let sample_param_tree = build_param_tree(&sample_params)?;
    
    let sample = Sample {
        id: 1,
        name: seq_row.sample_id.clone(),
        param_tree: Some(sample_param_tree),
        shared_param_tree_id: None,
    };
    
    // Source file with creation date, version, and path
    let mut source_params = vec![
        ("MS", "MS:1000768", "Thermo nativeID format", ""),
        ("MS", "MS:1000563", "Thermo RAW format", ""),
    ];
    
    if !creation_date.is_empty() {
        source_params.push(("MS", "MS:1000747", "creation date", &creation_date));
    }
    
    // Add original path if available
    if !seq_row.path.is_empty() {
        source_params.push(("MS", "MS:1000569", "file path", &seq_row.path));
    }
    
    let source_param_tree = build_param_tree(&source_params)?;
    
    let source_file = SourceFile {
        id: 1,
        name: seq_row.file_name.clone(),
        location: seq_row.path.clone(),
        param_tree: source_param_tree,
        shared_param_tree_id: None,
    };
    
    // Data processing - conversion to mzDB with processing method if available
    let data_processing = DataProcessing {
        id: 1,
        name: "conversion_to_mzDB".to_string(),
    };
    
    // Processing method
    let mut proc_params = vec![
        ("MS", "MS:1000544", "Conversion to mzML", ""),
    ];
    
    // Add processing method path if available
    if !seq_row.processing_method.is_empty() {
        proc_params.push(("MS", "MS:1000530", "processing method", &seq_row.processing_method));
    }
    
    let processing_method = ProcessingMethod {
        id: 1,
        number: 1,
        param_tree: build_param_tree(&proc_params)?,
        shared_param_tree_id: None,
        data_processing_id: 1,
        software_id: 1,
    };
    
    // Run with comprehensive scan statistics and method texts
    let mut run_params = vec![
        ("MS", "MS:1000016", "scan start time", start_time_str.as_str()),
    ];
    
    // Add MS level counts if available
    if stats.ms1_count > 0 {
        run_params.push(("PRIDE", "PRIDE:0000481", "Number of MS1 spectra", ms1_str.as_str()));
    }
    if stats.ms2_count > 0 {
        run_params.push(("PRIDE", "PRIDE:0000482", "Number of MS2 spectra", ms2_str.as_str()));
    }
    if stats.ms3_count > 0 {
        run_params.push(("PRIDE", "PRIDE:0000483", "Number of MS3 spectra", ms3_str.as_str()));
    }
    
    // Add RT range from stats
    if stats.max_rt > 0.0 {
        run_params.push(("PRIDE", "PRIDE:0000474", "MS min RT", min_rt_str.as_str()));
        run_params.push(("PRIDE", "PRIDE:0000475", "MS max RT", max_rt_str.as_str()));
    }
    
    // Add precursor m/z range from stats
    if stats.max_precursor_mz > 0.0 {
        run_params.push(("PRIDE", "PRIDE:0000476", "MS min MZ", min_mz_str.as_str()));
        run_params.push(("PRIDE", "PRIDE:0000477", "MS max MZ", max_mz_str.as_str()));
    }
    
    // Build basic param tree
    let mut run_param_tree = build_param_tree(&run_params)?;
    
    // Extract method texts for both run param_tree and mzdb param_tree
    let (ms_method_text, lc_method_text) = if let Some(method) = raw.embedded_method() {
        extract_method_texts(method)
    } else {
        (None, None)
    };
    
    // Add method texts to run param_tree
    if ms_method_text.is_some() || lc_method_text.is_some() {
        let user_texts = build_run_user_texts(
            ms_method_text.as_deref(),
            lc_method_text.as_deref()
        )?;
        
        // Insert userTexts before closing </paramTree> tag
        if let Some(pos) = run_param_tree.rfind("</paramTree>") {
            run_param_tree.truncate(pos);
            run_param_tree.push_str(&user_texts);
            run_param_tree.push_str("</paramTree>");
        }
    }
    
    let run = Run {
        id: 1,
        name: "run_1".to_string(),
        start_timestamp: if !creation_date.is_empty() {
            Some(creation_date)
        } else {
            None
        },
        param_tree: Some(run_param_tree),
        shared_param_tree_id: None,
        sample_id: Some(1),
        default_instrument_config_id: 1,
        default_source_file_id: Some(1),
        default_scan_processing_id: 1,
        default_chrom_processing_id: 1,
    };
    
    Ok((
        WriterMetadata {
            runs: vec![run],
            samples: vec![sample],
            software: vec![software],
            source_files: vec![source_file],
            instrument_configurations: vec![inst_config],
            data_processings: vec![data_processing],
            processing_methods: vec![processing_method],
        },
        ms_method_text,
        lc_method_text,
    ))
}

/// Convert a RAW scan to an mzDB Spectrum
fn convert_scan_to_spectrum(
    scan_num: usize,
    scan: &thernio::raw::Scan,
    scan_event: Option<&thernio::raw::ScanEvent>,
    cycle: i64,
    raw: &thernio::raw::RawFile,
) -> Result<Spectrum> {
    // Build scan list XML
    let scan_list_str = build_scan_list(scan.retention_time)?;
    
    // Get charge state from trailer extra (0-based index for trailer_extra)
    let charge_state = raw.charge_state(scan_num - 1)
        .map(|c| c as i32);
    
    // Get isolation width and offset from scan's reactions
    let (isolation_width, isolation_offset) = scan.reactions.first()
        .map(|r| (Some(r.isolation_width), Some(r.isolation_offset)))
        .unwrap_or((None, None));
    
    // Build precursor list XML for MS2+
    let precursor_list_str = if scan.ms_level > 1 && !scan.precursor_mzs.is_empty() {
        // Get activation type and collision energy from scan event if available
        let (activation_type, collision_energy) = if let Some(event) = scan_event {
            let act = activation_type_to_string(event.activation);
            let ce = event.collision_energies.first().copied();
            (act, ce)
        } else {
            // Fallback: try to get collision energy from scan reactions
            let ce = scan.collision_energy();
            ("", ce)
        };
        
        // Only build precursor list if we have activation info
        if !activation_type.is_empty() {
            Some(build_precursor_list(
                &scan.precursor_mzs,
                charge_state,
                collision_energy,
                activation_type,
                isolation_width,
                isolation_offset,
            )?)
        } else {
            // Build minimal precursor list without activation details
            Some(build_precursor_list(
                &scan.precursor_mzs,
                charge_state,
                scan.collision_energy(),
                "",
                isolation_width,
                isolation_offset,
            )?)
        }
    } else {
        None
    };
    
    // Build spectrum param tree with appropriate CV terms
    let mut spec_params = Vec::new();
    
    // MS level
    match scan.ms_level {
        1 => spec_params.push(("MS", "MS:1000579", "MS1 spectrum", "")),
        2 => spec_params.push(("MS", "MS:1000580", "MSn spectrum", "")),
        _ => spec_params.push(("MS", "MS:1000580", "MSn spectrum", "")),
    }
    
    // Scan mode (centroid/profile) from scan event
    if let Some(event) = scan_event {
        match event.scan_mode {
            thernio::raw::ScanMode::Centroid => {
                spec_params.push(("MS", "MS:1000127", "centroid spectrum", ""));
            }
            thernio::raw::ScanMode::Profile => {
                spec_params.push(("MS", "MS:1000128", "profile spectrum", ""));
            }
        }
        
        // Polarity from scan event
        match event.polarity {
            thernio::raw::Polarity::Positive => {
                spec_params.push(("MS", "MS:1000130", "positive scan", ""));
            }
            thernio::raw::Polarity::Negative => {
                spec_params.push(("MS", "MS:1000129", "negative scan", ""));
            }
            _ => {} // Don't add polarity if unknown
        }
    }
    
    // Base peak m/z and intensity
    let bp_mz_str = format!("{:.6}", scan.base_peak_mz);
    spec_params.push(("MS", "MS:1000504", "base peak m/z", &bp_mz_str));
    
    let bp_int_str = format!("{:.2}", scan.base_peak_intensity);
    spec_params.push(("MS", "MS:1000505", "base peak intensity", &bp_int_str));
    
    // Total ion current
    let tic_str = format!("{:.2}", scan.total_ion_current);
    spec_params.push(("MS", "MS:1000285", "total ion current", &tic_str));
    
    // Scan window (m/z range)
    let low_mz_str = format!("{:.4}", scan.low_mz);
    spec_params.push(("MS", "MS:1000501", "scan window lower limit", &low_mz_str));
    
    let high_mz_str = format!("{:.4}", scan.high_mz);
    spec_params.push(("MS", "MS:1000500", "scan window upper limit", &high_mz_str));
    
    let param_tree_str = build_param_tree(&spec_params)?;
    
    // Get activation type for header
    let activation_type = if scan.ms_level > 1 {
        scan_event.map(|e| activation_type_to_string(e.activation).to_string())
    } else {
        None
    };
    
    // Create spectrum header
    let header = SpectrumHeader {
        id: scan_num as i64,
        initial_id: scan.index as i64,
        title: format!("Scan {}", scan_num),
        cycle,
        time: (scan.retention_time * 60.0) as f32, // Convert minutes to seconds
        ms_level: scan.ms_level as i64,
        activation_type,
        tic: scan.total_ion_current as f32,
        base_peak_mz: scan.base_peak_mz,
        base_peak_intensity: scan.base_peak_intensity as f32,
        precursor_mz: scan.precursor_mzs.first().copied(),
        precursor_charge: charge_state,
        peaks_count: scan.spectrum.len() as i64,
        param_tree_str: Some(param_tree_str),
        scan_list_str: Some(scan_list_str),
        precursor_list_str,
        product_list_str: None,
        shared_param_tree_id: None,
        instrument_configuration_id: 1,
        source_file_id: 1,
        run_id: 1,
        data_processing_id: 1,
        data_encoding_id: 1, // Will be updated by writer
        bb_first_spectrum_id: 0, // Will be updated by writer
    };
    
    // Convert peaks to arrays
    let mut mz_array = Vec::with_capacity(scan.spectrum.len());
    let mut intensity_array = Vec::with_capacity(scan.spectrum.len());
    
    for peak in &scan.spectrum.peaks {
        mz_array.push(peak.mz);
        intensity_array.push(peak.intensity);
    }
    
    // Create encoding - mode determined by caller based on scan_event
    let mode = if let Some(event) = scan_event {
        match event.scan_mode {
            thernio::raw::ScanMode::Centroid => DataMode::Centroid,
            thernio::raw::ScanMode::Profile => DataMode::Profile,
        }
    } else {
        DataMode::Centroid
    };
    
    let encoding = DataEncoding {
        id: 0,
        mode,
        peak_encoding: PeakEncoding::HighRes,
        compression: "none".to_string(),
        byte_order: ByteOrder::LittleEndian,
    };
    
    // Create spectrum data
    let data = SpectrumData::new(
        encoding,
        mz_array,
        intensity_array,
        None, // No left HWHM for centroid data
        None, // No right HWHM for centroid data
    );
    
    Ok(Spectrum { header, data })
}
