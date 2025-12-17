//! Writer Metadata
//!
//! Handles metadata structures and insertion for the mzDB writer.

use anyhow::{Context, Result};
use rusqlite::Connection;

use crate::metadata::*;
use crate::model::BBSizes;

/// Metadata for creating a new mzDB file
///
/// This structure holds all the metadata needed to create a complete mzDB file
#[derive(Clone, Debug, Default)]
pub struct WriterMetadata {
    pub runs: Vec<Run>,
    pub samples: Vec<Sample>,
    pub software: Vec<Software>,
    pub source_files: Vec<SourceFile>,
    pub instrument_configurations: Vec<InstrumentConfiguration>,
    pub data_processings: Vec<DataProcessing>,
    pub processing_methods: Vec<ProcessingMethod>,
}

impl WriterMetadata {
    /// Create a new empty metadata structure
    pub fn new() -> Self {
        Self::default()
    }
    
    /// Create metadata with default values for required fields
    pub fn with_defaults() -> Self {
        Self {
            runs: vec![Run {
                id: 1,
                name: "run_1".to_string(),
                start_timestamp: None,
                param_tree: None,
                shared_param_tree_id: None,
                sample_id: Some(1),
                default_instrument_config_id: 1,
                default_source_file_id: Some(1),
                default_scan_processing_id: 1,
                default_chrom_processing_id: 1,
            }],
            samples: vec![Sample {
                id: 1,
                name: "default_sample".to_string(),
                param_tree: None,
                shared_param_tree_id: None,
            }],
            software: vec![Software {
                id: 1,
                name: "any2mzdb".to_string(),
                // FIXME: this is a hack, we inject here the pwiz-mzdb version for mzdb-access backward compat, we should update mzdb-access instead
                version: "0.9.10",
                //version: env!("CARGO_PKG_VERSION").to_string(),
                param_tree: "".to_string(),
                shared_param_tree_id: None,
            }],
            source_files: vec![SourceFile {
                id: 1,
                name: "unknown".to_string(),
                location: "".to_string(),
                param_tree: "".to_string(),
                shared_param_tree_id: None,
            }],
            instrument_configurations: vec![InstrumentConfiguration {
                id: 1,
                name: "default_instrument".to_string(),
                param_tree: None,
                component_list: "".to_string(),
                shared_param_tree_id: None,
                software_id: Some(1),
            }],
            data_processings: vec![],
            processing_methods: vec![],
        }
    }
}

/// Insert all metadata into the database
pub(crate) fn insert_metadata(
    conn: &mut Connection,
    metadata: &WriterMetadata,
    bb_sizes: &BBSizes,
    is_dia: bool,
) -> Result<()> {
    // Insert data processings
    insert_data_processings(conn, metadata)?;
    
    // Insert software
    insert_software(conn, metadata)?;
    
    // Insert processing methods
    insert_processing_methods(conn, metadata)?;
    
    // Insert samples
    insert_samples(conn, metadata)?;
    
    // Insert source files
    insert_source_files(conn, metadata)?;
    
    // Insert instrument configurations
    insert_instrument_configurations(conn, metadata)?;
    
    // Insert mzDB header
    insert_mzdb_header(conn, metadata, bb_sizes)?;
    
    // Insert runs
    insert_runs(conn, metadata, is_dia)?;
    
    Ok(())
}

fn insert_data_processings(conn: &Connection, metadata: &WriterMetadata) -> Result<()> {
    let mut stmt = conn.prepare("INSERT INTO data_processing VALUES (NULL, ?)")?;
    
    // Get unique data processing names
    let mut dp_names = std::collections::HashSet::new();
    
    for dp in &metadata.data_processings {
        dp_names.insert(&dp.name);
    }
    
    if dp_names.is_empty() {
        dp_names.insert("default_processing");
    }
    
    for name in dp_names {
        stmt.execute([name])?;
    }
    
    Ok(())
}

fn insert_software(conn: &Connection, metadata: &WriterMetadata) -> Result<()> {
    let mut stmt = conn.prepare(
        "INSERT INTO software VALUES (NULL, ?, ?, ?, NULL)"
    )?;
    
    if metadata.software.is_empty() {
        // Insert default software
        stmt.execute(rusqlite::params![
            "mzdb-rs",
            env!("CARGO_PKG_VERSION"),
            "",  // param_tree
        ])?;
    } else {
        for software in &metadata.software {
            stmt.execute(rusqlite::params![
                &software.name,
                &software.version,
                "",  // param_tree - TODO: serialize when available
            ])?;
        }
    }
    
    Ok(())
}

fn insert_processing_methods(conn: &Connection, metadata: &WriterMetadata) -> Result<()> {
    let mut stmt = conn.prepare(
        "INSERT INTO processing_method VALUES (NULL, ?, ?, ?, ?, ?)"
    )?;
    
    if metadata.processing_methods.is_empty() {
        // Insert default processing method
        stmt.execute(rusqlite::params![
            1i64,    // number
            "",      // param_tree
            rusqlite::types::Null, // shared_param_tree_id
            1i64,    // data_processing_id
            1i64,    // software_id
        ])?;
    } else {
        for pm in &metadata.processing_methods {
            stmt.execute(rusqlite::params![
                pm.number,
                &pm.param_tree,
                pm.shared_param_tree_id,
                pm.data_processing_id,
                pm.software_id,
            ])?;
        }
    }
    
    Ok(())
}

fn insert_samples(conn: &Connection, metadata: &WriterMetadata) -> Result<()> {
    let mut stmt = conn.prepare("INSERT INTO sample VALUES (NULL, ?, ?, NULL)")?;
    
    if metadata.samples.is_empty() {
        // Insert default sample
        stmt.execute(rusqlite::params![
            "default_sample",
            rusqlite::types::Null,  // param_tree
        ])?;
    } else {
        for sample in &metadata.samples {
            stmt.execute(rusqlite::params![
                &sample.name,
                rusqlite::types::Null,  // param_tree
            ])?;
        }
    }
    
    Ok(())
}

fn insert_source_files(conn: &Connection, metadata: &WriterMetadata) -> Result<()> {
    let mut stmt = conn.prepare(
        "INSERT INTO source_file VALUES (NULL, ?, ?, ?, NULL)"
    )?;
    
    if metadata.source_files.is_empty() {
        // Insert default source file
        stmt.execute(rusqlite::params![
            "unknown",
            "",
            "",  // param_tree
        ])?;
    } else {
        for source_file in &metadata.source_files {
            stmt.execute(rusqlite::params![
                &source_file.name,
                &source_file.location,
                &source_file.param_tree,
            ])?;
        }
    }
    
    Ok(())
}

fn insert_instrument_configurations(conn: &Connection, metadata: &WriterMetadata) -> Result<()> {
    let mut stmt = conn.prepare(
        "INSERT INTO instrument_configuration VALUES (NULL, ?, NULL, ?, NULL, ?)"
    )?;
    
    if metadata.instrument_configurations.is_empty() {
        // Insert default instrument configuration
        stmt.execute(rusqlite::params![
            "default_instrument",
            "",   // component_list
            1i64, // software_id
        ])?;
    } else {
        for inst_config in &metadata.instrument_configurations {
            stmt.execute(rusqlite::params![
                &inst_config.name,
                "",   // component_list - TODO: serialize when available
                1i64, // software_id
            ])?;
        }
    }
    
    Ok(())
}

fn insert_mzdb_header(
    conn: &Connection,
    _metadata: &WriterMetadata,
    bb_sizes: &BBSizes,
) -> Result<()> {
    let mut stmt = conn.prepare("INSERT INTO mzdb VALUES (?, ?, ?, ?, ?)")?;
    
    // Create param_tree XML with BB sizes
    let param_tree = format!(
        r#"<paramTree>
<userParam name="ms1_bb_mz_width" value="{}" type="xsd:float"/>
<userParam name="ms1_bb_time_width" value="{}" type="xsd:float"/>
<userParam name="msn_bb_mz_width" value="{}" type="xsd:float"/>
<userParam name="msn_bb_time_width" value="{}" type="xsd:float"/>
</paramTree>"#,
        bb_sizes.bb_mz_height_ms1,
        bb_sizes.bb_rt_width_ms1,
        bb_sizes.bb_mz_height_msn,
        bb_sizes.bb_rt_width_msn,
    );
    
    let timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    
    stmt.execute(rusqlite::params![
        "0.7",          // version
        timestamp.to_string(),
        "",             // file_content - TODO: proper serialization
        "",             // contacts
        param_tree,     // param_tree
    ])?;
    
    Ok(())
}

fn insert_runs(conn: &Connection, metadata: &WriterMetadata, is_dia: bool) -> Result<()> {
    let mut stmt = conn.prepare(
        "INSERT INTO run VALUES (NULL, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
    )?;
    
    if metadata.runs.is_empty() {
        // Insert default run
        let acquisition_mode = if is_dia { "SWATH" } else { "DDA" };
        let param_tree = format!(
            r#"<paramTree><cvParam accession="MS:1000000" name="acquisition parameter" value="{}"/></paramTree>"#,
            acquisition_mode
        );
        
        stmt.execute(rusqlite::params![
            "run_1",                           // name
            rusqlite::types::Null,             // start_timestamp
            param_tree,                        // param_tree
            rusqlite::types::Null,             // shared_param_tree_id
            1i64,                              // sample_id
            1i64,                              // default_instrument_config_id
            rusqlite::types::Null,             // default_source_file_id
            1i64,                              // default_scan_processing_id
            1i64,                              // default_chrom_processing_id
        ])?;
    } else {
        for run in &metadata.runs {
            // TODO: serialize param_tree properly
            stmt.execute(rusqlite::params![
                &run.name,
                rusqlite::types::Null, // start_timestamp
                "",                    // param_tree
                rusqlite::types::Null, // shared_param_tree_id
                1i64,                  // sample_id
                1i64,                  // default_instrument_config_id
                rusqlite::types::Null, // default_source_file_id
                1i64,                  // default_scan_processing_id
                1i64,                  // default_chrom_processing_id
            ])?;
        }
    }
    
    Ok(())
}
