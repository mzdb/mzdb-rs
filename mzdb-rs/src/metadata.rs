//! Metadata tables support for mzDB files
//!
//! This module provides complete support for all mzDB metadata tables including:
//! - File-level metadata (mzdb table)
//! - Run and sample information
//! - Software and instrument configurations
//! - Data processing workflows
//! - Controlled vocabularies (CV terms and units)
//! - Scan settings and targets
//! - Source files
//! - Shared parameter trees and schemas
#![allow(unused)]

use anyhow_ext::{Context, Result};
use rusqlite::{params, Connection, OptionalExtension};
use serde::{Deserialize, Serialize};

use crate::query_utils::query_single_string_with_params;

// ============================================================================
// File-Level Metadata (mzdb table)
// ============================================================================

/// Complete mzDB file metadata from the mzdb table
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct MzDbMetadata {
    /// mzDB format version (e.g., "0.7.0")
    pub version: String,
    /// File creation timestamp in ISO-8601 format
    pub creation_timestamp: String,
    /// XML string describing file content
    pub file_content: String,
    /// XML string with contact information
    pub contact: String,
    /// XML param tree with additional metadata
    pub param_tree: String,
}

/// Query complete mzDB file metadata
pub fn get_mzdb_metadata(db: &Connection) -> Result<Option<MzDbMetadata>> {
    let mut stmt = db
        .prepare("SELECT version, creation_timestamp, file_content, contact, param_tree FROM mzdb LIMIT 1")
        .dot()?;

    stmt.query_row([], |row| {
        Ok(MzDbMetadata {
            version: row.get(0)?,
            creation_timestamp: row.get(1)?,
            file_content: row.get(2)?,
            contact: row.get(3)?,
            param_tree: row.get(4)?,
        })
    })
    .optional()
    .dot()
}

// ============================================================================
// Run Table
// ============================================================================

/// A run represents a single mass spectrometry acquisition
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Run {
    pub id: i64,
    /// Unique name for this run
    pub name: String,
    /// Start timestamp in ISO-8601 format
    pub start_timestamp: Option<String>,
    /// XML param tree
    pub param_tree: Option<String>,
    /// Reference to shared param tree
    pub shared_param_tree_id: Option<i64>,
    /// Reference to sample
    pub sample_id: Option<i64>,
    /// Reference to default instrument configuration
    pub default_instrument_config_id: i64,
    /// Reference to default source file
    pub default_source_file_id: Option<i64>,
    /// Reference to default scan processing
    pub default_scan_processing_id: i64,
    /// Reference to default chromatogram processing
    pub default_chrom_processing_id: i64,
}

/// List all runs in the mzDB file
pub fn list_runs(db: &Connection) -> Result<Vec<Run>> {
    let mut stmt = db
        .prepare(
            "SELECT id, name, start_timestamp, param_tree, shared_param_tree_id, \
             sample_id, default_instrument_config_id, default_source_file_id, \
             default_scan_processing_id, default_chrom_processing_id FROM run",
        )
        .dot()?;

    let rows = stmt
        .query_map([], |row| {
            Ok(Run {
                id: row.get(0)?,
                name: row.get(1)?,
                start_timestamp: row.get(2)?,
                param_tree: row.get(3)?,
                shared_param_tree_id: row.get(4)?,
                sample_id: row.get(5)?,
                default_instrument_config_id: row.get(6)?,
                default_source_file_id: row.get(7)?,
                default_scan_processing_id: row.get(8)?,
                default_chrom_processing_id: row.get(9)?,
            })
        })
        .dot()?;

    rows.collect::<rusqlite::Result<Vec<_>>>().dot()
}

/// Get a specific run by ID
pub fn get_run(db: &Connection, run_id: i64) -> Result<Option<Run>> {
    let mut stmt = db
        .prepare(
            "SELECT id, name, start_timestamp, param_tree, shared_param_tree_id, \
             sample_id, default_instrument_config_id, default_source_file_id, \
             default_scan_processing_id, default_chrom_processing_id FROM run WHERE id = ?",
        )
        .dot()?;

    stmt.query_row(params![run_id], |row| {
        Ok(Run {
            id: row.get(0)?,
            name: row.get(1)?,
            start_timestamp: row.get(2)?,
            param_tree: row.get(3)?,
            shared_param_tree_id: row.get(4)?,
            sample_id: row.get(5)?,
            default_instrument_config_id: row.get(6)?,
            default_source_file_id: row.get(7)?,
            default_scan_processing_id: row.get(8)?,
            default_chrom_processing_id: row.get(9)?,
        })
    })
    .optional()
    .dot()
}

// ============================================================================
// Sample Table
// ============================================================================

/// Description of a sample used to generate the dataset
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Sample {
    pub id: i64,
    /// Unique name for this sample
    pub name: String,
    /// XML param tree
    pub param_tree: Option<String>,
    /// Reference to shared param tree
    pub shared_param_tree_id: Option<i64>,
}

/// List all samples
pub fn list_samples(db: &Connection) -> Result<Vec<Sample>> {
    let mut stmt = db
        .prepare("SELECT id, name, param_tree, shared_param_tree_id FROM sample")
        .dot()?;

    let rows = stmt
        .query_map([], |row| {
            Ok(Sample {
                id: row.get(0)?,
                name: row.get(1)?,
                param_tree: row.get(2)?,
                shared_param_tree_id: row.get(3)?,
            })
        })
        .dot()?;

    rows.collect::<rusqlite::Result<Vec<_>>>().dot()
}

/// Get a specific sample by ID
pub fn get_sample(db: &Connection, sample_id: i64) -> Result<Option<Sample>> {
    let mut stmt = db
        .prepare("SELECT id, name, param_tree, shared_param_tree_id FROM sample WHERE id = ?")
        .dot()?;

    stmt.query_row(params![sample_id], |row| {
        Ok(Sample {
            id: row.get(0)?,
            name: row.get(1)?,
            param_tree: row.get(2)?,
            shared_param_tree_id: row.get(3)?,
        })
    })
    .optional()
    .dot()
}

// ============================================================================
// Software Table
// ============================================================================

/// Software used in creating the mzDB file
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Software {
    pub id: i64,
    /// Unique name for this software
    pub name: String,
    /// Software version string
    pub version: String,
    /// XML param tree with additional info
    pub param_tree: String,
    /// Reference to shared param tree
    pub shared_param_tree_id: Option<i64>,
}

/// List all software entries
pub fn list_software(db: &Connection) -> Result<Vec<Software>> {
    let mut stmt = db
        .prepare("SELECT id, name, version, param_tree, shared_param_tree_id FROM software")
        .dot()?;

    let rows = stmt
        .query_map([], |row| {
            Ok(Software {
                id: row.get(0)?,
                name: row.get(1)?,
                version: row.get(2)?,
                param_tree: row.get(3)?,
                shared_param_tree_id: row.get(4)?,
            })
        })
        .dot()?;

    rows.collect::<rusqlite::Result<Vec<_>>>().dot()
}

/// Get software by ID
pub fn get_software(db: &Connection, software_id: i64) -> Result<Option<Software>> {
    let mut stmt = db
        .prepare("SELECT id, name, version, param_tree, shared_param_tree_id FROM software WHERE id = ?")
        .dot()?;

    stmt.query_row(params![software_id], |row| {
        Ok(Software {
            id: row.get(0)?,
            name: row.get(1)?,
            version: row.get(2)?,
            param_tree: row.get(3)?,
            shared_param_tree_id: row.get(4)?,
        })
    })
    .optional()
    .dot()
}

/// Get software by name pattern (e.g., "%mzDB" for pwiz-mzdb)
pub fn get_software_by_name(db: &Connection, name_pattern: &str) -> Result<Option<Software>> {
    let mut stmt = db
        .prepare("SELECT id, name, version, param_tree, shared_param_tree_id FROM software WHERE name LIKE ?")
        .dot()?;

    stmt.query_row(params![name_pattern], |row| {
        Ok(Software {
            id: row.get(0)?,
            name: row.get(1)?,
            version: row.get(2)?,
            param_tree: row.get(3)?,
            shared_param_tree_id: row.get(4)?,
        })
    })
    .optional()
    .dot()
}

// ============================================================================
// Instrument Configuration Table
// ============================================================================

/// Description of a particular hardware configuration
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct InstrumentConfiguration {
    pub id: i64,
    /// Unique name for this configuration
    pub name: String,
    /// XML param tree
    pub param_tree: String,
    /// XML list of instrument components
    pub component_list: String,
    /// Reference to shared param tree
    pub shared_param_tree_id: Option<i64>,
    /// Reference to software used with this configuration
    pub software_id: i64,
}

/// List all instrument configurations
pub fn list_instrument_configurations(db: &Connection) -> Result<Vec<InstrumentConfiguration>> {
    let mut stmt = db
        .prepare(
            "SELECT id, name, param_tree, component_list, shared_param_tree_id, software_id \
             FROM instrument_configuration",
        )
        .dot()?;

    let rows = stmt
        .query_map([], |row| {
            Ok(InstrumentConfiguration {
                id: row.get(0)?,
                name: row.get(1)?,
                param_tree: row.get(2)?,
                component_list: row.get(3)?,
                shared_param_tree_id: row.get(4)?,
                software_id: row.get(5)?,
            })
        })
        .dot()?;

    rows.collect::<rusqlite::Result<Vec<_>>>().dot()
}

/// Get instrument configuration by ID
pub fn get_instrument_configuration(
    db: &Connection,
    config_id: i64,
) -> Result<Option<InstrumentConfiguration>> {
    let mut stmt = db
        .prepare(
            "SELECT id, name, param_tree, component_list, shared_param_tree_id, software_id \
             FROM instrument_configuration WHERE id = ?",
        )
        .dot()?;

    stmt.query_row(params![config_id], |row| {
        Ok(InstrumentConfiguration {
            id: row.get(0)?,
            name: row.get(1)?,
            param_tree: row.get(2)?,
            component_list: row.get(3)?,
            shared_param_tree_id: row.get(4)?,
            software_id: row.get(5)?,
        })
    })
    .optional()
    .dot()
}

// ============================================================================
// Source File Table
// ============================================================================

/// Description of a source file from which this mzDB was derived
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SourceFile {
    pub id: i64,
    /// Name of the source file (without path)
    pub name: String,
    /// URI-formatted location where the file was retrieved
    pub location: String,
    /// XML param tree
    pub param_tree: String,
    /// Reference to shared param tree
    pub shared_param_tree_id: Option<i64>,
}

/// List all source files
pub fn list_source_files(db: &Connection) -> Result<Vec<SourceFile>> {
    let mut stmt = db
        .prepare("SELECT id, name, location, param_tree, shared_param_tree_id FROM source_file")
        .dot()?;

    let rows = stmt
        .query_map([], |row| {
            Ok(SourceFile {
                id: row.get(0)?,
                name: row.get(1)?,
                location: row.get(2)?,
                param_tree: row.get(3)?,
                shared_param_tree_id: row.get(4)?,
            })
        })
        .dot()?;

    rows.collect::<rusqlite::Result<Vec<_>>>().dot()
}

/// Get source file by ID
pub fn get_source_file(db: &Connection, source_file_id: i64) -> Result<Option<SourceFile>> {
    let mut stmt = db
        .prepare("SELECT id, name, location, param_tree, shared_param_tree_id FROM source_file WHERE id = ?")
        .dot()?;

    stmt.query_row(params![source_file_id], |row| {
        Ok(SourceFile {
            id: row.get(0)?,
            name: row.get(1)?,
            location: row.get(2)?,
            param_tree: row.get(3)?,
            shared_param_tree_id: row.get(4)?,
        })
    })
    .optional()
    .dot()
}

// ============================================================================
// Data Processing Table
// ============================================================================

/// Description of data processing workflows
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct DataProcessing {
    pub id: i64,
    /// Unique name for this data processing workflow
    pub name: String,
}

/// List all data processing workflows
pub fn list_data_processings(db: &Connection) -> Result<Vec<DataProcessing>> {
    let mut stmt = db.prepare("SELECT id, name FROM data_processing").dot()?;

    let rows = stmt
        .query_map([], |row| {
            Ok(DataProcessing {
                id: row.get(0)?,
                name: row.get(1)?,
            })
        })
        .dot()?;

    rows.collect::<rusqlite::Result<Vec<_>>>().dot()
}

/// Get data processing by ID
pub fn get_data_processing(db: &Connection, dp_id: i64) -> Result<Option<DataProcessing>> {
    let mut stmt = db
        .prepare("SELECT id, name FROM data_processing WHERE id = ?")
        .dot()?;

    stmt.query_row(params![dp_id], |row| {
        Ok(DataProcessing {
            id: row.get(0)?,
            name: row.get(1)?,
        })
    })
    .optional()
    .dot()
}

// ============================================================================
// Processing Method Table
// ============================================================================

/// A specific processing method within a data processing workflow
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ProcessingMethod {
    pub id: i64,
    /// Order of this method within the workflow
    pub number: i64,
    /// XML param tree describing the method
    pub param_tree: String,
    /// Reference to shared param tree
    pub shared_param_tree_id: Option<i64>,
    /// Reference to parent data processing workflow
    pub data_processing_id: i64,
    /// Reference to software used for this method
    pub software_id: i64,
}

/// List all processing methods
pub fn list_processing_methods(db: &Connection) -> Result<Vec<ProcessingMethod>> {
    let mut stmt = db
        .prepare(
            "SELECT id, number, param_tree, shared_param_tree_id, data_processing_id, software_id \
             FROM processing_method ORDER BY number",
        )
        .dot()?;

    let rows = stmt
        .query_map([], |row| {
            Ok(ProcessingMethod {
                id: row.get(0)?,
                number: row.get(1)?,
                param_tree: row.get(2)?,
                shared_param_tree_id: row.get(3)?,
                data_processing_id: row.get(4)?,
                software_id: row.get(5)?,
            })
        })
        .dot()?;

    rows.collect::<rusqlite::Result<Vec<_>>>().dot()
}

/// Get processing methods for a specific data processing workflow
pub fn get_processing_methods_for_workflow(
    db: &Connection,
    data_processing_id: i64,
) -> Result<Vec<ProcessingMethod>> {
    let mut stmt = db
        .prepare(
            "SELECT id, number, param_tree, shared_param_tree_id, data_processing_id, software_id \
             FROM processing_method WHERE data_processing_id = ? ORDER BY number",
        )
        .dot()?;

    let rows = stmt
        .query_map(params![data_processing_id], |row| {
            Ok(ProcessingMethod {
                id: row.get(0)?,
                number: row.get(1)?,
                param_tree: row.get(2)?,
                shared_param_tree_id: row.get(3)?,
                data_processing_id: row.get(4)?,
                software_id: row.get(5)?,
            })
        })
        .dot()?;

    rows.collect::<rusqlite::Result<Vec<_>>>().dot()
}

// ============================================================================
// Scan Settings Table
// ============================================================================

/// Scan settings configured for acquisition
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ScanSettings {
    pub id: i64,
    /// XML param tree
    pub param_tree: Option<String>,
    /// Reference to shared param tree
    pub shared_param_tree_id: Option<i64>,
}

/// List all scan settings
pub fn list_scan_settings(db: &Connection) -> Result<Vec<ScanSettings>> {
    let mut stmt = db
        .prepare("SELECT id, param_tree, shared_param_tree_id FROM scan_settings")
        .dot()?;

    let rows = stmt
        .query_map([], |row| {
            Ok(ScanSettings {
                id: row.get(0)?,
                param_tree: row.get(1)?,
                shared_param_tree_id: row.get(2)?,
            })
        })
        .dot()?;

    rows.collect::<rusqlite::Result<Vec<_>>>().dot()
}

/// Get scan settings by ID
pub fn get_scan_settings(db: &Connection, settings_id: i64) -> Result<Option<ScanSettings>> {
    let mut stmt = db
        .prepare("SELECT id, param_tree, shared_param_tree_id FROM scan_settings WHERE id = ?")
        .dot()?;

    stmt.query_row(params![settings_id], |row| {
        Ok(ScanSettings {
            id: row.get(0)?,
            param_tree: row.get(1)?,
            shared_param_tree_id: row.get(2)?,
        })
    })
    .optional()
    .dot()
}

// ============================================================================
// Target Table (Inclusion List)
// ============================================================================

/// A target in the inclusion list for targeted acquisition
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Target {
    pub id: i64,
    /// XML param tree describing the target
    pub param_tree: String,
    /// Reference to shared param tree
    pub shared_param_tree_id: Option<i64>,
    /// Reference to scan settings
    pub scan_settings_id: i64,
}

/// List all targets
pub fn list_targets(db: &Connection) -> Result<Vec<Target>> {
    let mut stmt = db
        .prepare("SELECT id, param_tree, shared_param_tree_id, scan_settings_id FROM target")
        .dot()?;

    let rows = stmt
        .query_map([], |row| {
            Ok(Target {
                id: row.get(0)?,
                param_tree: row.get(1)?,
                shared_param_tree_id: row.get(2)?,
                scan_settings_id: row.get(3)?,
            })
        })
        .dot()?;

    rows.collect::<rusqlite::Result<Vec<_>>>().dot()
}

/// Get targets for a specific scan settings
pub fn get_targets_for_scan_settings(
    db: &Connection,
    scan_settings_id: i64,
) -> Result<Vec<Target>> {
    let mut stmt = db
        .prepare(
            "SELECT id, param_tree, shared_param_tree_id, scan_settings_id \
             FROM target WHERE scan_settings_id = ?",
        )
        .dot()?;

    let rows = stmt
        .query_map(params![scan_settings_id], |row| {
            Ok(Target {
                id: row.get(0)?,
                param_tree: row.get(1)?,
                shared_param_tree_id: row.get(2)?,
                scan_settings_id: row.get(3)?,
            })
        })
        .dot()?;

    rows.collect::<rusqlite::Result<Vec<_>>>().dot()
}

// ============================================================================
// Source File Scan Settings Map
// ============================================================================

/// Mapping between source files and scan settings
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SourceFileScanSettingsMap {
    pub scan_settings_id: i64,
    pub source_file_id: i64,
}

/// List all source file to scan settings mappings
pub fn list_source_file_scan_settings_maps(
    db: &Connection,
) -> Result<Vec<SourceFileScanSettingsMap>> {
    let mut stmt = db
        .prepare("SELECT scan_settings_id, source_file_id FROM source_file_scan_settings_map")
        .dot()?;

    let rows = stmt
        .query_map([], |row| {
            Ok(SourceFileScanSettingsMap {
                scan_settings_id: row.get(0)?,
                source_file_id: row.get(1)?,
            })
        })
        .dot()?;

    rows.collect::<rusqlite::Result<Vec<_>>>().dot()
}

// ============================================================================
// Controlled Vocabulary (CV) Tables
// ============================================================================

/// A controlled vocabulary (e.g., PSI-MS, UO)
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ControlledVocabulary {
    /// Short identifier (e.g., "MS", "UO")
    pub id: String,
    /// Full name of the CV
    pub full_name: String,
    /// Version string
    pub version: Option<String>,
    /// URI where the CV can be found
    pub uri: String,
}

/// List all controlled vocabularies
pub fn list_controlled_vocabularies(db: &Connection) -> Result<Vec<ControlledVocabulary>> {
    let mut stmt = db.prepare("SELECT id, full_name, version, uri FROM cv").dot()?;

    let rows = stmt
        .query_map([], |row| {
            Ok(ControlledVocabulary {
                id: row.get(0)?,
                full_name: row.get(1)?,
                version: row.get(2)?,
                uri: row.get(3)?,
            })
        })
        .dot()?;

    rows.collect::<rusqlite::Result<Vec<_>>>().dot()
}

/// Get a controlled vocabulary by ID
pub fn get_controlled_vocabulary(db: &Connection, cv_id: &str) -> Result<Option<ControlledVocabulary>> {
    let mut stmt = db
        .prepare("SELECT id, full_name, version, uri FROM cv WHERE id = ?")
        .dot()?;

    stmt.query_row(params![cv_id], |row| {
        Ok(ControlledVocabulary {
            id: row.get(0)?,
            full_name: row.get(1)?,
            version: row.get(2)?,
            uri: row.get(3)?,
        })
    })
    .optional()
    .dot()
}

/// A term from a controlled vocabulary
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct CvTerm {
    /// Accession number (e.g., "MS:1000031")
    pub accession: String,
    /// Human-readable name
    pub name: String,
    /// Unit accession if applicable
    pub unit_accession: Option<String>,
    /// Reference to parent CV
    pub cv_id: String,
}

/// List all CV terms
pub fn list_cv_terms(db: &Connection) -> Result<Vec<CvTerm>> {
    let mut stmt = db
        .prepare("SELECT accession, name, unit_accession, cv_id FROM cv_term")
        .dot()?;

    let rows = stmt
        .query_map([], |row| {
            Ok(CvTerm {
                accession: row.get(0)?,
                name: row.get(1)?,
                unit_accession: row.get(2)?,
                cv_id: row.get(3)?,
            })
        })
        .dot()?;

    rows.collect::<rusqlite::Result<Vec<_>>>().dot()
}

/// Get CV term by accession
pub fn get_cv_term(db: &Connection, accession: &str) -> Result<Option<CvTerm>> {
    let mut stmt = db
        .prepare("SELECT accession, name, unit_accession, cv_id FROM cv_term WHERE accession = ?")
        .dot()?;

    stmt.query_row(params![accession], |row| {
        Ok(CvTerm {
            accession: row.get(0)?,
            name: row.get(1)?,
            unit_accession: row.get(2)?,
            cv_id: row.get(3)?,
        })
    })
    .optional()
    .dot()
}

/// Search CV terms by name pattern
pub fn search_cv_terms(db: &Connection, name_pattern: &str) -> Result<Vec<CvTerm>> {
    let mut stmt = db
        .prepare("SELECT accession, name, unit_accession, cv_id FROM cv_term WHERE name LIKE ?")
        .dot()?;

    let rows = stmt
        .query_map(params![format!("%{}%", name_pattern)], |row| {
            Ok(CvTerm {
                accession: row.get(0)?,
                name: row.get(1)?,
                unit_accession: row.get(2)?,
                cv_id: row.get(3)?,
            })
        })
        .dot()?;

    rows.collect::<rusqlite::Result<Vec<_>>>().dot()
}

/// A unit from a controlled vocabulary
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct CvUnit {
    /// Accession number
    pub accession: String,
    /// Human-readable name
    pub name: String,
    /// Reference to parent CV
    pub cv_id: String,
}

/// List all CV units
pub fn list_cv_units(db: &Connection) -> Result<Vec<CvUnit>> {
    let mut stmt = db
        .prepare("SELECT accession, name, cv_id FROM cv_unit")
        .dot()?;

    let rows = stmt
        .query_map([], |row| {
            Ok(CvUnit {
                accession: row.get(0)?,
                name: row.get(1)?,
                cv_id: row.get(2)?,
            })
        })
        .dot()?;

    rows.collect::<rusqlite::Result<Vec<_>>>().dot()
}

/// Get CV unit by accession
pub fn get_cv_unit(db: &Connection, accession: &str) -> Result<Option<CvUnit>> {
    let mut stmt = db
        .prepare("SELECT accession, name, cv_id FROM cv_unit WHERE accession = ?")
        .dot()?;

    stmt.query_row(params![accession], |row| {
        Ok(CvUnit {
            accession: row.get(0)?,
            name: row.get(1)?,
            cv_id: row.get(2)?,
        })
    })
    .optional()
    .dot()
}

/// A user-defined term (not from a standard CV)
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct UserTerm {
    pub id: i64,
    /// Unique name for this term
    pub name: String,
    /// Data type (e.g., "xsd:float")
    pub term_type: String,
    /// Unit accession if applicable
    pub unit_accession: Option<String>,
}

/// List all user terms
pub fn list_user_terms(db: &Connection) -> Result<Vec<UserTerm>> {
    let mut stmt = db
        .prepare("SELECT id, name, type, unit_accession FROM user_term")
        .dot()?;

    let rows = stmt
        .query_map([], |row| {
            Ok(UserTerm {
                id: row.get(0)?,
                name: row.get(1)?,
                term_type: row.get(2)?,
                unit_accession: row.get(3)?,
            })
        })
        .dot()?;

    rows.collect::<rusqlite::Result<Vec<_>>>().dot()
}

/// Get user term by ID
pub fn get_user_term(db: &Connection, term_id: i64) -> Result<Option<UserTerm>> {
    let mut stmt = db
        .prepare("SELECT id, name, type, unit_accession FROM user_term WHERE id = ?")
        .dot()?;

    stmt.query_row(params![term_id], |row| {
        Ok(UserTerm {
            id: row.get(0)?,
            name: row.get(1)?,
            term_type: row.get(2)?,
            unit_accession: row.get(3)?,
        })
    })
    .optional()
    .dot()
}

// ============================================================================
// Shared Param Tree Table
// ============================================================================

/// A reusable parameter tree that can be shared across multiple entities
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SharedParamTree {
    pub id: i64,
    /// XML data containing the param tree
    pub data: String,
    /// Name of the schema for this param tree
    pub schema_name: String,
}

/// List all shared param trees
pub fn list_shared_param_trees(db: &Connection) -> Result<Vec<SharedParamTree>> {
    let mut stmt = db
        .prepare("SELECT id, data, schema_name FROM shared_param_tree")
        .dot()?;

    let rows = stmt
        .query_map([], |row| {
            Ok(SharedParamTree {
                id: row.get(0)?,
                data: row.get(1)?,
                schema_name: row.get(2)?,
            })
        })
        .dot()?;

    rows.collect::<rusqlite::Result<Vec<_>>>().dot()
}

/// Get shared param tree by ID
pub fn get_shared_param_tree(db: &Connection, tree_id: i64) -> Result<Option<SharedParamTree>> {
    let mut stmt = db
        .prepare("SELECT id, data, schema_name FROM shared_param_tree WHERE id = ?")
        .dot()?;

    stmt.query_row(params![tree_id], |row| {
        Ok(SharedParamTree {
            id: row.get(0)?,
            data: row.get(1)?,
            schema_name: row.get(2)?,
        })
    })
    .optional()
    .dot()
}

// ============================================================================
// Param Tree Schema Tables
// ============================================================================

/// Schema definition for param trees
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ParamTreeSchema {
    /// Unique name of the schema
    pub name: String,
    /// Type of schema (e.g., "XSD")
    pub schema_type: String,
    /// The actual schema content
    pub schema: String,
}

/// List all param tree schemas
pub fn list_param_tree_schemas(db: &Connection) -> Result<Vec<ParamTreeSchema>> {
    let mut stmt = db
        .prepare("SELECT name, type, schema FROM param_tree_schema")
        .dot()?;

    let rows = stmt
        .query_map([], |row| {
            Ok(ParamTreeSchema {
                name: row.get(0)?,
                schema_type: row.get(1)?,
                schema: row.get(2)?,
            })
        })
        .dot()?;

    rows.collect::<rusqlite::Result<Vec<_>>>().dot()
}

/// Get param tree schema by name
pub fn get_param_tree_schema(db: &Connection, schema_name: &str) -> Result<Option<ParamTreeSchema>> {
    let mut stmt = db
        .prepare("SELECT name, type, schema FROM param_tree_schema WHERE name = ?")
        .dot()?;

    stmt.query_row(params![schema_name], |row| {
        Ok(ParamTreeSchema {
            name: row.get(0)?,
            schema_type: row.get(1)?,
            schema: row.get(2)?,
        })
    })
    .optional()
    .dot()
}

/// Mapping between tables and their param tree schemas
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct TableParamTreeSchema {
    /// Name of the table
    pub table_name: String,
    /// Name of the schema used for this table's param_tree column
    pub schema_name: String,
}

/// List all table to schema mappings
pub fn list_table_param_tree_schemas(db: &Connection) -> Result<Vec<TableParamTreeSchema>> {
    let mut stmt = db
        .prepare("SELECT table_name, schema_name FROM table_param_tree_schema")
        .dot()?;

    let rows = stmt
        .query_map([], |row| {
            Ok(TableParamTreeSchema {
                table_name: row.get(0)?,
                schema_name: row.get(1)?,
            })
        })
        .dot()?;

    rows.collect::<rusqlite::Result<Vec<_>>>().dot()
}

/// Get the schema name for a specific table
pub fn get_schema_for_table(db: &Connection, table_name: &str) -> Result<Option<String>> {
    query_single_string_with_params(
        db,
        "SELECT schema_name FROM table_param_tree_schema WHERE table_name = ?1",
        [table_name],
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    // Integration tests would require a real mzDB file
    // Unit tests for struct creation
    #[test]
    fn test_mzdb_metadata_struct() {
        let metadata = MzDbMetadata {
            version: "0.7.0".to_string(),
            creation_timestamp: "2024-01-01T00:00:00".to_string(),
            file_content: "<fileContent/>".to_string(),
            contact: "<contact/>".to_string(),
            param_tree: "<params/>".to_string(),
        };
        assert_eq!(metadata.version, "0.7.0");
    }

    #[test]
    fn test_software_struct() {
        let sw = Software {
            id: 1,
            name: "pwiz-mzDB".to_string(),
            version: "1.0.0".to_string(),
            param_tree: "<params/>".to_string(),
            shared_param_tree_id: None,
        };
        assert_eq!(sw.name, "pwiz-mzDB");
    }
}
