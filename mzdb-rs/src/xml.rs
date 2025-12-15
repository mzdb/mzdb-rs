//! XML parsing utilities for mzDB files
//!
//! This module provides structures and functions for parsing the XML content
//! stored in various mzDB tables. The XML follows mzML-derived schemas.
//!
//! # XML Fields in mzDB
//!
//! - **param_tree**: Generic params container (cvParams + userParams)
//! - **file_content**: File content description (mzdb table)
//! - **component_list**: Instrument components (instrument_configuration table)
//! - **scan_list**: Scan descriptions (spectrum table)
//! - **precursor_list**: Precursor ion info for MSn (spectrum table)
//! - **product_list**: Product ion info (spectrum table)
//!
//! # Example
//!
//! ```no_run
//! use mzdb::xml::{parse_param_tree, parse_precursor_list};
//!
//! let xml = r#"<params><cvParams><cvParam accession="MS:1000511" value="1"/></cvParams></params>"#;
//! let params = parse_param_tree(xml).unwrap();
//! ```

use anyhow_ext::{anyhow, Result};
use roxmltree::{Document, Node};
use serde::{Deserialize, Serialize};

// ============================================================================
// Core CV/User Parameter Structures
// ============================================================================

/// A controlled vocabulary parameter from mzML/mzDB XML
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct CvParam {
    /// Reference to the CV (e.g., "MS", "UO")
    pub cv_ref: Option<String>,
    /// CV accession number (e.g., "MS:1000511")
    pub accession: String,
    /// Human-readable name
    pub name: Option<String>,
    /// Parameter value (may be empty)
    pub value: Option<String>,
    /// Unit CV reference
    pub unit_cv_ref: Option<String>,
    /// Unit accession (e.g., "UO:0000031")
    pub unit_accession: Option<String>,
    /// Unit name (e.g., "minute")
    pub unit_name: Option<String>,
}

/// A user-defined parameter
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct UserParam {
    /// CV reference (optional)
    pub cv_ref: Option<String>,
    /// Accession (optional, often "MS:-1" for user params)
    pub accession: Option<String>,
    /// Parameter name
    pub name: String,
    /// Parameter value
    pub value: Option<String>,
    /// Data type (e.g., "xsd:float", "xsd:string")
    pub param_type: Option<String>,
    /// Unit CV reference
    pub unit_cv_ref: Option<String>,
    /// Unit accession
    pub unit_accession: Option<String>,
    /// Unit name
    pub unit_name: Option<String>,
}

/// A user-defined text block (for long text content like instrument methods)
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct UserText {
    /// CV reference (optional)
    pub cv_ref: Option<String>,
    /// Accession (optional)
    pub accession: Option<String>,
    /// Text block name (e.g., "instrumentMethods")
    pub name: String,
    /// Data type (e.g., "xsd:string")
    pub text_type: Option<String>,
    /// The actual text content
    pub text: String,
}

/// A container for CV and user parameters (generic param_tree)
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct ParamTree {
    pub cv_params: Vec<CvParam>,
    pub user_params: Vec<UserParam>,
    pub user_texts: Vec<UserText>,
}

// ============================================================================
// File Content Structure (mzdb.file_content)
// ============================================================================

/// File content description from mzdb.file_content
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct FileContent {
    pub cv_params: Vec<CvParam>,
}

// ============================================================================
// Component List Structure (instrument_configuration.component_list)
// ============================================================================

/// An instrument component (source, analyzer, or detector)
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct InstrumentComponent {
    /// Component type: "source", "analyzer", or "detector"
    pub component_type: String,
    /// Order in the component list (1-based)
    pub order: i32,
    /// CV parameters describing the component
    pub cv_params: Vec<CvParam>,
    /// User parameters
    pub user_params: Vec<UserParam>,
}

/// List of instrument components
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct ComponentList {
    pub count: i32,
    pub components: Vec<InstrumentComponent>,
}

// ============================================================================
// Scan List Structure (spectrum.scan_list)
// ============================================================================

/// A scan window (m/z range)
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct ScanWindow {
    /// Lower m/z limit
    pub lower_limit: Option<f64>,
    /// Upper m/z limit
    pub upper_limit: Option<f64>,
    /// All CV params
    pub cv_params: Vec<CvParam>,
}

/// A single scan in the scan list
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct Scan {
    /// Reference to instrument configuration
    pub instrument_configuration_ref: Option<String>,
    /// Scan start time in the unit specified
    pub scan_start_time: Option<f64>,
    /// Time unit (e.g., "minute", "second")
    pub time_unit: Option<String>,
    /// Filter string (vendor-specific)
    pub filter_string: Option<String>,
    /// Ion injection time in milliseconds
    pub ion_injection_time: Option<f64>,
    /// Scan windows
    pub scan_windows: Vec<ScanWindow>,
    /// All CV params
    pub cv_params: Vec<CvParam>,
    /// User params
    pub user_params: Vec<UserParam>,
}

/// List of scans for a spectrum
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct ScanList {
    pub count: i32,
    /// CV params at scanList level (e.g., combination method)
    pub cv_params: Vec<CvParam>,
    pub scans: Vec<Scan>,
}

// ============================================================================
// Precursor List Structure (spectrum.precursor_list)
// ============================================================================

/// Isolation window for precursor selection
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct IsolationWindow {
    /// Target m/z (MS:1000827)
    pub target_mz: Option<f64>,
    /// Lower offset from target (MS:1000828)
    pub lower_offset: Option<f64>,
    /// Upper offset from target (MS:1000829)
    pub upper_offset: Option<f64>,
    /// All CV params
    pub cv_params: Vec<CvParam>,
}

impl IsolationWindow {
    /// Calculate the lower m/z bound
    pub fn min_mz(&self) -> Option<f64> {
        match (self.target_mz, self.lower_offset) {
            (Some(t), Some(l)) => Some(t - l),
            (Some(t), None) => Some(t),
            _ => None,
        }
    }

    /// Calculate the upper m/z bound
    pub fn max_mz(&self) -> Option<f64> {
        match (self.target_mz, self.upper_offset) {
            (Some(t), Some(u)) => Some(t + u),
            (Some(t), None) => Some(t),
            _ => None,
        }
    }
}

/// A selected ion in the precursor
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct SelectedIon {
    /// Selected ion m/z (MS:1000744)
    pub mz: Option<f64>,
    /// Charge state (MS:1000041)
    pub charge: Option<i32>,
    /// Intensity (MS:1000042)
    pub intensity: Option<f64>,
    /// All CV params
    pub cv_params: Vec<CvParam>,
}

/// Activation method for fragmentation
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct Activation {
    /// Collision energy (MS:1000045)
    pub collision_energy: Option<f64>,
    /// Activation type (e.g., "CID", "HCD", "ETD")
    pub activation_type: Option<String>,
    /// All CV params
    pub cv_params: Vec<CvParam>,
}

/// A precursor ion description
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct Precursor {
    /// Reference to the precursor spectrum
    pub spectrum_ref: Option<String>,
    /// Isolation window
    pub isolation_window: Option<IsolationWindow>,
    /// Selected ions
    pub selected_ions: Vec<SelectedIon>,
    /// Activation method
    pub activation: Option<Activation>,
}

/// List of precursors for MSn spectra
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct PrecursorList {
    pub count: i32,
    pub precursors: Vec<Precursor>,
}

// ============================================================================
// Product List Structure (spectrum.product_list)
// ============================================================================

/// A product ion description
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct Product {
    /// Isolation window for product selection
    pub isolation_window: Option<IsolationWindow>,
}

/// List of products
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct ProductList {
    pub count: i32,
    pub products: Vec<Product>,
}

// ============================================================================
// Parsing Helper Functions
// ============================================================================

/// Parse a CV param from an XML node
fn parse_cv_param_node(node: &Node) -> CvParam {
    CvParam {
        cv_ref: node.attribute("cvRef").map(String::from),
        accession: node.attribute("accession").unwrap_or("").to_string(),
        name: node.attribute("name").map(String::from),
        value: node.attribute("value").map(String::from),
        unit_cv_ref: node.attribute("unitCvRef").map(String::from),
        unit_accession: node.attribute("unitAccession").map(String::from),
        unit_name: node.attribute("unitName").map(String::from),
    }
}

/// Parse a user param from an XML node
fn parse_user_param_node(node: &Node) -> UserParam {
    UserParam {
        cv_ref: node.attribute("cvRef").map(String::from),
        accession: node.attribute("accession").map(String::from),
        name: node.attribute("name").unwrap_or("").to_string(),
        value: node.attribute("value").map(String::from),
        param_type: node.attribute("type").map(String::from),
        unit_cv_ref: node.attribute("unitCvRef").map(String::from),
        unit_accession: node.attribute("unitAccession").map(String::from),
        unit_name: node.attribute("unitName").map(String::from),
    }
}

/// Parse a user text from an XML node
fn parse_user_text_node(node: &Node) -> UserText {
    UserText {
        cv_ref: node.attribute("cvRef").map(String::from),
        accession: node.attribute("accession").map(String::from),
        name: node.attribute("name").unwrap_or("").to_string(),
        text_type: node.attribute("type").map(String::from),
        text: node.text().unwrap_or("").to_string(),
    }
}

/// Collect all cvParam nodes from descendants
fn collect_cv_params(node: &Node) -> Vec<CvParam> {
    node.descendants()
        .filter(|n| n.tag_name().name() == "cvParam")
        .map(|n| parse_cv_param_node(&n))
        .collect()
}

/// Collect all userParam nodes from descendants
fn collect_user_params(node: &Node) -> Vec<UserParam> {
    node.descendants()
        .filter(|n| n.tag_name().name() == "userParam")
        .map(|n| parse_user_param_node(&n))
        .collect()
}

/// Collect all userText nodes from descendants
fn collect_user_texts(node: &Node) -> Vec<UserText> {
    node.descendants()
        .filter(|n| n.tag_name().name() == "userText")
        .map(|n| parse_user_text_node(&n))
        .collect()
}

/// Find a CV param by accession and parse its value as f64
fn find_cv_param_f64(cv_params: &[CvParam], accession: &str) -> Option<f64> {
    cv_params
        .iter()
        .find(|p| p.accession == accession)
        .and_then(|p| p.value.as_ref())
        .and_then(|v| v.parse().ok())
}

/// Find a CV param by accession and parse its value as i32
fn find_cv_param_i32(cv_params: &[CvParam], accession: &str) -> Option<i32> {
    cv_params
        .iter()
        .find(|p| p.accession == accession)
        .and_then(|p| p.value.as_ref())
        .and_then(|v| v.parse().ok())
}

/// Find a CV param by accession and get its name
fn find_cv_param_name(cv_params: &[CvParam], accession: &str) -> Option<String> {
    cv_params
        .iter()
        .find(|p| p.accession == accession)
        .and_then(|p| p.name.clone())
}

// ============================================================================
// Main Parsing Functions
// ============================================================================

/// Parse a generic param_tree XML string
///
/// Expected format:
/// ```xml
/// <params>
///   <cvParams>
///     <cvParam cvRef="MS" accession="MS:..." name="..." value="..." />
///   </cvParams>
///   <userParams>
///     <userParam cvRef="MS" accession="MS:-1" name="..." value="..." type="xsd:..." />
///   </userParams>
///   <userTexts>
///     <userText cvRef="MS" accession="MS:-1" name="..." type="xsd:string">Long text content...</userText>
///   </userTexts>
/// </params>
/// ```
pub fn parse_param_tree(xml: &str) -> Result<ParamTree> {
    if xml.trim().is_empty() {
        return Ok(ParamTree::default());
    }

    let doc = Document::parse(xml)?;
    let root = doc.root_element();

    Ok(ParamTree {
        cv_params: collect_cv_params(&root),
        user_params: collect_user_params(&root),
        user_texts: collect_user_texts(&root),
    })
}

/// Parse file_content XML string
///
/// Expected format:
/// ```xml
/// <fileContent>
///   <cvParams>
///     <cvParam cvRef="MS" accession="MS:..." name="..." value="" />
///   </cvParams>
/// </fileContent>
/// ```
pub fn parse_file_content(xml: &str) -> Result<FileContent> {
    if xml.trim().is_empty() {
        return Ok(FileContent::default());
    }

    let doc = Document::parse(xml)?;
    let root = doc.root_element();

    Ok(FileContent {
        cv_params: collect_cv_params(&root),
    })
}

/// Parse component_list XML string
///
/// Expected format:
/// ```xml
/// <componentList count="3">
///   <source order="1"><cvParams>...</cvParams></source>
///   <analyzer order="2"><cvParams>...</cvParams></analyzer>
///   <detector order="3"><cvParams>...</cvParams></detector>
/// </componentList>
/// ```
pub fn parse_component_list(xml: &str) -> Result<ComponentList> {
    if xml.trim().is_empty() {
        return Ok(ComponentList::default());
    }

    let doc = Document::parse(xml)?;
    let root = doc.root_element();

    let count = root
        .attribute("count")
        .and_then(|v| v.parse().ok())
        .unwrap_or(0);

    let mut components = Vec::new();

    for component_type in &["source", "analyzer", "detector"] {
        for node in root.children().filter(|n| n.tag_name().name() == *component_type) {
            let order = node
                .attribute("order")
                .and_then(|v| v.parse().ok())
                .unwrap_or(0);

            components.push(InstrumentComponent {
                component_type: component_type.to_string(),
                order,
                cv_params: collect_cv_params(&node),
                user_params: collect_user_params(&node),
            });
        }
    }

    // Sort by order
    components.sort_by_key(|c| c.order);

    Ok(ComponentList { count, components })
}

/// Parse scan_list XML string
///
/// Expected format:
/// ```xml
/// <scanList count="1">
///   <cvParam ... />
///   <scan instrumentConfigurationRef="IC1">
///     <cvParam ... />
///     <scanWindowList count="1">
///       <scanWindow>
///         <cvParam accession="MS:1000501" value="200" name="scan window lower limit" />
///         <cvParam accession="MS:1000500" value="2000" name="scan window upper limit" />
///       </scanWindow>
///     </scanWindowList>
///   </scan>
/// </scanList>
/// ```
pub fn parse_scan_list(xml: &str) -> Result<ScanList> {
    if xml.trim().is_empty() {
        return Ok(ScanList::default());
    }

    let doc = Document::parse(xml)?;
    let root = doc.root_element();

    let count = root
        .attribute("count")
        .and_then(|v| v.parse().ok())
        .unwrap_or(0);

    // Get CV params at scanList level (direct children only)
    let cv_params: Vec<CvParam> = root
        .children()
        .filter(|n| n.tag_name().name() == "cvParam")
        .map(|n| parse_cv_param_node(&n))
        .collect();

    let mut scans = Vec::new();

    for scan_node in root.children().filter(|n| n.tag_name().name() == "scan") {
        let scan_cv_params = collect_cv_params(&scan_node);
        let scan_user_params = collect_user_params(&scan_node);

        // Parse scan windows
        let mut scan_windows = Vec::new();
        for swl_node in scan_node
            .children()
            .filter(|n| n.tag_name().name() == "scanWindowList")
        {
            for sw_node in swl_node
                .children()
                .filter(|n| n.tag_name().name() == "scanWindow")
            {
                let sw_cv_params = collect_cv_params(&sw_node);
                scan_windows.push(ScanWindow {
                    lower_limit: find_cv_param_f64(&sw_cv_params, "MS:1000501"),
                    upper_limit: find_cv_param_f64(&sw_cv_params, "MS:1000500"),
                    cv_params: sw_cv_params,
                });
            }
        }

        scans.push(Scan {
            instrument_configuration_ref: scan_node
                .attribute("instrumentConfigurationRef")
                .map(String::from),
            scan_start_time: find_cv_param_f64(&scan_cv_params, "MS:1000016"),
            time_unit: scan_cv_params
                .iter()
                .find(|p| p.accession == "MS:1000016")
                .and_then(|p| p.unit_name.clone()),
            filter_string: scan_cv_params
                .iter()
                .find(|p| p.accession == "MS:1000512")
                .and_then(|p| p.value.clone()),
            ion_injection_time: find_cv_param_f64(&scan_cv_params, "MS:1000927"),
            scan_windows,
            cv_params: scan_cv_params,
            user_params: scan_user_params,
        });
    }

    Ok(ScanList { count, cv_params, scans })
}

/// Parse precursor_list XML string
///
/// Expected format:
/// ```xml
/// <precursorList count="1">
///   <precursor spectrumRef="...">
///     <isolationWindow>
///       <cvParam accession="MS:1000827" value="..." name="isolation window target m/z" />
///       <cvParam accession="MS:1000828" value="..." name="isolation window lower offset" />
///       <cvParam accession="MS:1000829" value="..." name="isolation window upper offset" />
///     </isolationWindow>
///     <selectedIonList count="1">
///       <selectedIon>
///         <cvParam accession="MS:1000744" value="..." name="selected ion m/z" />
///       </selectedIon>
///     </selectedIonList>
///     <activation>
///       <cvParam accession="MS:1000045" value="..." name="collision energy" />
///       <cvParam accession="MS:1000133" name="collision-induced dissociation" />
///     </activation>
///   </precursor>
/// </precursorList>
/// ```
pub fn parse_precursor_list(xml: &str) -> Result<PrecursorList> {
    if xml.trim().is_empty() {
        return Ok(PrecursorList::default());
    }

    let doc = Document::parse(xml)?;
    let root = doc.root_element();

    let count = root
        .attribute("count")
        .and_then(|v| v.parse().ok())
        .unwrap_or(0);

    let mut precursors = Vec::new();

    for prec_node in root.children().filter(|n| n.tag_name().name() == "precursor") {
        // Parse isolation window
        let isolation_window = prec_node
            .children()
            .find(|n| n.tag_name().name() == "isolationWindow")
            .map(|iw_node| {
                let cv_params = collect_cv_params(&iw_node);
                IsolationWindow {
                    target_mz: find_cv_param_f64(&cv_params, "MS:1000827"),
                    lower_offset: find_cv_param_f64(&cv_params, "MS:1000828"),
                    upper_offset: find_cv_param_f64(&cv_params, "MS:1000829"),
                    cv_params,
                }
            });

        // Parse selected ions
        let mut selected_ions = Vec::new();
        for sil_node in prec_node
            .children()
            .filter(|n| n.tag_name().name() == "selectedIonList")
        {
            for si_node in sil_node
                .children()
                .filter(|n| n.tag_name().name() == "selectedIon")
            {
                let cv_params = collect_cv_params(&si_node);
                selected_ions.push(SelectedIon {
                    mz: find_cv_param_f64(&cv_params, "MS:1000744"),
                    charge: find_cv_param_i32(&cv_params, "MS:1000041"),
                    intensity: find_cv_param_f64(&cv_params, "MS:1000042"),
                    cv_params,
                });
            }
        }

        // Parse activation
        let activation = prec_node
            .children()
            .find(|n| n.tag_name().name() == "activation")
            .map(|act_node| {
                let cv_params = collect_cv_params(&act_node);

                // Determine activation type from CV params
                let activation_type = cv_params
                    .iter()
                    .find(|p| {
                        // Common activation type accessions
                        matches!(
                            p.accession.as_str(),
                            "MS:1000133" | // CID
                            "MS:1000422" | // HCD
                            "MS:1000598" | // ETD
                            "MS:1000599" | // ECD
                            "MS:1002631" | // EThcD
                            "MS:1000435"   // PQD
                        )
                    })
                    .and_then(|p| p.name.clone());

                Activation {
                    collision_energy: find_cv_param_f64(&cv_params, "MS:1000045"),
                    activation_type,
                    cv_params,
                }
            });

        precursors.push(Precursor {
            spectrum_ref: prec_node.attribute("spectrumRef").map(String::from),
            isolation_window,
            selected_ions,
            activation,
        });
    }

    Ok(PrecursorList { count, precursors })
}

/// Parse product_list XML string
pub fn parse_product_list(xml: &str) -> Result<ProductList> {
    if xml.trim().is_empty() {
        return Ok(ProductList::default());
    }

    let doc = Document::parse(xml)?;
    let root = doc.root_element();

    let count = root
        .attribute("count")
        .and_then(|v| v.parse().ok())
        .unwrap_or(0);

    let mut products = Vec::new();

    for prod_node in root.children().filter(|n| n.tag_name().name() == "product") {
        let isolation_window = prod_node
            .children()
            .find(|n| n.tag_name().name() == "isolationWindow")
            .map(|iw_node| {
                let cv_params = collect_cv_params(&iw_node);
                IsolationWindow {
                    target_mz: find_cv_param_f64(&cv_params, "MS:1000827"),
                    lower_offset: find_cv_param_f64(&cv_params, "MS:1000828"),
                    upper_offset: find_cv_param_f64(&cv_params, "MS:1000829"),
                    cv_params,
                }
            });

        products.push(Product { isolation_window });
    }

    Ok(ProductList { count, products })
}

// ============================================================================
// Convenience Functions
// ============================================================================

/// Extract isolation window bounds from precursor_list XML
/// Returns (min_mz, max_mz) for the first precursor
pub fn extract_isolation_window(precursor_list_xml: &str) -> Option<(f64, f64)> {
    let prec_list = parse_precursor_list(precursor_list_xml).ok()?;
    let precursor = prec_list.precursors.first()?;
    let iw = precursor.isolation_window.as_ref()?;
    Some((iw.min_mz()?, iw.max_mz()?))
}

/// Extract selected ion m/z from precursor_list XML
pub fn extract_selected_ion_mz(precursor_list_xml: &str) -> Option<f64> {
    let prec_list = parse_precursor_list(precursor_list_xml).ok()?;
    let precursor = prec_list.precursors.first()?;
    precursor.selected_ions.first()?.mz
}

/// Extract collision energy from precursor_list XML
pub fn extract_collision_energy(precursor_list_xml: &str) -> Option<f64> {
    let prec_list = parse_precursor_list(precursor_list_xml).ok()?;
    let precursor = prec_list.precursors.first()?;
    precursor.activation.as_ref()?.collision_energy
}

/// Extract scan start time from scan_list XML (in minutes)
pub fn extract_scan_time(scan_list_xml: &str) -> Option<f64> {
    let scan_list = parse_scan_list(scan_list_xml).ok()?;
    scan_list.scans.first()?.scan_start_time
}

/// Find a CV param value by accession in param_tree XML
pub fn find_param_value(param_tree_xml: &str, accession: &str) -> Option<String> {
    let params = parse_param_tree(param_tree_xml).ok()?;
    params
        .cv_params
        .iter()
        .find(|p| p.accession == accession)
        .and_then(|p| p.value.clone())
}

/// Find a user param value by name in param_tree XML
pub fn find_user_param_value(param_tree_xml: &str, name: &str) -> Option<String> {
    let params = parse_param_tree(param_tree_xml).ok()?;
    params
        .user_params
        .iter()
        .find(|p| p.name == name)
        .and_then(|p| p.value.clone())
}

/// Find a user text content by name in param_tree XML
pub fn find_user_text(param_tree_xml: &str, name: &str) -> Option<String> {
    let params = parse_param_tree(param_tree_xml).ok()?;
    params
        .user_texts
        .iter()
        .find(|t| t.name == name)
        .map(|t| t.text.clone())
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_param_tree() {
        let xml = r#"<params>
            <cvParams>
                <cvParam cvRef="MS" accession="MS:1000511" value="1" name="ms level" />
            </cvParams>
            <userParams>
                <userParam cvRef="MS" accession="MS:-1" name="ms1_bb_mz_width" value="5.0" type="xsd:float" />
            </userParams>
        </params>"#;

        let result = parse_param_tree(xml).unwrap();
        assert_eq!(result.cv_params.len(), 1);
        assert_eq!(result.cv_params[0].accession, "MS:1000511");
        assert_eq!(result.cv_params[0].value, Some("1".to_string()));
        assert_eq!(result.user_params.len(), 1);
        assert_eq!(result.user_params[0].name, "ms1_bb_mz_width");
        assert_eq!(result.user_params[0].cv_ref, Some("MS".to_string()));
        assert_eq!(result.user_params[0].accession, Some("MS:-1".to_string()));
        assert_eq!(result.user_texts.len(), 0);
    }

    #[test]
    fn test_parse_param_tree_with_user_texts() {
        let xml = r#"<params>
            <userParams>
                <userParam cvRef="MS" accession="MS:-1" name="ms1_bb_mz_width" type="xsd:float" value="5" />
            </userParams>
            <userTexts>
                <userText cvRef="MS" accession="MS:-1" name="instrumentMethods" type="xsd:string">Method content here</userText>
            </userTexts>
        </params>"#;

        let result = parse_param_tree(xml).unwrap();
        assert_eq!(result.user_params.len(), 1);
        assert_eq!(result.user_texts.len(), 1);
        assert_eq!(result.user_texts[0].name, "instrumentMethods");
        assert_eq!(result.user_texts[0].text, "Method content here");
        assert_eq!(result.user_texts[0].text_type, Some("xsd:string".to_string()));
    }

    #[test]
    fn test_parse_file_content() {
        let xml = r#"<fileContent>
            <cvParams>
                <cvParam cvRef="MS" accession="MS:1000579" name="MS1 spectrum" value="" />
                <cvParam cvRef="MS" accession="MS:1000580" name="MSn spectrum" value="" />
            </cvParams>
        </fileContent>"#;

        let result = parse_file_content(xml).unwrap();
        assert_eq!(result.cv_params.len(), 2);
        assert_eq!(result.cv_params[0].accession, "MS:1000579");
    }

    #[test]
    fn test_parse_component_list() {
        let xml = r#"<componentList count="3">
            <source order="1">
                <cvParams>
                    <cvParam cvRef="MS" accession="MS:1000073" name="electrospray ionization" value="" />
                </cvParams>
            </source>
            <analyzer order="2">
                <cvParams>
                    <cvParam cvRef="MS" accession="MS:1000079" name="fourier transform ion cyclotron resonance mass spectrometer" value="" />
                </cvParams>
            </analyzer>
            <detector order="3">
                <cvParams>
                    <cvParam cvRef="MS" accession="MS:1000624" name="inductive detector" value="" />
                </cvParams>
            </detector>
        </componentList>"#;

        let result = parse_component_list(xml).unwrap();
        assert_eq!(result.count, 3);
        assert_eq!(result.components.len(), 3);
        assert_eq!(result.components[0].component_type, "source");
        assert_eq!(result.components[1].component_type, "analyzer");
        assert_eq!(result.components[2].component_type, "detector");
    }

    #[test]
    fn test_parse_precursor_list() {
        let xml = r#"<precursorList count="1">
            <precursor spectrumRef="scan=1">
                <isolationWindow>
                    <cvParam cvRef="MS" accession="MS:1000827" value="810.79" name="isolation window target m/z" />
                    <cvParam cvRef="MS" accession="MS:1000828" value="1" name="isolation window lower offset" />
                    <cvParam cvRef="MS" accession="MS:1000829" value="1" name="isolation window upper offset" />
                </isolationWindow>
                <selectedIonList count="1">
                    <selectedIon>
                        <cvParam cvRef="MS" accession="MS:1000744" value="810.79" name="selected ion m/z" />
                    </selectedIon>
                </selectedIonList>
                <activation>
                    <cvParam cvRef="MS" accession="MS:1000045" value="35" name="collision energy" />
                    <cvParam cvRef="MS" accession="MS:1000133" value="" name="collision-induced dissociation" />
                </activation>
            </precursor>
        </precursorList>"#;

        let result = parse_precursor_list(xml).unwrap();
        assert_eq!(result.count, 1);
        assert_eq!(result.precursors.len(), 1);

        let prec = &result.precursors[0];
        assert_eq!(prec.spectrum_ref, Some("scan=1".to_string()));

        let iw = prec.isolation_window.as_ref().unwrap();
        assert_eq!(iw.target_mz, Some(810.79));
        assert_eq!(iw.lower_offset, Some(1.0));
        assert_eq!(iw.upper_offset, Some(1.0));
        assert_eq!(iw.min_mz(), Some(809.79));
        assert_eq!(iw.max_mz(), Some(811.79));

        assert_eq!(prec.selected_ions[0].mz, Some(810.79));

        let act = prec.activation.as_ref().unwrap();
        assert_eq!(act.collision_energy, Some(35.0));
        assert_eq!(act.activation_type, Some("collision-induced dissociation".to_string()));
    }

    #[test]
    fn test_extract_isolation_window() {
        let xml = r#"<precursorList count="1">
            <precursor>
                <isolationWindow>
                    <cvParam accession="MS:1000827" value="500.5" />
                    <cvParam accession="MS:1000828" value="0.5" />
                    <cvParam accession="MS:1000829" value="0.5" />
                </isolationWindow>
            </precursor>
        </precursorList>"#;

        let result = extract_isolation_window(xml);
        assert_eq!(result, Some((500.0, 501.0)));
    }

    #[test]
    fn test_empty_xml() {
        assert!(parse_param_tree("").unwrap().cv_params.is_empty());
        assert!(parse_file_content("").unwrap().cv_params.is_empty());
        assert_eq!(parse_component_list("").unwrap().count, 0);
        assert_eq!(parse_scan_list("").unwrap().count, 0);
        assert_eq!(parse_precursor_list("").unwrap().count, 0);
    }
}
