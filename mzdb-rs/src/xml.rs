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
#![allow(unused)]

use anyhow_ext::{Result};
use compact_str::CompactString;
use roxmltree::{Document, Node};
use serde::{Deserialize, Serialize};
use std::fmt;
use std::str::FromStr;

use crate::model::MzRange;

// ============================================================================
// PSI-MS Controlled Vocabulary Constants
// ============================================================================

// Data encoding
//const PSI_MS_32_BIT_FLOAT: &str = "MS:1000521";
//const PSI_MS_64_BIT_FLOAT: &str = "MS:1000523";

// Acquisition
//const ACQUISITION_PARAMETER: &str = "MS:1001954";

// Isolation window
const ISOLATION_WINDOW_TARGET_MZ: &str = "MS:1000827";
const ISOLATION_WINDOW_LOWER_OFFSET: &str = "MS:1000828";
const ISOLATION_WINDOW_UPPER_OFFSET: &str = "MS:1000829";

// Selected ion
const SELECTED_ION_MZ: &str = "MS:1000744";
const CHARGE_STATE: &str = "MS:1000041";
const PEAK_INTENSITY: &str = "MS:1000042";

// Activation
const COLLISION_ENERGY: &str = "MS:1000045";
const COLLISION_INDUCED_DISSOCIATION: &str = "MS:1000133";
const HCD: &str = "MS:1000422";
const ETD: &str = "MS:1000598";
const ECD: &str = "MS:1000599";
const ETHCD: &str = "MS:1002631";
const PQD: &str = "MS:1000435";

// Scan parameters
const SCAN_START_TIME: &str = "MS:1000016";
const FILTER_STRING: &str = "MS:1000512";
const ION_INJECTION_TIME: &str = "MS:1000927";
const SCAN_WINDOW_LOWER_LIMIT: &str = "MS:1000501";
const SCAN_WINDOW_UPPER_LIMIT: &str = "MS:1000500";

// ============================================================================
// CV Reference Enum
// ============================================================================

/// Controlled vocabulary reference identifier.
///
/// Represents the ontology a CV term belongs to. Uses a compact enum (1 byte)
/// instead of a String since only a few ontologies are used in mzDB/mzML.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum CvRef {
    /// PSI Mass Spectrometry Ontology
    #[default]
    MS,
    /// PSI Ontology (generic)
    PSI,
    /// Unit Ontology
    UO,
}

impl fmt::Display for CvRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CvRef::MS => write!(f, "MS"),
            CvRef::PSI => write!(f, "PSI"),
            CvRef::UO => write!(f, "UO"),
        }
    }
}

impl FromStr for CvRef {
    type Err = ();
    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s {
            "MS" => Ok(CvRef::MS),
            "PSI" | "psi_ms" | "psi-ms" => Ok(CvRef::PSI),
            "UO" => Ok(CvRef::UO),
            _ => Err(()),
        }
    }
}

impl From<&str> for CvRef {
    /// Parse a CV reference string, defaulting to MS for unknown values.
    fn from(s: &str) -> Self {
        CvRef::from_str(s).unwrap_or(CvRef::MS)
    }
}

// ============================================================================
// CV Unit
// ============================================================================

/// Unit reference for a CV parameter (e.g. minutes, m/z, milliseconds).
///
/// Unit fields always come as a triplet; grouping them in a struct enforces
/// this invariant. 56 bytes with CompactString; `Option<CvUnit>` is also 56
/// bytes thanks to niche optimization.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct CvUnit {
    /// Unit CV reference (e.g. UO, MS)
    pub cv_ref: CvRef,
    /// Unit accession (e.g. "UO:0000031")
    pub accession: CompactString,
    /// Unit name (e.g. "minute")
    pub name: CompactString,
}

impl CvUnit {
    pub fn new(cv_ref: CvRef, accession: &str, name: &str) -> Self {
        Self {
            cv_ref,
            accession: CompactString::from(accession),
            name: CompactString::from(name),
        }
    }
}

// ============================================================================
// Core CV/User Parameter Structures
// ============================================================================

/// A controlled vocabulary parameter from mzML/mzDB XML.
///
/// 136 bytes with CompactString + CvRef enum (vs 168 with all String).
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct CvParam {
    /// Reference to the CV (e.g., MS, UO)
    pub cv_ref: Option<CvRef>,
    /// CV accession number (e.g., "MS:1000511")
    pub accession: CompactString,
    /// Human-readable name
    pub name: Option<CompactString>,
    /// Parameter value (may be empty)
    pub value: Option<CompactString>,
    /// Unit reference (grouped; None when no unit)
    pub unit: Option<CvUnit>,
}

impl CvParam {
    /// Create a CV parameter without units
    pub fn new(cv_ref: CvRef, accession: &str, name: &str, value: &str) -> Self {
        Self {
            cv_ref: Some(cv_ref),
            accession: CompactString::from(accession),
            name: Some(CompactString::from(name)),
            value: if value.is_empty() { None } else { Some(CompactString::from(value)) },
            unit: None,
        }
    }

    /// Create a CV parameter with units
    pub fn new_with_unit(
        cv_ref: CvRef,
        accession: &str,
        name: &str,
        value: &str,
        unit_cv_ref: CvRef,
        unit_accession: &str,
        unit_name: &str,
    ) -> Self {
        Self {
            cv_ref: Some(cv_ref),
            accession: CompactString::from(accession),
            name: Some(CompactString::from(name)),
            value: if value.is_empty() { None } else { Some(CompactString::from(value)) },
            unit: Some(CvUnit::new(unit_cv_ref, unit_accession, unit_name)),
        }
    }
}

/// A user-defined parameter (no CV equivalent).
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct UserParam {
    /// CV reference (optional)
    pub cv_ref: Option<CvRef>,
    /// Accession (optional, often "MS:-1" for user params)
    pub accession: Option<CompactString>,
    /// Parameter name
    pub name: CompactString,
    /// Parameter value
    pub value: Option<CompactString>,
    /// Data type (e.g., "xsd:float", "xsd:string")
    pub param_type: Option<CompactString>,
    /// Unit reference (grouped; None when no unit)
    pub unit: Option<CvUnit>,
}

impl UserParam {
    /// Create a user parameter with type xsd:string and default MS cv_ref
    pub fn new(name: &str, value: &str) -> Self {
        Self {
            cv_ref: Some(CvRef::MS),
            accession: None,
            name: CompactString::from(name),
            value: if value.is_empty() { None } else { Some(CompactString::from(value)) },
            param_type: Some(CompactString::from("xsd:string")),
            unit: None,
        }
    }

    /// Create a user parameter with an explicit type
    pub fn new_with_type(name: &str, value: &str, param_type: &str) -> Self {
        Self {
            cv_ref: Some(CvRef::MS),
            accession: None,
            name: CompactString::from(name),
            value: if value.is_empty() { None } else { Some(CompactString::from(value)) },
            param_type: Some(CompactString::from(param_type)),
            unit: None,
        }
    }
}

/// A user-defined text block (for long text content like instrument methods)
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct UserText {
    /// CV reference (optional)
    pub cv_ref: Option<CvRef>,
    /// Accession (optional)
    pub accession: Option<CompactString>,
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

// ============================================================================
// Thermo scan filter string
// ============================================================================

/// One fragmentation event described by a Thermo scan filter string.
///
/// Port of `fr.profi.mzdb.db.model.params.thermo.ThermoFragmentationTarget` (mzdb-access).
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct ThermoFragmentationTarget {
    /// The MS level this target was isolated *at* — 1 for the precursor selected from the MS1
    /// survey, 2 for the one selected from the MS2 spectrum, and so on. Note this is one less than
    /// the level of the spectrum describing it.
    pub ms_level: i32,
    /// Isolation m/z.
    pub mz: f64,
    /// Activation method as written in the filter string, lowercase (`cid`, `hcd`, `etd`...).
    pub activation_type: String,
    pub collision_energy: f32,
}

/// Metadata decoded from a Thermo scan filter string.
///
/// Port of `fr.profi.mzdb.db.model.params.thermo.ThermoScanMetaData` (mzdb-access), reached there
/// through `ScanParamTree.getThermoMetaData()`. Unlike the rest of this module the source is not XML
/// but a fixed-grammar vendor string, carried in the `filterString` CV param (`MS:1000512`) and
/// already extracted into [`Scan::filter_string`].
///
/// Examples, from the reference's own comments:
///
/// ```text
/// MS2: ITMS + c NSI d Full ms2 476.20@cid30.00 [120.00-1440.00]
/// MS3: FTMS + p NSI sps d Full ms3 707.8472@cid35.00 463.3669@hcd45.00 [115.0000-140.0000]
/// ```
///
/// The MS3 case is what makes this worth parsing: isobaric quantification of MS3 data pairs each MS3
/// spectrum to its MS2 parent by comparing this structure's first target m/z against the MS2's
/// isolation-window target m/z, and there is no other source for that value.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct ThermoScanMetaData {
    /// Everything up to and including `Full msN`, e.g. `"FTMS + p NSI sps d Full ms3"`.
    pub acquisition_type: String,
    /// The leading analyzer token, e.g. `"FTMS"` or `"ITMS"`.
    pub analyzer_type: String,
    pub ms_level: i32,
    /// Scan range `[low, high]` from the trailing bracketed pair.
    pub mz_range: [f32; 2],
    /// One target per fragmentation step: 1 for an MS2 filter, 2 for an MS3 filter.
    pub targets: Vec<ThermoFragmentationTarget>,
}

impl ThermoScanMetaData {
    /// Parse a Thermo filter string.
    ///
    /// # Errors
    ///
    /// Returns an error when the string does not have the expected shape. The Java reference instead
    /// leaves the targets array full of nulls and the range at `[0, 0]` when its regex does not
    /// match, which pushes the failure to whatever dereferences a target later; failing here reports
    /// it where the cause is visible.
    pub fn parse(filter_string: &str) -> Result<Self> {
        use anyhow_ext::bail;

        // Everything hinges on the `Full msN` marker: it separates the instrument description from
        // the fragmentation description, and its digit is the MS level.
        let Some((left, right)) = filter_string.split_once("Full ms") else {
            bail!("not a Thermo filter string (no `Full ms` marker): {filter_string:?}");
        };

        let ms_level_char = right.chars().next().unwrap_or(' ');
        let Some(ms_level) = ms_level_char.to_digit(10).map(|d| d as i32) else {
            bail!("no MS level digit after `Full ms` in {filter_string:?}");
        };

        let acquisition_type = format!("{left}Full ms{ms_level_char}");
        let analyzer_type =
            left.split_whitespace().next().unwrap_or_default().to_string();

        // After the level digit come `mz@methodEnergy` groups, then a bracketed range.
        let rest = &right[ms_level_char.len_utf8()..];

        let mut targets = Vec::new();
        for (index, token) in rest.split_whitespace().filter(|t| t.contains('@')).enumerate() {
            let Some((mz_str, activation)) = token.split_once('@') else {
                continue;
            };

            let mz: f64 = mz_str
                .parse()
                .map_err(|_| anyhow_ext::anyhow!("bad target m/z {mz_str:?} in {filter_string:?}"))?;

            // `cid30.00` -> ("cid", 30.00): the method is the leading alphabetic run.
            let split_at = activation
                .find(|c: char| !c.is_ascii_alphabetic())
                .unwrap_or(activation.len());
            let (activation_type, energy_str) = activation.split_at(split_at);

            let collision_energy: f32 = energy_str.parse().map_err(|_| {
                anyhow_ext::anyhow!("bad collision energy {energy_str:?} in {filter_string:?}")
            })?;

            targets.push(ThermoFragmentationTarget {
                // The first target is the precursor selected from MS1, hence level 1.
                ms_level: index as i32 + 1,
                mz,
                activation_type: activation_type.to_string(),
                collision_energy,
            });
        }

        if targets.is_empty() {
            bail!("no fragmentation targets found in {filter_string:?}");
        }

        // Trailing `[low-high]`.
        let mz_range = match rest.rsplit_once('[') {
            Some((_, bracketed)) => {
                let bracketed = bracketed.trim_end_matches(']');
                match bracketed.split_once('-') {
                    Some((low, high)) => {
                        let low: f32 = low.trim().parse().map_err(|_| {
                            anyhow_ext::anyhow!("bad range start {low:?} in {filter_string:?}")
                        })?;
                        let high: f32 = high.trim().parse().map_err(|_| {
                            anyhow_ext::anyhow!("bad range end {high:?} in {filter_string:?}")
                        })?;
                        [low, high]
                    }
                    None => bail!("malformed scan range in {filter_string:?}"),
                }
            }
            None => bail!("no scan range in {filter_string:?}"),
        };

        Ok(Self {
            acquisition_type,
            analyzer_type,
            ms_level,
            mz_range,
            targets,
        })
    }
}

impl Scan {
    /// Decode this scan's Thermo filter string, when it has one.
    ///
    /// Port of `ScanParamTree.getThermoMetaData()`. `None` means the scan carries no `filterString`
    /// CV param — a non-Thermo instrument, or a file that did not record it — which the reference
    /// signals by returning `null` and is not an error. A `Some(Err(..))` means the string was
    /// present but unparseable.
    pub fn thermo_meta_data(&self) -> Option<Result<ThermoScanMetaData>> {
        self.filter_string.as_deref().map(ThermoScanMetaData::parse)
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
    let unit = match (node.attribute("unitCvRef"), node.attribute("unitAccession"), node.attribute("unitName")) {
        (Some(cv_ref), Some(acc), Some(name)) => Some(CvUnit {
            cv_ref: CvRef::from(cv_ref),
            accession: CompactString::from(acc),
            name: CompactString::from(name),
        }),
        _ => None,
    };
    CvParam {
        cv_ref: node.attribute("cvRef").map(CvRef::from),
        accession: CompactString::from(node.attribute("accession").unwrap_or("")),
        name: node.attribute("name").map(CompactString::from),
        value: node.attribute("value").map(CompactString::from),
        unit,
    }
}

/// Parse a user param from an XML node
fn parse_user_param_node(node: &Node) -> UserParam {
    let unit = match (node.attribute("unitCvRef"), node.attribute("unitAccession"), node.attribute("unitName")) {
        (Some(cv_ref), Some(acc), Some(name)) => Some(CvUnit {
            cv_ref: CvRef::from(cv_ref),
            accession: CompactString::from(acc),
            name: CompactString::from(name),
        }),
        _ => None,
    };
    UserParam {
        cv_ref: node.attribute("cvRef").map(CvRef::from),
        accession: node.attribute("accession").map(CompactString::from),
        name: CompactString::from(node.attribute("name").unwrap_or("")),
        value: node.attribute("value").map(CompactString::from),
        param_type: node.attribute("type").map(CompactString::from),
        unit,
    }
}

/// Parse a user text from an XML node
fn parse_user_text_node(node: &Node) -> UserText {
    UserText {
        cv_ref: node.attribute("cvRef").map(CvRef::from),
        accession: node.attribute("accession").map(CompactString::from),
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

/// Find a CV param by accession and parse its value
fn find_cv_param_value<T: std::str::FromStr>(cv_params: &[CvParam], accession: &str) -> Option<T> {
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
        .and_then(|p| p.name.as_ref().map(|n| n.to_string()))
}

/// Parse an XML document and get the root element's count attribute
/// Returns None if XML is empty, otherwise returns (Document, count)
fn parse_xml_with_count(xml: &str) -> Result<Option<(Document<'_>, i32)>> {
    if xml.trim().is_empty() {
        return Ok(None);
    }
    let doc = Document::parse(xml)?;
    let count = doc
        .root_element()
        .attribute("count")
        .and_then(|v| v.parse().ok())
        .unwrap_or(0);
    Ok(Some((doc, count)))
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
                    lower_limit: find_cv_param_value(&sw_cv_params, SCAN_WINDOW_LOWER_LIMIT),
                    upper_limit: find_cv_param_value(&sw_cv_params, SCAN_WINDOW_UPPER_LIMIT),
                    cv_params: sw_cv_params,
                });
            }
        }

        scans.push(Scan {
            instrument_configuration_ref: scan_node
                .attribute("instrumentConfigurationRef")
                .map(String::from),
            scan_start_time: find_cv_param_value(&scan_cv_params, SCAN_START_TIME),
            time_unit: scan_cv_params
                .iter()
                .find(|p| p.accession == SCAN_START_TIME)
                .and_then(|p| p.unit.as_ref())
                .map(|u| u.name.to_string()),
            filter_string: scan_cv_params
                .iter()
                .find(|p| p.accession == FILTER_STRING)
                .and_then(|p| p.value.as_ref().map(|v| v.to_string())),
            ion_injection_time: find_cv_param_value(&scan_cv_params, ION_INJECTION_TIME),
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
                    target_mz: find_cv_param_value(&cv_params, ISOLATION_WINDOW_TARGET_MZ),
                    lower_offset: find_cv_param_value(&cv_params, ISOLATION_WINDOW_LOWER_OFFSET),
                    upper_offset: find_cv_param_value(&cv_params, ISOLATION_WINDOW_UPPER_OFFSET),
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
                    mz: find_cv_param_value(&cv_params, SELECTED_ION_MZ),
                    charge: find_cv_param_value(&cv_params, CHARGE_STATE),
                    intensity: find_cv_param_value(&cv_params, PEAK_INTENSITY),
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
                            COLLISION_INDUCED_DISSOCIATION | // CID
                            HCD | // HCD
                            ETD | // ETD
                            ECD | // ECD
                            ETHCD | // EThcD
                            PQD     // PQD
                        )
                    })
                    .and_then(|p| p.name.as_ref().map(|n| n.to_string()));

                Activation {
                    collision_energy: find_cv_param_value(&cv_params, COLLISION_ENERGY),
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
                    target_mz: find_cv_param_value(&cv_params, ISOLATION_WINDOW_TARGET_MZ),
                    lower_offset: find_cv_param_value(&cv_params, ISOLATION_WINDOW_LOWER_OFFSET),
                    upper_offset: find_cv_param_value(&cv_params, ISOLATION_WINDOW_UPPER_OFFSET),
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
        .and_then(|p| p.value.as_ref().map(|v| v.to_string()))
}

/// Find a user param value by name in param_tree XML
pub fn find_user_param_value(param_tree_xml: &str, name: &str) -> Option<String> {
    let params = parse_param_tree(param_tree_xml).ok()?;
    params
        .user_params
        .iter()
        .find(|p| p.name == name)
        .and_then(|p| p.value.as_ref().map(|v| v.to_string()))
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
// XML Parsing Helpers for DIA/SWATH
// ============================================================================

/// Parse a CV param float value from XML descendants
pub fn parse_cv_param_f32_value(children: &mut roxmltree::Descendants, cv_param_ac: &str) -> Option<f32> {
    children.find(|n| n.attribute("accession") == Some(cv_param_ac)).and_then(|n| {
        n.attributes()
            .find(|a| a.name().starts_with("value"))
            .and_then(|attr| attr.value().parse::<f32>().ok())
    })
}

/// Parse an isolation window from precursor_list XML
pub fn parse_isolation_window_from_xml(prec_list_xml: &str) -> Option<MzRange> {
    let xml_doc = roxmltree::Document::parse(prec_list_xml).ok()?;
    let mut children = xml_doc.descendants();
    
    // MS:1000827 = isolation window target m/z
    let target_mz = parse_cv_param_f32_value(&mut children, ISOLATION_WINDOW_TARGET_MZ)?;
    
    // Reset iterator for next search
    let mut children = xml_doc.descendants();
    // MS:1000828 = isolation window lower offset
    let lower_offset = parse_cv_param_f32_value(&mut children, ISOLATION_WINDOW_LOWER_OFFSET).unwrap_or(0.0);
    
    let mut children = xml_doc.descendants();
    // MS:1000829 = isolation window upper offset
    let upper_offset = parse_cv_param_f32_value(&mut children, ISOLATION_WINDOW_UPPER_OFFSET).unwrap_or(0.0);
    
    Some(MzRange {
        min_mz: (target_mz - lower_offset) as f64,
        max_mz: (target_mz + upper_offset) as f64,
    })
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
        assert_eq!(result.cv_params[0].value.as_deref(), Some("1"));
        assert_eq!(result.user_params.len(), 1);
        assert_eq!(result.user_params[0].name, "ms1_bb_mz_width");
        assert_eq!(result.user_params[0].cv_ref, Some(CvRef::MS));
        assert_eq!(result.user_params[0].accession.as_deref(), Some("MS:-1"));
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

    // ------------------------------------------------------------------------
    // Thermo scan filter string
    //
    // Ported from mzdb-access's `MzDbMetaDataTest`, including its exact expected values.
    // ------------------------------------------------------------------------

    /// The MS3 case from the reference test, with its documented decomposition.
    #[test]
    fn parses_an_ms3_thermo_filter_string() {
        let s = "FTMS + p NSI sps d Full ms3 707.8472@cid35.00 463.3669@hcd45.00 [115.0000-140.0000]";
        let meta = ThermoScanMetaData::parse(s).expect("MS3 filter string should parse");

        assert_eq!(meta.ms_level, 3);
        assert_eq!(meta.analyzer_type, "FTMS");
        assert_eq!(meta.acquisition_type, "FTMS + p NSI sps d Full ms3");
        assert_eq!(meta.mz_range, [115.0, 140.0]);

        assert_eq!(meta.targets.len(), 2);

        let ms1_target = &meta.targets[0];
        assert_eq!(ms1_target.ms_level, 1);
        assert!((ms1_target.mz - 707.8472).abs() < 1e-12);
        assert_eq!(ms1_target.activation_type, "cid");
        assert!((ms1_target.collision_energy - 35.0).abs() < 1e-4);

        let ms2_target = &meta.targets[1];
        assert_eq!(ms2_target.ms_level, 2);
        assert!((ms2_target.mz - 463.3669).abs() < 1e-12);
        assert_eq!(ms2_target.activation_type, "hcd");
        assert!((ms2_target.collision_energy - 45.0).abs() < 1e-4);
    }

    /// The MS2 case from the reference's own comment.
    #[test]
    fn parses_an_ms2_thermo_filter_string() {
        let s = "ITMS + c NSI d Full ms2 476.20@cid30.00 [120.00-1440.00]";
        let meta = ThermoScanMetaData::parse(s).expect("MS2 filter string should parse");

        assert_eq!(meta.ms_level, 2);
        assert_eq!(meta.analyzer_type, "ITMS");
        assert_eq!(meta.mz_range, [120.0, 1440.0]);

        assert_eq!(meta.targets.len(), 1, "an MS2 filter describes one fragmentation step");
        assert!((meta.targets[0].mz - 476.20).abs() < 1e-12);
        assert_eq!(meta.targets[0].activation_type, "cid");
        assert!((meta.targets[0].collision_energy - 30.0).abs() < 1e-4);
    }

    /// A scan with no `filterString` yields `None`, not an error.
    ///
    /// The Java reference returns `null` here — a non-Thermo instrument is an ordinary case, not a
    /// malformed one.
    #[test]
    fn scan_without_a_filter_string_has_no_thermo_metadata() {
        let scan = Scan::default();
        assert!(scan.thermo_meta_data().is_none());
    }

    /// A present but malformed string is an error rather than silently-empty metadata.
    ///
    /// The Java reference leaves its targets array full of nulls when the regex misses, deferring
    /// the failure to whatever dereferences one later.
    #[test]
    fn malformed_filter_strings_are_rejected() {
        for s in [
            "ITMS + c NSI d ms2 476.20@cid30.00 [120.00-1440.00]", // no `Full ms`
            "ITMS + c NSI d Full msX 476.20@cid30.00 [120.00]",    // no level digit
            "ITMS + c NSI d Full ms2 [120.00-1440.00]",            // no targets
            "ITMS + c NSI d Full ms2 476.20@cid30.00",             // no scan range
        ] {
            assert!(
                ThermoScanMetaData::parse(s).is_err(),
                "should have rejected {s:?}"
            );
        }
    }

    /// A scan carrying a filter string decodes through the `Scan` accessor.
    #[test]
    fn scan_accessor_decodes_the_filter_string() {
        let scan = Scan {
            filter_string: Some(
                "ITMS + c NSI d Full ms2 476.20@cid30.00 [120.00-1440.00]".to_string(),
            ),
            ..Default::default()
        };

        let meta = scan.thermo_meta_data().expect("filter string present").expect("parses");
        assert_eq!(meta.ms_level, 2);
        assert_eq!(meta.analyzer_type, "ITMS");
    }

}
