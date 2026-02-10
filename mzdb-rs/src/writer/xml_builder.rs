//! XML Metadata Builder for mzDB Files
//!
//! This module provides generic XML building utilities for mzDB metadata.
//! These functions generate PSI-MS compliant XML fragments for spectrum metadata.

use anyhow_ext::Result;

use xmltree::{Element, XMLNode, EmitterConfig};

// ============================================================================
// XML Element Utilities (requires xmltree feature)
// ============================================================================

/// Write an XML element to a string without XML declaration, with pretty printing
pub fn element_to_string(element: &Element) -> Result<String> {
    let mut output = Vec::new();
    let config = EmitterConfig::new()
        .write_document_declaration(false)
        .perform_indent(true)
        .indent_string("  ");
    element.write_with_config(&mut output, config)?;
    Ok(String::from_utf8(output)?)
}

/// Build scan list XML for a spectrum with optional additional metadata
///
/// # Arguments
/// * `retention_time` - Scan start time in minutes
/// * `filter_string` - Optional instrument filter string
/// * `ion_injection_time` - Optional ion injection time in milliseconds
/// * `instrument_config_ref` - Optional instrument configuration reference (e.g., "IC1", "IC2")
/// * `scan_window_lower` - Optional lower m/z limit of scan window
/// * `scan_window_upper` - Optional upper m/z limit of scan window
pub fn build_scan_list(
    filter_string: Option<&str>,
    ion_injection_time: Option<f64>,
    instrument_config_ref: Option<&str>,
    scan_window_lower: Option<f64>,
    scan_window_upper: Option<f64>,
) -> Result<String> {
    let mut root = Element::new("scanList");
    root.attributes.insert("count".to_string(), "1".to_string());
    
    // Add "no combination" CV param
    let mut cv_no_comb = Element::new("cvParam");
    cv_no_comb.attributes.insert("cvRef".to_string(), "MS".to_string());
    cv_no_comb.attributes.insert("accession".to_string(), "MS:1000795".to_string());
    cv_no_comb.attributes.insert("name".to_string(), "no combination".to_string());
    cv_no_comb.attributes.insert("value".to_string(), "".to_string());
    root.children.push(XMLNode::Element(cv_no_comb));
    
    // Scan element
    let mut scan = Element::new("scan");
    
    // Add instrumentConfigurationRef if provided
    if let Some(inst_ref) = instrument_config_ref {
        scan.attributes.insert("instrumentConfigurationRef".to_string(), inst_ref.to_string());
    }
    
    // Note: scan start time (MS:1000016) is not included here as it is already
    // stored in the spectrum.time column, avoiding redundant per-spectrum XML bloat.
    
    // Filter string if provided
    if let Some(filter) = filter_string {
        let mut cv_filter = Element::new("cvParam");
        cv_filter.attributes.insert("cvRef".to_string(), "MS".to_string());
        cv_filter.attributes.insert("accession".to_string(), "MS:1000512".to_string());
        cv_filter.attributes.insert("name".to_string(), "filter string".to_string());
        cv_filter.attributes.insert("value".to_string(), filter.to_string());
        scan.children.push(XMLNode::Element(cv_filter));
    }
    
    // Ion injection time if provided
    if let Some(iit) = ion_injection_time {
        let mut cv_iit = Element::new("cvParam");
        cv_iit.attributes.insert("cvRef".to_string(), "MS".to_string());
        cv_iit.attributes.insert("accession".to_string(), "MS:1000927".to_string());
        cv_iit.attributes.insert("name".to_string(), "ion injection time".to_string());
        cv_iit.attributes.insert("value".to_string(), format!("{:.3}", iit));
        cv_iit.attributes.insert("unitCvRef".to_string(), "UO".to_string());
        cv_iit.attributes.insert("unitAccession".to_string(), "UO:0000028".to_string());
        cv_iit.attributes.insert("unitName".to_string(), "millisecond".to_string());
        scan.children.push(XMLNode::Element(cv_iit));
    }
    
    // Scan window list if bounds provided
    if scan_window_lower.is_some() || scan_window_upper.is_some() {
        let mut scan_window_list = Element::new("scanWindowList");
        scan_window_list.attributes.insert("count".to_string(), "1".to_string());
        
        let mut scan_window = Element::new("scanWindow");
        
        if let Some(lower) = scan_window_lower {
            let mut cv_lower = Element::new("cvParam");
            cv_lower.attributes.insert("cvRef".to_string(), "MS".to_string());
            cv_lower.attributes.insert("accession".to_string(), "MS:1000501".to_string());
            cv_lower.attributes.insert("name".to_string(), "scan window lower limit".to_string());
            cv_lower.attributes.insert("value".to_string(), format!("{:.0}", lower));
            cv_lower.attributes.insert("unitCvRef".to_string(), "MS".to_string());
            cv_lower.attributes.insert("unitAccession".to_string(), "MS:1000040".to_string());
            cv_lower.attributes.insert("unitName".to_string(), "m/z".to_string());
            scan_window.children.push(XMLNode::Element(cv_lower));
        }
        
        if let Some(upper) = scan_window_upper {
            let mut cv_upper = Element::new("cvParam");
            cv_upper.attributes.insert("cvRef".to_string(), "MS".to_string());
            cv_upper.attributes.insert("accession".to_string(), "MS:1000500".to_string());
            cv_upper.attributes.insert("name".to_string(), "scan window upper limit".to_string());
            cv_upper.attributes.insert("value".to_string(), format!("{:.0}", upper));
            cv_upper.attributes.insert("unitCvRef".to_string(), "MS".to_string());
            cv_upper.attributes.insert("unitAccession".to_string(), "MS:1000040".to_string());
            cv_upper.attributes.insert("unitName".to_string(), "m/z".to_string());
            scan_window.children.push(XMLNode::Element(cv_upper));
        }
        
        scan_window_list.children.push(XMLNode::Element(scan_window));
        scan.children.push(XMLNode::Element(scan_window_list));
    }
    
    root.children.push(XMLNode::Element(scan));
    
    element_to_string(&root)
}

/// Build precursor list XML for MS2+ spectra
///
/// # Arguments
/// * `precursor_mzs` - List of precursor m/z values (first is used as target)
/// * `precursor_charge` - Optional charge state
/// * `collision_energy` - Optional collision energy in eV
/// * `activation_type` - Activation type string (e.g., "CID", "HCD", "ETD")
/// * `isolation_width` - Optional isolation window width in Da
/// * `isolation_offset` - Optional isolation window offset in Da
pub fn build_precursor_list(
    isolation_target_mz: f64,          // Center of isolation window (selected by quadrupole)
    selected_ion_mz: Option<f64>,      // Monoisotopic m/z (for selectedIon, defaults to isolation_target_mz)
    precursor_charge: Option<i32>,
    collision_energy: Option<f64>,
    activation_type: &str,
    isolation_width: Option<f64>,
    isolation_offset: Option<f64>,
) -> Result<String> {
    let mut root = Element::new("precursorList");
    root.attributes.insert("count".to_string(), "1".to_string());
    
    let mut precursor = Element::new("precursor");
    
    // Isolation window - uses isolation_target_mz (center of quadrupole selection)
    let mut iso_window = Element::new("isolationWindow");
    
    // Target m/z (center of isolation window) - rounded to 1 decimal place
    let mut cv_target = Element::new("cvParam");
    cv_target.attributes.insert("cvRef".to_string(), "MS".to_string());
    cv_target.attributes.insert("accession".to_string(), "MS:1000827".to_string());
    cv_target.attributes.insert("name".to_string(), "isolation window target m/z".to_string());
    cv_target.attributes.insert("value".to_string(), format!("{:.1}", isolation_target_mz));
    cv_target.attributes.insert("unitCvRef".to_string(), "MS".to_string());
    cv_target.attributes.insert("unitAccession".to_string(), "MS:1000040".to_string());
    cv_target.attributes.insert("unitName".to_string(), "m/z".to_string());
    iso_window.children.push(XMLNode::Element(cv_target));
    
    // Add isolation window lower/upper offset if width is available
    if let Some(width) = isolation_width {
        let offset = isolation_offset.unwrap_or(0.0);
        let half_width = width / 2.0;
        
        // Lower offset (MS:1000828)
        let mut cv_lower = Element::new("cvParam");
        cv_lower.attributes.insert("cvRef".to_string(), "MS".to_string());
        cv_lower.attributes.insert("accession".to_string(), "MS:1000828".to_string());
        cv_lower.attributes.insert("name".to_string(), "isolation window lower offset".to_string());
        cv_lower.attributes.insert("value".to_string(), format!("{:.6}", half_width - offset));
        cv_lower.attributes.insert("unitCvRef".to_string(), "MS".to_string());
        cv_lower.attributes.insert("unitAccession".to_string(), "MS:1000040".to_string());
        cv_lower.attributes.insert("unitName".to_string(), "m/z".to_string());
        iso_window.children.push(XMLNode::Element(cv_lower));
        
        // Upper offset (MS:1000829)
        let mut cv_upper = Element::new("cvParam");
        cv_upper.attributes.insert("cvRef".to_string(), "MS".to_string());
        cv_upper.attributes.insert("accession".to_string(), "MS:1000829".to_string());
        cv_upper.attributes.insert("name".to_string(), "isolation window upper offset".to_string());
        cv_upper.attributes.insert("value".to_string(), format!("{:.6}", half_width + offset));
        cv_upper.attributes.insert("unitCvRef".to_string(), "MS".to_string());
        cv_upper.attributes.insert("unitAccession".to_string(), "MS:1000040".to_string());
        cv_upper.attributes.insert("unitName".to_string(), "m/z".to_string());
        iso_window.children.push(XMLNode::Element(cv_upper));
    }
    
    precursor.children.push(XMLNode::Element(iso_window));
    
    // Selected ion list - uses monoisotopic m/z when available, otherwise isolation target
    let selected_mz = selected_ion_mz.unwrap_or(isolation_target_mz);
    
    let mut selected_ion_list = Element::new("selectedIonList");
    selected_ion_list.attributes.insert("count".to_string(), "1".to_string());
    
    let mut selected_ion = Element::new("selectedIon");
    
    // Selected ion m/z (monoisotopic when available)
    let mut cv_sel_mz = Element::new("cvParam");
    cv_sel_mz.attributes.insert("cvRef".to_string(), "MS".to_string());
    cv_sel_mz.attributes.insert("accession".to_string(), "MS:1000744".to_string());
    cv_sel_mz.attributes.insert("name".to_string(), "selected ion m/z".to_string());
    cv_sel_mz.attributes.insert("value".to_string(), format!("{:.6}", selected_mz));
    cv_sel_mz.attributes.insert("unitCvRef".to_string(), "MS".to_string());
    cv_sel_mz.attributes.insert("unitAccession".to_string(), "MS:1000040".to_string());
    cv_sel_mz.attributes.insert("unitName".to_string(), "m/z".to_string());
    selected_ion.children.push(XMLNode::Element(cv_sel_mz));
    
    // Charge state if available
    if let Some(charge) = precursor_charge {
        let mut cv_charge = Element::new("cvParam");
        cv_charge.attributes.insert("cvRef".to_string(), "MS".to_string());
        cv_charge.attributes.insert("accession".to_string(), "MS:1000041".to_string());
        cv_charge.attributes.insert("name".to_string(), "charge state".to_string());
        cv_charge.attributes.insert("value".to_string(), charge.to_string());
        selected_ion.children.push(XMLNode::Element(cv_charge));
    }
    
    selected_ion_list.children.push(XMLNode::Element(selected_ion));
    precursor.children.push(XMLNode::Element(selected_ion_list));
    
    // Activation - only add if we have activation info
    if !activation_type.is_empty() {
        let mut activation = Element::new("activation");
        
        // Activation method
        let (act_accession, act_name) = activation_type_to_cv(activation_type);
        
        let mut cv_act = Element::new("cvParam");
        cv_act.attributes.insert("cvRef".to_string(), "MS".to_string());
        cv_act.attributes.insert("accession".to_string(), act_accession.to_string());
        cv_act.attributes.insert("name".to_string(), act_name.to_string());
        cv_act.attributes.insert("value".to_string(), "".to_string());
        activation.children.push(XMLNode::Element(cv_act));
        
        // Collision energy if available
        if let Some(ce) = collision_energy {
            if ce > 0.0 {
                let mut cv_ce = Element::new("cvParam");
                cv_ce.attributes.insert("cvRef".to_string(), "MS".to_string());
                cv_ce.attributes.insert("accession".to_string(), "MS:1000045".to_string());
                cv_ce.attributes.insert("name".to_string(), "collision energy".to_string());
                cv_ce.attributes.insert("value".to_string(), format!("{:.2}", ce));
                cv_ce.attributes.insert("unitCvRef".to_string(), "UO".to_string());
                cv_ce.attributes.insert("unitAccession".to_string(), "UO:0000266".to_string());
                cv_ce.attributes.insert("unitName".to_string(), "electronvolt".to_string());
                activation.children.push(XMLNode::Element(cv_ce));
            }
        }
        
        precursor.children.push(XMLNode::Element(activation));
    }
    
    root.children.push(XMLNode::Element(precursor));
    
    element_to_string(&root)
}

/// Build param tree XML with CV parameters
///
/// Each parameter is a tuple of (cvRef, accession, name, value) or
/// (cvRef, accession, name, value, unitCvRef, unitAccession, unitName) for parameters with units
pub fn build_param_tree(params: &[CvParam]) -> Result<String> {
    build_full_param_tree(params, &[])
}

/// Build a param_tree XML string containing both cvParams and/or userParams.
///
/// Either section is omitted when its slice is empty.
pub fn build_full_param_tree(cv_params: &[CvParam], user_params: &[SimpleUserParam]) -> Result<String> {
    let mut root = Element::new("params");

    if !cv_params.is_empty() {
        let mut cv_wrapper = Element::new("cvParams");
        for param in cv_params {
            let mut el = Element::new("cvParam");
            el.attributes.insert("cvRef".to_string(), param.cv_ref.to_string());
            el.attributes.insert("accession".to_string(), param.accession.to_string());
            el.attributes.insert("name".to_string(), param.name.to_string());
            if !param.value.is_empty() {
                el.attributes.insert("value".to_string(), param.value.to_string());
            }
            if let Some(ref unit) = param.unit {
                el.attributes.insert("unitCvRef".to_string(), unit.cv_ref.to_string());
                el.attributes.insert("unitAccession".to_string(), unit.accession.to_string());
                el.attributes.insert("unitName".to_string(), unit.name.to_string());
            }
            cv_wrapper.children.push(XMLNode::Element(el));
        }
        root.children.push(XMLNode::Element(cv_wrapper));
    }

    if !user_params.is_empty() {
        let mut user_wrapper = Element::new("userParams");
        for param in user_params {
            let mut el = Element::new("userParam");
            el.attributes.insert("cvRef".to_string(), "MS".to_string());
            el.attributes.insert("accession".to_string(), "MS:-1".to_string());
            el.attributes.insert("name".to_string(), param.name.to_string());
            el.attributes.insert("type".to_string(), param.param_type.to_string());
            el.attributes.insert("value".to_string(), param.value.to_string());
            user_wrapper.children.push(XMLNode::Element(el));
        }
        root.children.push(XMLNode::Element(user_wrapper));
    }

    element_to_string(&root)
}

/// Helper to build param tree from simple tuples (for backward compatibility)
pub fn build_param_tree_simple(params: &[(&str, &str, &str, &str)]) -> Result<String> {
    let cv_params: Vec<CvParam> = params.iter()
        .map(|(cv_ref, accession, name, value)| CvParam::new(cv_ref, accession, name, value))
        .collect();
    build_param_tree(&cv_params)
}

/// Helper to build a full param tree (cvParams + userParams) from simple tuples.
///
/// `cv_params`: `(cv_ref, accession, name, value)` tuples
/// `user_params`: `(name, value)` tuples (type defaults to xsd:string)
pub fn build_full_param_tree_simple(
    cv_params: &[(&str, &str, &str, &str)],
    user_params: &[(&str, &str)],
) -> Result<String> {
    let cv: Vec<CvParam> = cv_params.iter()
        .map(|(cv_ref, accession, name, value)| CvParam::new(cv_ref, accession, name, value))
        .collect();
    let up: Vec<SimpleUserParam> = user_params.iter()
        .map(|(name, value)| SimpleUserParam::new(name, value))
        .collect();
    build_full_param_tree(&cv, &up)
}

/// CV Parameter structure for building param trees
#[derive(Debug, Clone)]
pub struct CvParam {
    pub cv_ref: String,
    pub accession: String,
    pub name: String,
    pub value: String,
    pub unit: Option<CvUnit>,
}

impl CvParam {
    /// Create a CV parameter without units
    pub fn new(cv_ref: &str, accession: &str, name: &str, value: &str) -> Self {
        Self {
            cv_ref: cv_ref.to_string(),
            accession: accession.to_string(),
            name: name.to_string(),
            value: value.to_string(),
            unit: None,
        }
    }
    
    /// Create a CV parameter with units
    pub fn with_unit(
        cv_ref: &str,
        accession: &str,
        name: &str,
        value: &str,
        unit_cv_ref: &str,
        unit_accession: &str,
        unit_name: &str,
    ) -> Self {
        Self {
            cv_ref: cv_ref.to_string(),
            accession: accession.to_string(),
            name: name.to_string(),
            value: value.to_string(),
            unit: Some(CvUnit {
                cv_ref: unit_cv_ref.to_string(),
                accession: unit_accession.to_string(),
                name: unit_name.to_string(),
            }),
        }
    }
}

/// CV Unit structure
#[derive(Debug, Clone)]
pub struct CvUnit {
    pub cv_ref: String,
    pub accession: String,
    pub name: String,
}

/// User-defined parameter (no CV equivalent)
#[derive(Debug, Clone)]
pub struct SimpleUserParam {
    pub name: String,
    pub value: String,
    pub param_type: String,
}

impl SimpleUserParam {
    /// Create a user parameter with type xsd:string
    pub fn new(name: &str, value: &str) -> Self {
        Self {
            name: name.to_string(),
            value: value.to_string(),
            param_type: "xsd:string".to_string(),
        }
    }

    /// Create a user parameter with an explicit type
    pub fn with_type(name: &str, value: &str, param_type: &str) -> Self {
        Self {
            name: name.to_string(),
            value: value.to_string(),
            param_type: param_type.to_string(),
        }
    }
}

/// Convert activation type string to CV accession and name
fn activation_type_to_cv(activation_type: &str) -> (&'static str, &'static str) {
    match activation_type {
        "CID" => ("MS:1000133", "collision-induced dissociation"),
        "HCD" => ("MS:1000422", "beam-type collision-induced dissociation"),
        "ETD" => ("MS:1000598", "electron transfer dissociation"),
        "ECD" => ("MS:1000250", "electron capture dissociation"),
        "PQD" => ("MS:1000599", "pulsed q dissociation"),
        "UVPD" => ("MS:1003246", "ultraviolet photodissociation"),
        _ => ("MS:1000044", "dissociation method"), // Generic fallback
    }
}

// ============================================================================
// Simple XML String Builders (no xmltree dependency)
// ============================================================================

/// Generate param_tree XML for a DIA/simplified spectrum
/// 
/// Creates a minimal param_tree with MS level 2, MSn spectrum type,
/// centroid mode, and scan start time, following the official mzDB schema.
///
/// # Arguments
/// * `time_seconds` - Retention time in seconds (will be converted to minutes)
pub fn generate_ms2_param_tree_xml(time_seconds: f32) -> String {
    format!(
        r#"<params>
  <cvParams>
    <cvParam cvRef="MS" accession="MS:1000511" value="2" name="ms level"/>
    <cvParam cvRef="MS" accession="MS:1000580" value="" name="MSn spectrum"/>
    <cvParam cvRef="MS" accession="MS:1000127" value="" name="centroid spectrum"/>
    <cvParam cvRef="MS" accession="MS:1000016" value="{:.4}" name="scan start time" unitCvRef="UO" unitAccession="UO:0000031" unitName="minute"/>
  </cvParams>
</params>"#,
        time_seconds / 60.0
    )
}

/// Generate precursor_list XML for a DIA spectrum with symmetric isolation window
///
/// # Arguments
/// * `target_mz` - Isolation window target m/z
/// * `half_width` - Half-width of the isolation window in Da
pub fn generate_dia_precursor_list_xml(target_mz: f64, half_width: f64) -> String {
    format!(
        r#"<precursorList count="1">
  <precursor>
    <isolationWindow>
      <cvParam cvRef="MS" accession="MS:1000827" value="{:.4}" name="isolation window target m/z" unitCvRef="MS" unitAccession="MS:1000040" unitName="m/z"/>
      <cvParam cvRef="MS" accession="MS:1000828" value="{:.4}" name="isolation window lower offset" unitCvRef="MS" unitAccession="MS:1000040" unitName="m/z"/>
      <cvParam cvRef="MS" accession="MS:1000829" value="{:.4}" name="isolation window upper offset" unitCvRef="MS" unitAccession="MS:1000040" unitName="m/z"/>
    </isolationWindow>
    <activation>
      <cvParam cvRef="MS" accession="MS:1000422" value="" name="beam-type collision-induced dissociation"/>
    </activation>
  </precursor>
</precursorList>"#,
        target_mz, half_width, half_width
    )
}

/// Generate precursor_list XML for a DIA spectrum with asymmetric isolation window
///
/// # Arguments
/// * `target_mz` - Isolation window target m/z
/// * `lower_mz` - Lower bound of isolation window
/// * `upper_mz` - Upper bound of isolation window
pub fn generate_dia_precursor_list_xml_asymmetric(
    target_mz: f64,
    lower_mz: f64,
    upper_mz: f64,
) -> String {
    let lower_offset = target_mz - lower_mz;
    let upper_offset = upper_mz - target_mz;

    format!(
        r#"<precursorList count="1">
  <precursor>
    <isolationWindow>
      <cvParam cvRef="MS" accession="MS:1000827" value="{:.4}" name="isolation window target m/z" unitCvRef="MS" unitAccession="MS:1000040" unitName="m/z"/>
      <cvParam cvRef="MS" accession="MS:1000828" value="{:.4}" name="isolation window lower offset" unitCvRef="MS" unitAccession="MS:1000040" unitName="m/z"/>
      <cvParam cvRef="MS" accession="MS:1000829" value="{:.4}" name="isolation window upper offset" unitCvRef="MS" unitAccession="MS:1000040" unitName="m/z"/>
    </isolationWindow>
    <activation>
      <cvParam cvRef="MS" accession="MS:1000422" value="" name="beam-type collision-induced dissociation"/>
    </activation>
  </precursor>
</precursorList>"#,
        target_mz, lower_offset, upper_offset
    )
}

/// Generate scan_list XML for a spectrum
///
/// # Arguments
/// * `time_minutes` - Scan start time in minutes
pub fn generate_scan_list_xml(time_minutes: f64) -> String {
    format!(
        r#"<scanList count="1">
  <cvParam cvRef="MS" accession="MS:1000795" value="" name="no combination"/>
  <scan>
    <cvParam cvRef="MS" accession="MS:1000016" value="{:.6}" name="scan start time" unitCvRef="UO" unitAccession="UO:0000031" unitName="minute"/>
  </scan>
</scanList>"#,
        time_minutes
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ms2_param_tree_xml() {
        let xml = generate_ms2_param_tree_xml(120.0);
        assert!(xml.contains("ms level"));
        assert!(xml.contains("value=\"2\""));
        assert!(xml.contains("MSn spectrum"));
        assert!(xml.contains("centroid spectrum"));
        assert!(xml.contains("scan start time"));
        // 120 seconds = 2 minutes
        assert!(xml.contains("2.0000"));
    }

    #[test]
    fn test_dia_precursor_list_xml() {
        let xml = generate_dia_precursor_list_xml(500.0, 25.0);
        assert!(xml.contains("isolation window target m/z"));
        assert!(xml.contains("500.0000"));
        assert!(xml.contains("isolation window lower offset"));
        assert!(xml.contains("isolation window upper offset"));
        assert!(xml.contains("25.0000"));
        assert!(xml.contains("beam-type collision-induced dissociation"));
    }

    #[test]
    fn test_dia_precursor_list_xml_asymmetric() {
        let xml = generate_dia_precursor_list_xml_asymmetric(500.0, 475.0, 525.0);
        assert!(xml.contains("500.0000")); // target
        assert!(xml.contains("25.0000")); // both offsets should be 25
    }
}
