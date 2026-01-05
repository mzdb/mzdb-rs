//! XML Metadata Builder for Thermo RAW Files
//!
//! This module constructs mzDB-compatible XML metadata from Thermo RAW file information.

use anyhow::Result;
use thernio::raw::{RawFile, Analyzer, ActivationType};
use xmltree::{Element, XMLNode, EmitterConfig};

/// Write an XML element to a string without XML declaration
fn element_to_string(element: &Element) -> Result<String> {
    let mut output = Vec::new();
    let config = EmitterConfig::new()
        .write_document_declaration(false);
    element.write_with_config(&mut output, config)?;
    Ok(String::from_utf8(output)?)
}

/// Build component list XML for instrument configuration
pub(crate) fn build_component_list(raw: &mut RawFile) -> Result<String> {
    let mut root = Element::new("componentList");
    root.attributes.insert("count".to_string(), "3".to_string());
    
    // Source component
    let mut source = Element::new("source");
    source.attributes.insert("order".to_string(), "1".to_string());
    
    // Add ionization type CV param (ESI is most common for Thermo instruments)
    let mut cv_esi = Element::new("cvParam");
    cv_esi.attributes.insert("cvRef".to_string(), "MS".to_string());
    cv_esi.attributes.insert("accession".to_string(), "MS:1000073".to_string());
    cv_esi.attributes.insert("name".to_string(), "electrospray ionization".to_string());
    cv_esi.attributes.insert("value".to_string(), "".to_string());
    source.children.push(XMLNode::Element(cv_esi));
    
    root.children.push(XMLNode::Element(source));
    
    // Analyzer component - determined from first scan
    let mut analyzer = Element::new("analyzer");
    analyzer.attributes.insert("order".to_string(), "2".to_string());
    
    if let Ok(scan) = (raw).scan(1) {
        let (accession, name) = match scan.analyzer {
            Analyzer::Itms => ("MS:1000264", "ion trap"),
            Analyzer::Ftms => ("MS:1000079", "fourier transform ion cyclotron resonance mass spectrometer"),
            Analyzer::Tqms => ("MS:1000081", "quadrupole"),
            Analyzer::Tofms => ("MS:1000084", "time-of-flight"),
            _ => ("MS:1000443", "mass analyzer type"),
        };
        
        let mut cv_analyzer = Element::new("cvParam");
        cv_analyzer.attributes.insert("cvRef".to_string(), "MS".to_string());
        cv_analyzer.attributes.insert("accession".to_string(), accession.to_string());
        cv_analyzer.attributes.insert("name".to_string(), name.to_string());
        cv_analyzer.attributes.insert("value".to_string(), "".to_string());
        analyzer.children.push(XMLNode::Element(cv_analyzer));
    }
    
    root.children.push(XMLNode::Element(analyzer));
    
    // Detector component
    let mut detector = Element::new("detector");
    detector.attributes.insert("order".to_string(), "3".to_string());
    
    let mut cv_detector = Element::new("cvParam");
    cv_detector.attributes.insert("cvRef".to_string(), "MS".to_string());
    cv_detector.attributes.insert("accession".to_string(), "MS:1000253".to_string());
    cv_detector.attributes.insert("name".to_string(), "electron multiplier".to_string());
    cv_detector.attributes.insert("value".to_string(), "".to_string());
    detector.children.push(XMLNode::Element(cv_detector));
    
    root.children.push(XMLNode::Element(detector));
    
    // Convert to string
    element_to_string(&root)
}

/// Build scan list XML for a spectrum
pub(crate) fn build_scan_list(retention_time: f64) -> Result<String> {
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
    
    // Scan start time
    let mut cv_time = Element::new("cvParam");
    cv_time.attributes.insert("cvRef".to_string(), "MS".to_string());
    cv_time.attributes.insert("accession".to_string(), "MS:1000016".to_string());
    cv_time.attributes.insert("name".to_string(), "scan start time".to_string());
    cv_time.attributes.insert("value".to_string(), format!("{:.6}", retention_time));
    cv_time.attributes.insert("unitCvRef".to_string(), "UO".to_string());
    cv_time.attributes.insert("unitAccession".to_string(), "UO:0000031".to_string());
    cv_time.attributes.insert("unitName".to_string(), "minute".to_string());
    scan.children.push(XMLNode::Element(cv_time));
    
    root.children.push(XMLNode::Element(scan));
    
    element_to_string(&root)
}

/// Build precursor list XML for MS2+ spectra
pub(crate) fn build_precursor_list(
    precursor_mzs: &[f64],
    precursor_charge: Option<i32>,
    collision_energy: Option<f64>,
    activation_type: &str,
    isolation_width: Option<f64>,
    isolation_offset: Option<f64>,
) -> Result<String> {
    let mut root = Element::new("precursorList");
    root.attributes.insert("count".to_string(), "1".to_string());
    
    let mut precursor = Element::new("precursor");
    
    // Isolation window
    let mut iso_window = Element::new("isolationWindow");
    
    if let Some(&target_mz) = precursor_mzs.first() {
        // Target m/z
        let mut cv_target = Element::new("cvParam");
        cv_target.attributes.insert("cvRef".to_string(), "MS".to_string());
        cv_target.attributes.insert("accession".to_string(), "MS:1000827".to_string());
        cv_target.attributes.insert("name".to_string(), "isolation window target m/z".to_string());
        cv_target.attributes.insert("value".to_string(), format!("{:.6}", target_mz));
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
    }
    
    precursor.children.push(XMLNode::Element(iso_window));
    
    // Selected ion list
    if let Some(&selected_mz) = precursor_mzs.first() {
        let mut selected_ion_list = Element::new("selectedIonList");
        selected_ion_list.attributes.insert("count".to_string(), "1".to_string());
        
        let mut selected_ion = Element::new("selectedIon");
        
        // Selected ion m/z
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
    }
    
    // Activation - only add if we have activation info
    if !activation_type.is_empty() {
        let mut activation = Element::new("activation");
        
        // Activation method
        let (act_accession, act_name) = match activation_type {
            "CID" => ("MS:1000133", "collision-induced dissociation"),
            "HCD" => ("MS:1000422", "beam-type collision-induced dissociation"),
            "ETD" => ("MS:1000598", "electron transfer dissociation"),
            "ECD" => ("MS:1000250", "electron capture dissociation"),
            "PQD" => ("MS:1000599", "pulsed q dissociation"),
            "UVPD" => ("MS:1003246", "ultraviolet photodissociation"),
            _ => ("MS:1000044", "dissociation method"), // Generic fallback
        };
        
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
pub(crate) fn build_param_tree(params: &[(&str, &str, &str, &str)]) -> Result<String> {
    let mut root = Element::new("paramTree");
    
    // Create cvParams wrapper
    let mut cv_params = Element::new("cvParams");
    
    for (cv_ref, accession, name, value) in params {
        let mut cv_param = Element::new("cvParam");
        cv_param.attributes.insert("cvRef".to_string(), cv_ref.to_string());
        cv_param.attributes.insert("accession".to_string(), accession.to_string());
        cv_param.attributes.insert("name".to_string(), name.to_string());
        if !value.is_empty() {
            cv_param.attributes.insert("value".to_string(), value.to_string());
        }
        cv_params.children.push(XMLNode::Element(cv_param));
    }
    
    root.children.push(XMLNode::Element(cv_params));
    
    element_to_string(&root)
}

/// Determine activation type string from ActivationType enum
pub(crate) fn activation_type_to_string(act: ActivationType) -> &'static str {
    match act {
        ActivationType::CID => "CID",
        ActivationType::HCD => "HCD",
        ActivationType::ETD => "ETD",
        ActivationType::ECD => "ECD",
        ActivationType::PQD => "PQD",
        ActivationType::UVPD => "UVPD",
        _ => "CID",
    }
}
