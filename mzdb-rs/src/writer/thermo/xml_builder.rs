//! XML Metadata Builder for Thermo RAW Files
//!
//! This module constructs mzDB-compatible XML metadata from Thermo RAW file information.
//! Generic XML building utilities are in the parent `xml_builder` module.

use anyhow_ext::Result;
use thernio::raw::{RawFile, Analyzer, ActivationType};
use xmltree::{Element, XMLNode};

// Re-export generic functions for convenience within the thermo module
pub(crate) use crate::writer::xml_builder::{
    element_to_string, build_scan_list, build_precursor_list, build_param_tree
};

/// Build component list XML for instrument configuration
/// 
/// This is Thermo-specific as it reads analyzer type from the RAW file.
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

/// Determine activation type string from Thermo ActivationType enum
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
