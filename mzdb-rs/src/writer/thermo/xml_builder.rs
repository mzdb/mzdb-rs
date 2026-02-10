//! XML Metadata Builder for Thermo RAW Files
//!
//! This module constructs mzDB-compatible XML metadata from Thermo RAW file information.
//! Generic XML building utilities are in the parent `xml_builder` module.

use anyhow_ext::Result;
use thernio::raw::{Analyzer, ActivationType, IonizationMode};
use xmltree::{Element, XMLNode};

// Re-export generic functions for convenience within the thermo module
pub(crate) use crate::writer::xml_builder::{
    element_to_string, build_scan_list, build_precursor_list,
    build_param_tree, build_param_tree_simple,
    build_full_param_tree, build_full_param_tree_simple,
    CvParam, SimpleUserParam,
};

/// Map an Analyzer to its CV accession and name.
///
/// Returns `None` for unknown analyzer types — callers should skip these
/// rather than emit a meaningless parent-level CV term.
///
/// For Orbitrap-family instruments, FTMS maps to the orbitrap analyzer (MS:1000484)
/// rather than the generic FTICR (MS:1000079), which is physically different hardware.
/// ITMS maps to radial ejection linear ion trap (MS:1000083) for LTQ-style traps.
pub(crate) fn analyzer_cv(analyzer: Analyzer) -> Option<(&'static str, &'static str)> {
    match analyzer {
        Analyzer::Ftms => Some(("MS:1000484", "orbitrap")),
        Analyzer::Itms => Some(("MS:1000083", "radial ejection linear ion trap")),
        Analyzer::Tqms => Some(("MS:1000081", "quadrupole")),
        Analyzer::Tofms => Some(("MS:1000084", "time-of-flight")),
        Analyzer::Sqms => Some(("MS:1000081", "quadrupole")),
        Analyzer::Sector => Some(("MS:1000080", "magnetic sector")),
        Analyzer::Unknown(_) => None,
    }
}

/// Map an Analyzer to the appropriate detector CV accession and name.
///
/// Returns `None` for unknown analyzer types.
///
/// FTMS/orbitrap uses an inductive (image current) detector, while
/// ion trap and other instruments use electron multiplier detectors.
pub(crate) fn detector_cv_for_analyzer(analyzer: Analyzer) -> Option<(&'static str, &'static str)> {
    match analyzer {
        Analyzer::Unknown(_) => None,
        Analyzer::Ftms => Some(("MS:1000624", "inductive detector")),
        _ => Some(("MS:1000253", "electron multiplier")),
    }
}

/// Map an IonizationMode to CV params for the source component.
///
/// Returns `None` for unknown ionization modes. Some modes produce multiple
/// CV params (e.g. NSI -> nanoelectrospray + nanospray inlet).
pub(crate) fn ionization_cv(mode: IonizationMode) -> Option<Vec<(&'static str, &'static str)>> {
    match mode {
        IonizationMode::ESI => Some(vec![
            ("MS:1000073", "electrospray ionization"),
        ]),
        IonizationMode::NSI => Some(vec![
            ("MS:1000398", "nanoelectrospray"),
            ("MS:1000485", "nanospray inlet"),
        ]),
        IonizationMode::APCI => Some(vec![
            ("MS:1000070", "atmospheric pressure chemical ionization"),
        ]),
        IonizationMode::EI => Some(vec![
            ("MS:1000389", "electron ionization"),
        ]),
        IonizationMode::CI => Some(vec![
            ("MS:1000071", "chemical ionization"),
        ]),
        IonizationMode::FAB => Some(vec![
            ("MS:1000074", "fast atom bombardment ionization"),
        ]),
        IonizationMode::MALDI => Some(vec![
            ("MS:1000075", "matrix-assisted laser desorption ionization"),
        ]),
        _ => None,
    }
}

/// Build a component list XML string for a given analyzer and ionization mode.
///
/// The component list contains three components:
/// 1. Source (ionization) - derived from the IonizationMode
/// 2. Analyzer - derived from the Analyzer enum
/// 3. Detector - matched to the analyzer type
///
/// Components with unknown/unmapped types are included as empty elements.
pub(crate) fn build_component_list_for_analyzer(
    analyzer: Analyzer,
    ionization: IonizationMode,
) -> Result<String> {
    let mut root = Element::new("componentList");
    root.attributes.insert("count".to_string(), "3".to_string());

    // Source component
    let mut source = Element::new("source");
    source.attributes.insert("order".to_string(), "1".to_string());

    if let Some(cv_params) = ionization_cv(ionization) {
        for (accession, name) in cv_params {
            let mut cv = Element::new("cvParam");
            cv.attributes.insert("cvRef".to_string(), "MS".to_string());
            cv.attributes.insert("accession".to_string(), accession.to_string());
            cv.attributes.insert("name".to_string(), name.to_string());
            cv.attributes.insert("value".to_string(), "".to_string());
            source.children.push(XMLNode::Element(cv));
        }
    }

    root.children.push(XMLNode::Element(source));

    // Analyzer component
    let mut analyzer_elem = Element::new("analyzer");
    analyzer_elem.attributes.insert("order".to_string(), "2".to_string());

    if let Some((acc, name)) = analyzer_cv(analyzer) {
        let mut cv_analyzer = Element::new("cvParam");
        cv_analyzer.attributes.insert("cvRef".to_string(), "MS".to_string());
        cv_analyzer.attributes.insert("accession".to_string(), acc.to_string());
        cv_analyzer.attributes.insert("name".to_string(), name.to_string());
        cv_analyzer.attributes.insert("value".to_string(), "".to_string());
        analyzer_elem.children.push(XMLNode::Element(cv_analyzer));
    }

    root.children.push(XMLNode::Element(analyzer_elem));

    // Detector component
    let mut detector = Element::new("detector");
    detector.attributes.insert("order".to_string(), "3".to_string());

    if let Some((det_acc, det_name)) = detector_cv_for_analyzer(analyzer) {
        let mut cv_detector = Element::new("cvParam");
        cv_detector.attributes.insert("cvRef".to_string(), "MS".to_string());
        cv_detector.attributes.insert("accession".to_string(), det_acc.to_string());
        cv_detector.attributes.insert("name".to_string(), det_name.to_string());
        cv_detector.attributes.insert("value".to_string(), "".to_string());
        detector.children.push(XMLNode::Element(cv_detector));
    }

    root.children.push(XMLNode::Element(detector));

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

/// Map a Thermo instrument model name to its PSI-MS CV accession.
///
/// Returns `None` for unrecognized models. The caller should fall back
/// to the generic "Thermo Scientific instrument model" (MS:1000494).
pub(crate) fn thermo_model_cv(model: &str) -> Option<(&'static str, &'static str)> {
    let lower = model.to_lowercase();
    // Match from most specific to least specific
    if lower.contains("orbitrap astral") {
        Some(("MS:1003378", "Orbitrap Astral"))
    } else if lower.contains("eclipse") {
        Some(("MS:1003029", "Orbitrap Eclipse"))
    } else if lower.contains("exploris 480") {
        Some(("MS:1003028", "Orbitrap Exploris 480"))
    } else if lower.contains("exploris 240") {
        Some(("MS:1003293", "Orbitrap Exploris 240"))
    } else if lower.contains("exploris 120") {
        Some(("MS:1003294", "Orbitrap Exploris 120"))
    } else if lower.contains("exploris") {
        Some(("MS:1003028", "Orbitrap Exploris 480"))
    } else if lower.contains("fusion lumos") {
        Some(("MS:1002732", "Orbitrap Fusion Lumos"))
    } else if lower.contains("fusion") {
        Some(("MS:1002416", "Orbitrap Fusion"))
    } else if lower.contains("id-x") {
        Some(("MS:1003112", "Orbitrap ID-X"))
    } else if lower.contains("q exactive hf-x") {
        Some(("MS:1002877", "Q Exactive HF-X"))
    } else if lower.contains("q exactive hf") {
        Some(("MS:1002523", "Q Exactive HF"))
    } else if lower.contains("q exactive plus") {
        Some(("MS:1002634", "Q Exactive Plus"))
    } else if lower.contains("q exactive") {
        Some(("MS:1001911", "Q Exactive"))
    } else if lower.contains("exactive plus") {
        Some(("MS:1002526", "Exactive Plus"))
    } else if lower.contains("exactive") {
        Some(("MS:1000649", "Exactive"))
    } else if lower.contains("orbitrap velos pro") {
        Some(("MS:1001742", "LTQ Orbitrap Velos"))
    } else if lower.contains("orbitrap velos") {
        Some(("MS:1001742", "LTQ Orbitrap Velos"))
    } else if lower.contains("orbitrap elite") {
        Some(("MS:1001910", "LTQ Orbitrap Elite"))
    } else if lower.contains("orbitrap xl") || lower.contains("orbitrap discovery") {
        Some(("MS:1000556", "LTQ Orbitrap XL"))
    } else if lower.contains("orbitrap classic") || (lower.contains("orbitrap") && lower.contains("ltq") && !lower.contains("velos") && !lower.contains("elite") && !lower.contains("xl")) {
        Some(("MS:1000449", "LTQ Orbitrap"))
    } else if lower.contains("ltq velos") {
        Some(("MS:1000855", "LTQ Velos"))
    } else if lower.contains("ltq xl") {
        Some(("MS:1000854", "LTQ XL"))
    } else if lower.contains("ltq") {
        Some(("MS:1000447", "LTQ"))
    } else if lower.contains("tsg altis") || lower.contains("tsq altis") {
        Some(("MS:1002874", "TSQ Altis"))
    } else if lower.contains("tsq quantiva") {
        Some(("MS:1002419", "TSQ Quantiva"))
    } else if lower.contains("tsq") {
        Some(("MS:1000750", "TSQ Quantum Ultra"))
    } else {
        None
    }
}

/// Build the XML content for a CommonInstrumentParams shared_param_tree.
///
/// This follows the mzML referenceableParamGroup pattern used by the reference
/// mzDB files, containing the instrument model CV term and serial number.
pub(crate) fn build_common_instrument_params(
    model_name: &str,
    serial_number: &str,
) -> Result<String> {
    let mut root = Element::new("referenceableParamGroup");
    root.attributes.insert("id".to_string(), "CommonInstrumentParams".to_string());

    // Instrument model CV param
    let (accession, cv_name) = thermo_model_cv(model_name)
        .unwrap_or(("MS:1000494", "Thermo Scientific instrument model"));

    let mut cv_model = Element::new("cvParam");
    cv_model.attributes.insert("cvRef".to_string(), "MS".to_string());
    cv_model.attributes.insert("accession".to_string(), accession.to_string());
    cv_model.attributes.insert("name".to_string(), cv_name.to_string());
    cv_model.attributes.insert("value".to_string(), "".to_string());
    root.children.push(XMLNode::Element(cv_model));

    // Serial number
    if !serial_number.is_empty() {
        let mut cv_serial = Element::new("cvParam");
        cv_serial.attributes.insert("cvRef".to_string(), "MS".to_string());
        cv_serial.attributes.insert("accession".to_string(), "MS:1000529".to_string());
        cv_serial.attributes.insert("name".to_string(), "instrument serial number".to_string());
        cv_serial.attributes.insert("value".to_string(), serial_number.to_string());
        root.children.push(XMLNode::Element(cv_serial));
    }

    element_to_string(&root)
}
