//! Mass spectrometry utilities
//!
//! This module provides common utilities for mass spectrometry calculations,
//! including m/z tolerance conversions and isotope pattern calculations.

// ============================================================================
// m/z Tolerance Conversions
// ============================================================================

/// Convert ppm tolerance to Daltons at a given m/z
///
/// # Arguments
/// * `mz` - The m/z value
/// * `ppm` - The tolerance in parts per million
///
/// # Returns
/// The tolerance in Daltons
pub fn ppm_to_da(mz: f64, ppm: f64) -> f64 {
    mz * ppm / 1_000_000.0
}

/// Convert Dalton tolerance to ppm at a given m/z
///
/// # Arguments
/// * `mz` - The m/z value
/// * `da` - The tolerance in Daltons
///
/// # Returns
/// The tolerance in ppm
pub fn da_to_ppm(mz: f64, da: f64) -> f64 {
    da / mz * 1_000_000.0
}

/// Calculate the m/z range for a given tolerance
///
/// # Arguments
/// * `mz` - The target m/z value
/// * `ppm` - The tolerance in ppm
///
/// # Returns
/// A tuple of (min_mz, max_mz)
pub fn mz_range_from_ppm(mz: f64, ppm: f64) -> (f64, f64) {
    let tolerance = ppm_to_da(mz, ppm);
    (mz - tolerance, mz + tolerance)
}

/// Check if two m/z values are within tolerance
///
/// # Arguments
/// * `mz1` - First m/z value
/// * `mz2` - Second m/z value
/// * `ppm` - Tolerance in ppm
///
/// # Returns
/// True if the values are within tolerance
pub fn mz_within_tolerance(mz1: f64, mz2: f64, ppm: f64) -> bool {
    let tolerance = ppm_to_da(mz1, ppm);
    (mz1 - mz2).abs() <= tolerance
}

// ============================================================================
// Mass Calculations
// ============================================================================

/// Neutron mass in Daltons (approximately)
pub const NEUTRON_MASS: f64 = 1.008664916;

/// Proton mass in Daltons
pub const PROTON_MASS: f64 = 1.00727647;

/// Electron mass in Daltons
pub const ELECTRON_MASS: f64 = 0.0005486;

/// Calculate molecular mass from m/z and charge
///
/// # Arguments
/// * `mz` - The m/z value
/// * `charge` - The charge state (positive integer)
///
/// # Returns
/// The molecular mass (assuming protonation)
pub fn mz_to_mass(mz: f64, charge: i32) -> f64 {
    if charge == 0 {
        return mz;
    }
    mz * charge.abs() as f64 - charge.abs() as f64 * PROTON_MASS
}

/// Calculate m/z from molecular mass and charge
///
/// # Arguments
/// * `mass` - The molecular mass
/// * `charge` - The charge state (positive integer)
///
/// # Returns
/// The m/z value (assuming protonation)
pub fn mass_to_mz(mass: f64, charge: i32) -> f64 {
    if charge == 0 {
        return mass;
    }
    (mass + charge.abs() as f64 * PROTON_MASS) / charge.abs() as f64
}

/// Calculate the m/z of an isotope peak
///
/// # Arguments
/// * `mono_mz` - Monoisotopic m/z
/// * `charge` - Charge state
/// * `isotope_index` - Isotope index (0 = monoisotopic, 1 = M+1, etc.)
///
/// # Returns
/// The m/z of the isotope peak
pub fn isotope_mz(mono_mz: f64, charge: i32, isotope_index: i32) -> f64 {
    mono_mz + (isotope_index as f64 * NEUTRON_MASS) / charge.abs() as f64
}

// ============================================================================
// Isotope Pattern
// ============================================================================

/// Theoretical isotope pattern
#[derive(Clone, Debug)]
pub struct TheoreticalIsotopePattern {
    /// Monoisotopic m/z
    pub mono_mz: f64,
    /// Charge state
    pub charge: i32,
    /// Relative intensities for each isotope (0 = mono, 1 = M+1, etc.)
    pub relative_intensities: Vec<f64>,
}

impl TheoreticalIsotopePattern {
    /// Create a new theoretical isotope pattern
    pub fn new(mono_mz: f64, charge: i32, relative_intensities: Vec<f64>) -> Self {
        Self {
            mono_mz,
            charge,
            relative_intensities,
        }
    }

    /// Create an averagine-based isotope pattern
    ///
    /// Uses simplified averagine model for peptides
    pub fn from_averagine(mono_mz: f64, charge: i32, num_isotopes: usize) -> Self {
        // Estimate mass
        let mass = mz_to_mass(mono_mz, charge);

        // Simplified averagine model (average amino acid composition)
        // The number of atoms scales with mass
        let carbon_count = (mass / 111.1254) * 4.9384; // Average carbons per residue

        // Simple Poisson-based approximation for isotope pattern
        let lambda = carbon_count * 0.0107; // C13/C12 natural abundance

        let mut intensities: Vec<f64> = Vec::with_capacity(num_isotopes);
        let mut current = 1.0f64;

        for k in 0..num_isotopes {
            if k == 0 {
                intensities.push(1.0);
            } else {
                current *= lambda / k as f64;
                intensities.push(current);
            }
        }

        // Normalize
        let max_int = intensities.iter().cloned().fold(0.0, f64::max);
        if max_int > 0.0 {
            for int in &mut intensities {
                *int /= max_int;
            }
        }

        Self::new(mono_mz, charge, intensities)
    }

    /// Get the m/z values for all isotopes
    pub fn mz_values(&self) -> Vec<f64> {
        (0..self.relative_intensities.len())
            .map(|i| isotope_mz(self.mono_mz, self.charge, i as i32))
            .collect()
    }

    /// Get the expected m/z for a specific isotope
    pub fn isotope_mz(&self, index: usize) -> Option<f64> {
        if index < self.relative_intensities.len() {
            Some(isotope_mz(self.mono_mz, self.charge, index as i32))
        } else {
            None
        }
    }

    /// Get the expected relative intensity for a specific isotope
    pub fn isotope_intensity(&self, index: usize) -> Option<f64> {
        self.relative_intensities.get(index).copied()
    }
}

// ============================================================================
// XIC Method
// ============================================================================

/// Method for extracting XIC values when multiple peaks match
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum XicMethod {
    /// Use the maximum intensity peak
    Max,
    /// Use the nearest peak to target m/z
    Nearest,
    /// Sum all matching peaks
    Sum,
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ppm_to_da() {
        let mz = 500.0;
        let ppm = 10.0;
        let da = ppm_to_da(mz, ppm);
        assert!((da - 0.005).abs() < 1e-10);
    }

    #[test]
    fn test_da_to_ppm() {
        let mz = 500.0;
        let da = 0.005;
        let ppm = da_to_ppm(mz, da);
        assert!((ppm - 10.0).abs() < 1e-10);
    }

    #[test]
    fn test_mz_range_from_ppm() {
        let mz = 500.0;
        let ppm = 10.0;
        let (min_mz, max_mz) = mz_range_from_ppm(mz, ppm);
        assert!((min_mz - 499.995).abs() < 1e-10);
        assert!((max_mz - 500.005).abs() < 1e-10);
    }

    #[test]
    fn test_mz_within_tolerance() {
        assert!(mz_within_tolerance(500.0, 500.004, 10.0));
        assert!(!mz_within_tolerance(500.0, 500.01, 10.0));
    }

    #[test]
    fn test_mz_to_mass() {
        let mz = 500.0;
        let charge = 2;
        let mass = mz_to_mass(mz, charge);
        // Mass should be approximately 2*500 - 2*proton_mass
        assert!((mass - 997.985).abs() < 0.01);
    }

    #[test]
    fn test_mass_to_mz() {
        let mass = 1000.0;
        let charge = 2;
        let mz = mass_to_mz(mass, charge);
        // m/z should be approximately (1000 + 2*proton_mass) / 2
        assert!((mz - 501.007).abs() < 0.01);
    }

    #[test]
    fn test_isotope_mz() {
        let mono_mz = 500.0;
        let charge = 2;

        let m1 = isotope_mz(mono_mz, charge, 1);
        let expected_shift = NEUTRON_MASS / 2.0;
        assert!((m1 - mono_mz - expected_shift).abs() < 1e-6);
    }

    #[test]
    fn test_theoretical_isotope_pattern() {
        let pattern = TheoreticalIsotopePattern::from_averagine(600.0, 2, 5);

        assert_eq!(pattern.mono_mz, 600.0);
        assert_eq!(pattern.charge, 2);
        assert_eq!(pattern.relative_intensities.len(), 5);

        // First isotope should be at or near 1.0 (normalized)
        assert!((pattern.relative_intensities[0] - 1.0).abs() < 0.1);

        // m/z values should be increasing
        let mzs = pattern.mz_values();
        for i in 1..mzs.len() {
            assert!(mzs[i] > mzs[i - 1]);
        }
    }
}
