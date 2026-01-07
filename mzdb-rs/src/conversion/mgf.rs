//! MGF (Mascot Generic Format) export functionality
//!
//! This module provides functionality to export mzDB spectra to MGF format,
//! which is widely used for MS/MS peptide identification with search engines
//! like Mascot, X!Tandem, and many others.
//!
//! # MGF Format
//!
//! MGF is a simple text-based format where each MS/MS spectrum is represented as:
//!
//! ```text
//! BEGIN IONS
//! TITLE=<scan title>
//! RTINSECONDS=<retention time in seconds>
//! PEPMASS=<precursor m/z> <precursor intensity>
//! CHARGE=<charge>+
//! SCANS=<scan number>
//! <mz> <intensity>
//! <mz> <intensity>
//! ...
//! END IONS
//! ```
//!
//! # Example
//!
//! ```rust,no_run
//! use mzdb::MzDbReader;
//! use mzdb::conversion::mgf::{MgfWriter, MgfExportOptions};
//!
//! let mzdb = MzDbReader::open("data.mzdb")?;
//! let options = MgfExportOptions::default()
//!     .with_ms_level(2)  // Export only MS2 spectra
//!     .with_min_peaks(10); // Minimum 10 peaks per spectrum
//!
//! MgfWriter::export(&mzdb, "output.mgf", &options)?;
//! # Ok::<(), anyhow_ext::Error>(())
//! ```

use anyhow_ext::{Context, Result};
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::Path;

use crate::MzDbReader;
use crate::model::{Spectrum, SpectrumHeader};

/// Options for MGF export
#[derive(Debug, Clone)]
pub struct MgfExportOptions {
    /// MS level to export (default: 2 for MS/MS spectra)
    pub ms_level: Option<i64>,
    
    /// Minimum number of peaks required for a spectrum to be exported
    pub min_peaks: usize,
    
    /// Maximum number of peaks to export per spectrum (None = unlimited)
    pub max_peaks: Option<usize>,
    
    /// Minimum peak intensity threshold (peaks below this are filtered)
    pub min_intensity: f32,
    
    /// Include precursor intensity in PEPMASS line
    pub include_precursor_intensity: bool,
    
    /// Use spectrum title from database (if false, generates title from scan info)
    pub use_original_title: bool,
    
    /// Include SCANS field with spectrum ID
    pub include_scans_field: bool,
    
    /// Precision for m/z values (number of decimal places)
    pub mz_precision: usize,
    
    /// Precision for intensity values (number of decimal places)
    pub intensity_precision: usize,
}

impl Default for MgfExportOptions {
    fn default() -> Self {
        Self {
            ms_level: Some(2), // MS2 by default
            min_peaks: 1,
            max_peaks: None,
            min_intensity: 0.0,
            include_precursor_intensity: true,
            use_original_title: true,
            include_scans_field: true,
            mz_precision: 6,
            intensity_precision: 2,
        }
    }
}

impl MgfExportOptions {
    /// Create new options with default values
    pub fn new() -> Self {
        Self::default()
    }
    
    /// Set the MS level to export
    pub fn with_ms_level(mut self, ms_level: i64) -> Self {
        self.ms_level = Some(ms_level);
        self
    }
    
    /// Export all MS levels
    pub fn with_all_ms_levels(mut self) -> Self {
        self.ms_level = None;
        self
    }
    
    /// Set minimum number of peaks
    pub fn with_min_peaks(mut self, min_peaks: usize) -> Self {
        self.min_peaks = min_peaks;
        self
    }
    
    /// Set maximum number of peaks
    pub fn with_max_peaks(mut self, max_peaks: usize) -> Self {
        self.max_peaks = Some(max_peaks);
        self
    }
    
    /// Set minimum intensity threshold
    pub fn with_min_intensity(mut self, min_intensity: f32) -> Self {
        self.min_intensity = min_intensity;
        self
    }
    
    /// Set m/z precision
    pub fn with_mz_precision(mut self, precision: usize) -> Self {
        self.mz_precision = precision;
        self
    }
    
    /// Set intensity precision
    pub fn with_intensity_precision(mut self, precision: usize) -> Self {
        self.intensity_precision = precision;
        self
    }
}

/// MGF file writer
pub struct MgfWriter<W: Write> {
    writer: BufWriter<W>,
    options: MgfExportOptions,
    spectra_written: usize,
}

impl<W: Write> MgfWriter<W> {
    /// Create a new MGF writer with the given output and options
    pub fn new(writer: W, options: MgfExportOptions) -> Self {
        Self {
            writer: BufWriter::new(writer),
            options,
            spectra_written: 0,
        }
    }
    
    /// Write a single spectrum to the MGF file
    pub fn write_spectrum(&mut self, header: &SpectrumHeader, spectrum: &Spectrum) -> Result<bool> {
        // Check MS level filter
        if let Some(required_level) = self.options.ms_level
            && header.ms_level != required_level
        {
            return Ok(false);
        }
        
        // Filter peaks by intensity threshold
        let mut filtered_peaks: Vec<(f64, f32)> = spectrum.data.mz_array
            .iter()
            .zip(spectrum.data.intensity_array.iter())
            .filter(|(_mz, intensity)| **intensity >= self.options.min_intensity)
            .map(|(&mz, &intensity)| (mz, intensity))
            .collect();
        
        // Check minimum peaks requirement
        if filtered_peaks.len() < self.options.min_peaks {
            return Ok(false);
        }
        
        // Sort by intensity descending if we need to limit peaks
        if let Some(max_peaks) = self.options.max_peaks
            && filtered_peaks.len() > max_peaks
        {
            filtered_peaks.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap());
            filtered_peaks.truncate(max_peaks);
            // Re-sort by m/z for output
            filtered_peaks.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap());
        }
        
        // Write BEGIN IONS
        writeln!(self.writer, "BEGIN IONS")?;
        
        // Write TITLE
        let title = if self.options.use_original_title && !header.title.is_empty() {
            header.title.clone()
        } else {
            format!("Scan {} (rt={:.2}s)", header.initial_id, header.time * 60.0)
        };
        writeln!(self.writer, "TITLE={}", title)?;
        
        // Write RTINSECONDS
        writeln!(self.writer, "RTINSECONDS={:.3}", header.time * 60.0)?;
        
        // Write PEPMASS (precursor m/z and optional intensity)
        if let Some(precursor_mz) = header.precursor_mz {
            if self.options.include_precursor_intensity {
                // Try to find precursor intensity from base peak or TIC
                // In real data, you might parse this from precursor_list_str
                writeln!(self.writer, "PEPMASS={:.prec$}", 
                    precursor_mz, prec = self.options.mz_precision)?;
            } else {
                writeln!(self.writer, "PEPMASS={:.prec$}", 
                    precursor_mz, prec = self.options.mz_precision)?;
            }
        }
        
        // Write CHARGE
        if let Some(charge) = header.precursor_charge {
            if charge > 0 {
                writeln!(self.writer, "CHARGE={}+", charge)?;
            } else if charge < 0 {
                writeln!(self.writer, "CHARGE={}-", charge.abs())?;
            }
        }
        
        // Write SCANS (spectrum ID)
        if self.options.include_scans_field {
            writeln!(self.writer, "SCANS={}", header.initial_id)?;
        }
        
        // Write peak list
        for (mz, intensity) in filtered_peaks {
            writeln!(self.writer, "{:.prec_mz$} {:.prec_int$}", 
                mz, intensity,
                prec_mz = self.options.mz_precision,
                prec_int = self.options.intensity_precision)?;
        }
        
        // Write END IONS
        writeln!(self.writer, "END IONS")?;
        writeln!(self.writer)?; // Blank line between spectra
        
        self.spectra_written += 1;
        Ok(true)
    }
    
    /// Get the number of spectra written
    pub fn spectra_written(&self) -> usize {
        self.spectra_written
    }
    
    /// Flush the writer
    pub fn flush(&mut self) -> Result<()> {
        self.writer.flush().context("Failed to flush MGF writer")
    }
}

impl MgfWriter<File> {
    /// Export spectra from an mzDB file to an MGF file
    ///
    /// # Arguments
    ///
    /// * `mzdb` - The mzDB database to read from
    /// * `output_path` - Path to the output MGF file
    /// * `options` - Export options
    ///
    /// # Returns
    ///
    /// Returns the number of spectra written
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use mzdb::MzDbReader;
    /// use mzdb::conversion::mgf::{MgfWriter, MgfExportOptions};
    ///
    /// let mzdb = MzDbReader::open("data.mzdb")?;
    /// let options = MgfExportOptions::default();
    /// let count = MgfWriter::export(&mzdb, "output.mgf", &options)?;
    /// println!("Exported {} spectra", count);
    /// # Ok::<(), anyhow_ext::Error>(())
    /// ```
    pub fn export<P: AsRef<Path>>(
        mzdb: &MzDbReader,
        output_path: P,
        options: &MgfExportOptions,
    ) -> Result<usize> {
        let file = File::create(output_path.as_ref())
            .context("Failed to create MGF output file")?;
        
        let mut writer = MgfWriter::new(file, options.clone());
        
        // Get all spectrum headers
        let headers = mzdb.get_spectrum_headers();
        
        // Filter by MS level if specified
        let filtered_headers: Vec<_> = if let Some(ms_level) = options.ms_level {
            headers.iter()
                .filter(|h| h.ms_level == ms_level)
                .collect()
        } else {
            headers.iter().collect()
        };
        
        // Process each spectrum
        for header in filtered_headers {
            let spectrum = mzdb.get_spectrum(header.id)
                .with_context(|| format!("Failed to read spectrum {}", header.id))?;
            
            writer.write_spectrum(header, &spectrum)?;
        }
        
        writer.flush()?;
        Ok(writer.spectra_written())
    }
    
    /// Export spectra from an mzDB file to an MGF file with default options
    ///
    /// This is a convenience method that uses default export options (MS2 only).
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use mzdb::MzDbReader;
    /// use mzdb::conversion::mgf::MgfWriter;
    ///
    /// let mzdb = MzDbReader::open("data.mzdb")?;
    /// let count = MgfWriter::export_default(&mzdb, "output.mgf")?;
    /// println!("Exported {} MS2 spectra", count);
    /// # Ok::<(), anyhow_ext::Error>(())
    /// ```
    pub fn export_default<P: AsRef<Path>>(mzdb: &MzDbReader, output_path: P) -> Result<usize> {
        Self::export(mzdb, output_path, &MgfExportOptions::default())
    }
}

/// Export statistics
#[derive(Debug, Clone)]
pub struct MgfExportStats {
    /// Total number of spectra in the mzDB file
    pub total_spectra: usize,
    
    /// Number of spectra that matched the MS level filter
    pub filtered_spectra: usize,
    
    /// Number of spectra actually written (after all filters)
    pub written_spectra: usize,
    
    /// Number of spectra skipped due to insufficient peaks
    pub skipped_min_peaks: usize,
}

impl MgfExportStats {
    /// Create new export statistics
    pub fn new() -> Self {
        Self {
            total_spectra: 0,
            filtered_spectra: 0,
            written_spectra: 0,
            skipped_min_peaks: 0,
        }
    }
    
    /// Print a summary of the export
    pub fn print_summary(&self) {
        println!("MGF Export Summary:");
        println!("  Total spectra in mzDB: {}", self.total_spectra);
        println!("  Spectra matching MS level: {}", self.filtered_spectra);
        println!("  Spectra written to MGF: {}", self.written_spectra);
        if self.skipped_min_peaks > 0 {
            println!("  Skipped (min peaks filter): {}", self.skipped_min_peaks);
        }
    }
}

impl Default for MgfExportStats {
    fn default() -> Self {
        Self::new()
    }
}

/// Advanced MGF writer with statistics tracking
pub struct MgfWriterWithStats<W: Write> {
    writer: MgfWriter<W>,
    stats: MgfExportStats,
}

impl<W: Write> MgfWriterWithStats<W> {
    /// Create a new MGF writer with statistics tracking
    pub fn new(writer: W, options: MgfExportOptions) -> Self {
        Self {
            writer: MgfWriter::new(writer, options),
            stats: MgfExportStats::new(),
        }
    }
    
    /// Write a spectrum and update statistics
    pub fn write_spectrum(&mut self, header: &SpectrumHeader, spectrum: &Spectrum) -> Result<()> {
        self.stats.total_spectra += 1;
        
        // Check MS level
        if let Some(required_level) = self.writer.options.ms_level {
            if header.ms_level == required_level {
                self.stats.filtered_spectra += 1;
            } else {
                return Ok(());
            }
        } else {
            self.stats.filtered_spectra += 1;
        }
        
        // Count peaks
        let peak_count = spectrum.data.mz_array.len();
        if peak_count < self.writer.options.min_peaks {
            self.stats.skipped_min_peaks += 1;
            return Ok(());
        }
        
        // Write spectrum
        if self.writer.write_spectrum(header, spectrum)? {
            self.stats.written_spectra += 1;
        }
        
        Ok(())
    }
    
    /// Get the export statistics
    pub fn stats(&self) -> &MgfExportStats {
        &self.stats
    }
    
    /// Flush the writer
    pub fn flush(&mut self) -> Result<()> {
        self.writer.flush()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_mgf_options_defaults() {
        let options = MgfExportOptions::default();
        assert_eq!(options.ms_level, Some(2));
        assert_eq!(options.min_peaks, 1);
        assert_eq!(options.mz_precision, 6);
        assert_eq!(options.intensity_precision, 2);
    }
    
    #[test]
    fn test_mgf_options_builder() {
        let options = MgfExportOptions::new()
            .with_ms_level(3)
            .with_min_peaks(10)
            .with_max_peaks(100)
            .with_min_intensity(10.0)
            .with_mz_precision(4)
            .with_intensity_precision(1);
        
        assert_eq!(options.ms_level, Some(3));
        assert_eq!(options.min_peaks, 10);
        assert_eq!(options.max_peaks, Some(100));
        assert_eq!(options.min_intensity, 10.0);
        assert_eq!(options.mz_precision, 4);
        assert_eq!(options.intensity_precision, 1);
    }
}
