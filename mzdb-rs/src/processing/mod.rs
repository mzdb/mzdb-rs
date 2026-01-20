//! mzdb-processing - LC-MS signal processing algorithms
//!
//! This module provides signal processing algorithms for mass spectrometry data,
//! ported from the profiproteomics/mzdb-processing Scala library.
//!
//! # Features
//!
//! - **Signal filtering**: Savitzky-Golay smoothing, baseline removal, partial smoothing
//! - **Peak detection**: Basic, Smart, and Histogram-based peakel finder algorithms
//! - **Math utilities**: Derivative analysis, histogram computation, statistical functions
//! - **MS utilities**: m/z tolerance conversions, isotope pattern calculations
//! - **DIA processing**: MS2 peakel detection for Data Independent Acquisition data
//! - **DIA simplifier** (with `shrinkdia` feature): Simplify DIA files using peakels
//!
//! # Example
//!
//! ```rust
//! use mzdb::processing::signal::filtering::{SavitzkyGolaySmoother, SignalSmoother};
//! use mzdb::processing::signal::finder::{SmartPeakelFinder, PeakelFinder};
//!
//! // Create some time-intensity pairs
//! let data: Vec<(f32, f64)> = vec![
//!     (1.0, 100.0), (2.0, 200.0), (3.0, 500.0),
//!     (4.0, 400.0), (5.0, 100.0), (6.0, 150.0),
//!     (7.0, 200.0), (8.0, 150.0), (9.0, 100.0),
//! ];
//!
//! // Smooth the data
//! let smoother = SavitzkyGolaySmoother::new(2, 2, 1);
//! let smoothed = smoother.smooth_time_intensity_pairs(&data);
//!
//! // Find peakels
//! let finder = SmartPeakelFinder::new();
//! let peakels = finder.find_peakels_indices(&data);
//! ```
//!
//! # Module Organization
//!
//! - [`signal`]: Signal processing algorithms (filtering and detection)
//! - [`math`]: Mathematical utilities (derivatives, histograms, statistics)
//! - [`ms`]: Mass spectrometry utilities (m/z conversions, isotope patterns)
//! - [`model`]: Core data structures (Peak, Peakel, Feature, etc.)
//! - [`dia`]: DIA (Data Independent Acquisition) MS2 peakel detection
//! - [`dia_simplifier`]: DIA file simplification (requires `shrinkdia` feature)
//! - [`staggered`]: Staggered DIA detection and unstaggering support

pub mod signal;
pub mod math;
pub mod ms;
pub mod model;
pub mod dia;
pub mod peakeldb;
pub mod staggered;

#[cfg(feature = "shrinkdia")]
pub mod dia_simplifier;

// Re-export commonly used types
pub use model::{
    Peak, Peakel, PeakelBuilder, Feature, PutativeFeature,
    LcContext, RtIntensityPair, RtIntensityPairs,
    XicPeak, DetectedPeak,
    HasPeakelData,
};

pub use signal::filtering::{
    SignalSmoother, SavitzkyGolaySmoother, SavitzkyGolaySmoothingConfig,
    PartialSavitzkyGolaySmoother, BaselineRemover,
    XicBinner, XicBinnerConfig, ExtendedBin, Bin,
    compute_sg_coefficients,
};

pub use signal::finder::{
    PeakelFinder, BasicPeakelFinder, SmartPeakelFinder, SmartPeakelFinderConfig,
    HistogramBasedPeakelFinder,
};

// Re-export common detection utilities
pub use signal::detection::{
    find_nearest_peak_from_slices,
    sort_indices_by_descending_f32_value, is_target_mz_within_range,
    create_peakel_finder,
};

// Re-export MS1 detection types
pub use signal::ms1_detection::{
    Ms1PeakelDetector, Ms1PeakelConfig,
};

// Re-export MS2 detection types (also available via dia module)
pub use signal::ms2_detection::{
    IsolationWindow, DiaMs2PeakelRecord, DiaMs2PeakelConfig,
    DiaMs2PeakelDetector,
};

pub use ms::{
    ppm_to_da, da_to_ppm, mz_range_from_ppm, mz_within_tolerance,
    mz_to_mass, mass_to_mz, isotope_mz,
    TheoreticalIsotopePattern, XicMethod,
    NEUTRON_MASS, PROTON_MASS, ELECTRON_MASS,
};

pub use math::{
    calc_ternary_slopes, LocalExtremum, find_local_extrema, filter_significant_extrema,
    HistogramBin, compute_histogram, compute_histogram_2d,
    median, mad, robust_noise_threshold,
};

// Re-export DIA types (for backward compatibility, these come from signal::ms2_detection)
pub use dia::write_dia_peakeldb;

// Re-export peakeldb types
pub use peakeldb::{
    ExtendedPeakel, PeakelSerializer,
    chrono_lite_timestamp,
    Ms1PeakelDbReader, Ms1PeakelDbWriter, Ms1PeakelDbSchema, Ms1PeakelRecord,
    Ms2PeakelDbReader, Ms2PeakelDbWriter, Ms2PeakelDbSchema,
};

#[cfg(feature = "dda2dia")]
pub mod diafication;
#[cfg(feature = "dda2dia")]
pub use diafication::{
    Dda2DiaConverter, DiaConversionOptions, DiaConversionStats
};


// Re-export DIA simplifier types (when feature enabled)
#[cfg(feature = "shrinkdia")]
pub use dia_simplifier::{
    DiaSimplifier, DiaSimplifierConfig, SimplifiedSpectrum,
    SimplificationStats, SpectrumHeader,
};

// Re-export staggered DIA types
pub use staggered::{
    StaggeredDiaDetector, StaggeredDiaInfo, StaggeredPeakelConfig,
    UnstaggeredWindow, UnstaggeredWindowType,
    MzTolerance, SingleObservationStrategy, MultipleMatchStrategy,
    StaggeredPeakelMatcher, PeakelMatchResult, MatchedPeakelPair,
    SingleObservationPeakel, AmbiguousMatch, CycleType,
    PeakelMerger, MergedPeakel, MergeStats, MergeConfig,
};