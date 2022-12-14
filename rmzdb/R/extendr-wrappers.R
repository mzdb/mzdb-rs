# Generated by extendr: Do not edit by hand
#
# This file was created with the following call:
#   .Call("wrap__make_rmzdb_wrappers", use_symbols = TRUE, package_name = "rmzdb")

#' @docType package
#' @usage NULL
#' @useDynLib rmzdb, .registration = TRUE
NULL

#' Return string `"Hello world!"` to R.
#' @export
hello_world <- function() .Call(wrap__hello_world)

get_mzdb_version <- function(path) .Call(wrap__get_mzdb_version, path)

MzdbReader <- new.env(parent = emptyenv())

MzdbReader$new <- function(path) .Call(wrap__MzdbReader__new, path)

MzdbReader$close <- function() .Call(wrap__MzdbReader__close, self)

MzdbReader$is_closed <- function() .Call(wrap__MzdbReader__is_closed, self)

MzdbReader$get_mzdb_version <- function() .Call(wrap__MzdbReader__get_mzdb_version, self)

MzdbReader$get_pwiz_mzdb_version <- function() .Call(wrap__MzdbReader__get_pwiz_mzdb_version, self)

MzdbReader$get_param_tree_chromatogram <- function() .Call(wrap__MzdbReader__get_param_tree_chromatogram, self)

MzdbReader$get_param_tree_spectrum <- function(spectrum_id) .Call(wrap__MzdbReader__get_param_tree_spectrum, self, spectrum_id)

MzdbReader$get_param_tree_mzdb <- function() .Call(wrap__MzdbReader__get_param_tree_mzdb, self)

MzdbReader$get_last_cycle_number <- function() .Call(wrap__MzdbReader__get_last_cycle_number, self)

MzdbReader$get_last_time <- function() .Call(wrap__MzdbReader__get_last_time, self)

MzdbReader$get_max_ms_level <- function() .Call(wrap__MzdbReader__get_max_ms_level, self)

MzdbReader$get_run_slice_bounding_boxes_count <- function(run_slice_id) .Call(wrap__MzdbReader__get_run_slice_bounding_boxes_count, self, run_slice_id)

MzdbReader$get_spectra_count_single_ms_level <- function(ms_level) .Call(wrap__MzdbReader__get_spectra_count_single_ms_level, self, ms_level)

MzdbReader$get_data_encodings_count <- function() .Call(wrap__MzdbReader__get_data_encodings_count, self)

MzdbReader$get_bounding_boxes_count <- function() .Call(wrap__MzdbReader__get_bounding_boxes_count, self)

MzdbReader$get_spectra_count <- function() .Call(wrap__MzdbReader__get_spectra_count, self)

MzdbReader$get_bounding_box_first_spectrum_id <- function(first_id) .Call(wrap__MzdbReader__get_bounding_box_first_spectrum_id, self, first_id)

MzdbReader$get_bounding_box_min_mz <- function(bb_r_tree_id) .Call(wrap__MzdbReader__get_bounding_box_min_mz, self, bb_r_tree_id)

MzdbReader$get_bounding_box_min_time <- function(bb_r_tree_id) .Call(wrap__MzdbReader__get_bounding_box_min_time, self, bb_r_tree_id)

MzdbReader$get_run_slice_id <- function(bb_id) .Call(wrap__MzdbReader__get_run_slice_id, self, bb_id)

MzdbReader$get_ms_level_from_run_slice_id <- function(run_slice_id) .Call(wrap__MzdbReader__get_ms_level_from_run_slice_id, self, run_slice_id)

MzdbReader$get_bounding_box_ms_level <- function(bb_id) .Call(wrap__MzdbReader__get_bounding_box_ms_level, self, bb_id)

MzdbReader$get_data_encoding_id <- function(bb_id) .Call(wrap__MzdbReader__get_data_encoding_id, self, bb_id)

MzdbReader$get_data_encoding_count <- function() .Call(wrap__MzdbReader__get_data_encoding_count, self)

MzdbReader$get_spectrum <- function(spectrum_id) .Call(wrap__MzdbReader__get_spectrum, self, spectrum_id)

MzdbReader$for_each_spectrum <- function(ms_level, on_each_spectrum_fn) .Call(wrap__MzdbReader__for_each_spectrum, self, ms_level, on_each_spectrum_fn)

#' @export
`$.MzdbReader` <- function (self, name) { func <- MzdbReader[[name]]; environment(func) <- environment(); func }

#' @export
`[[.MzdbReader` <- `$.MzdbReader`

MzdbSpectrum <- new.env(parent = emptyenv())

MzdbSpectrum$header <- function() .Call(wrap__MzdbSpectrum__header, self)

MzdbSpectrum$data <- function() .Call(wrap__MzdbSpectrum__data, self)

#' @export
`$.MzdbSpectrum` <- function (self, name) { func <- MzdbSpectrum[[name]]; environment(func) <- environment(); func }

#' @export
`[[.MzdbSpectrum` <- `$.MzdbSpectrum`

MzdbSpectrumHeader <- new.env(parent = emptyenv())

MzdbSpectrumHeader$id <- function() .Call(wrap__MzdbSpectrumHeader__id, self)

MzdbSpectrumHeader$initial_id <- function() .Call(wrap__MzdbSpectrumHeader__initial_id, self)

MzdbSpectrumHeader$title <- function() .Call(wrap__MzdbSpectrumHeader__title, self)

MzdbSpectrumHeader$cycle <- function() .Call(wrap__MzdbSpectrumHeader__cycle, self)

MzdbSpectrumHeader$time <- function() .Call(wrap__MzdbSpectrumHeader__time, self)

MzdbSpectrumHeader$ms_level <- function() .Call(wrap__MzdbSpectrumHeader__ms_level, self)

MzdbSpectrumHeader$activation_type <- function() .Call(wrap__MzdbSpectrumHeader__activation_type, self)

MzdbSpectrumHeader$tic <- function() .Call(wrap__MzdbSpectrumHeader__tic, self)

MzdbSpectrumHeader$base_peak_mz <- function() .Call(wrap__MzdbSpectrumHeader__base_peak_mz, self)

MzdbSpectrumHeader$base_peak_intensity <- function() .Call(wrap__MzdbSpectrumHeader__base_peak_intensity, self)

MzdbSpectrumHeader$precursor_mz <- function() .Call(wrap__MzdbSpectrumHeader__precursor_mz, self)

MzdbSpectrumHeader$precursor_charge <- function() .Call(wrap__MzdbSpectrumHeader__precursor_charge, self)

MzdbSpectrumHeader$peaks_count <- function() .Call(wrap__MzdbSpectrumHeader__peaks_count, self)

MzdbSpectrumHeader$param_tree_str <- function() .Call(wrap__MzdbSpectrumHeader__param_tree_str, self)

MzdbSpectrumHeader$scan_list_str <- function() .Call(wrap__MzdbSpectrumHeader__scan_list_str, self)

MzdbSpectrumHeader$precursor_list_str <- function() .Call(wrap__MzdbSpectrumHeader__precursor_list_str, self)

MzdbSpectrumHeader$product_list_str <- function() .Call(wrap__MzdbSpectrumHeader__product_list_str, self)

MzdbSpectrumHeader$shared_param_tree_id <- function() .Call(wrap__MzdbSpectrumHeader__shared_param_tree_id, self)

MzdbSpectrumHeader$instrument_configuration_id <- function() .Call(wrap__MzdbSpectrumHeader__instrument_configuration_id, self)

MzdbSpectrumHeader$source_file_id <- function() .Call(wrap__MzdbSpectrumHeader__source_file_id, self)

MzdbSpectrumHeader$run_id <- function() .Call(wrap__MzdbSpectrumHeader__run_id, self)

MzdbSpectrumHeader$data_processing_id <- function() .Call(wrap__MzdbSpectrumHeader__data_processing_id, self)

MzdbSpectrumHeader$data_encoding_id <- function() .Call(wrap__MzdbSpectrumHeader__data_encoding_id, self)

MzdbSpectrumHeader$bb_first_spectrum_id <- function() .Call(wrap__MzdbSpectrumHeader__bb_first_spectrum_id, self)

#' @export
`$.MzdbSpectrumHeader` <- function (self, name) { func <- MzdbSpectrumHeader[[name]]; environment(func) <- environment(); func }

#' @export
`[[.MzdbSpectrumHeader` <- `$.MzdbSpectrumHeader`

MzdbSpectrumData <- new.env(parent = emptyenv())

MzdbSpectrumData$mz_list <- function() .Call(wrap__MzdbSpectrumData__mz_list, self)

MzdbSpectrumData$intensity_list <- function() .Call(wrap__MzdbSpectrumData__intensity_list, self)

MzdbSpectrumData$as_matrix <- function() .Call(wrap__MzdbSpectrumData__as_matrix, self)

#' @export
`$.MzdbSpectrumData` <- function (self, name) { func <- MzdbSpectrumData[[name]]; environment(func) <- environment(); func }

#' @export
`[[.MzdbSpectrumData` <- `$.MzdbSpectrumData`

