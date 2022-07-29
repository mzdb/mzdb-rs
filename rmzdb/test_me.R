reader <- MzdbReader$new("../mzdb-rs/data/OVEMB150205_12.mzDB")

spectrum <- reader$get_spectrum(1)
spectrum_header <- spectrum$header()

print(paste0("param_tree_str: ",spectrum_header$param_tree_str()))

print(paste0("precursor_charge: ",spectrum_header$precursor_charge()))

print("spectrum as matrix:")
m <- spectrum$data()$as_matrix()
print(head(m))

reader$for_each_spectrum(NA, function(s) {
    print(s$header()$time())
})