

import pymzdb

import matplotlib.pyplot as plt
import pandas as pd
import spectrum_utils.plot as sup
import spectrum_utils.spectrum as sus

reader = pymzdb.MzdbReader("../mzdb-rs/data/OVEMB150205_12.mzDB")

precursor_mz = 718.3600
precursor_charge = 2

usi = 'mzspec:PXD004732:01650b_BC2-TUM_first_pool_53_01_01-3xHCD-1h-R2:scan:41840'
spectrum = sus.MsmsSpectrum(usi, precursor_mz, precursor_charge,
                            reader.get_spectrum_data(1).mz_list, reader.get_spectrum_data(1).intensity_list,
                            peptide='WNQLQAFWGTGK')

# Process the MS/MS spectrum.
fragment_tol_mass = 10
fragment_tol_mode = 'ppm'
spectrum = (spectrum.set_mz_range(min_mz=0, max_mz=3000)
            #.remove_precursor_peak(fragment_tol_mass, fragment_tol_mode)
            #.filter_intensity(min_intensity=0.05, max_num_peaks=50)
            .scale_intensity('root')
            .annotate_peptide_fragments(fragment_tol_mass, fragment_tol_mode,
                                        ion_types='aby'))

# Plot the MS/MS spectrum.
fig, ax = plt.subplots(figsize=(12, 6))
sup.spectrum(spectrum, ax=ax)
plt.show()

plt.savefig('./spectrum_test.png')


reader.close()