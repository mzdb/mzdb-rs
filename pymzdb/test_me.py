import pymzdb


#print(pymzdb.sum_as_string(1, 9))
#pymzdb.print_spectrum("../mzdb/data/OVEMB150205_12.mzDB")
#print(pymzdb.get_mzdb_version("../mzdb/data/OVEMB150205_12.mzDB"))

reader = pymzdb.MzdbReader("../mzdb-rs/data/OVEMB150205_12.mzDB")
#print(reader.is_closed)


#print(reader.get_mzdb_version())
#print(reader.get_pwiz_mzdb_version())
#print(reader.get_param_tree_chromatogram())
#print(reader.get_param_tree_spectrum(1))
print(reader.get_param_tree_mzdb())
#print(reader.get_max_ms_level())

print(reader.get_bounding_box_min_mz(22))
print(reader.get_bounding_box_min_time(22))
print(reader.get_run_slice_id(161))
print(reader.get_data_encoding_count())

reader.close()

#print(reader.get_last_time())
#print(reader.get_data_encoding_id(3))
print(reader.get_data_encodings_count())
#print(reader.get_spectra_count())
#print(reader.get_spectra_count_single_ms_level(1))
#print(reader.get_spectra_count_single_ms_level(2))
#print(reader.get_data_encodings_count_from_sequence("run_slice"))
#print(reader.get_bounding_boxes_count())
#print(reader.get_run_slice_bounding_boxes_count(1))
#print(reader.get_bounding_box_first_spectrum_id(1024))
#print(reader.get_ms_level_from_run_slice_id_manually(45))
#print(reader.get_bounding_box_ms_level(72))

#print(reader.get_spectrum_data(1).mz_list)
#print(reader.get_spectrum_data(1).intensity_list)

#sh = reader.get_spectrum(59).header
#sh_as_dict = sh.as_dict()
#print(sh_as_dict)

#sh_as_dict = dict((name, getattr(sh, name)) for name in dir(sh) if not name.startswith('__'))
#print(sh_as_dict)


#print(reader.get_spectrum(1).data.mz_list)
#print(reader.get_spectrum(1).data.intensity_list)

#def callback(spectrum_data, spectrum_index):
    #print('spectrum number: ', 1 + spectrum_index)
    #print(spectrum_data.mz_list)
#print(spectrum_data.intensity_list)

#reader.for_each_spectrum_data(1, callback)

#reader.close()

#print(reader.is_closed)

