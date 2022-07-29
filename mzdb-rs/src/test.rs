#![allow(
dead_code,
unused_imports,
unused_variables
)]

use anyhow::*;
use std::collections::HashMap;
use rusqlite::{Connection, DatabaseName, MappedRows, OptionalExtension, Row, Statement};
use rusqlite::{Result as RusqliteResult};

use crate::anyhow_ext::*;
use crate::model::*;
use crate::mzdb::create_entity_cache;
use crate::queries::*;

#[test]
pub fn run_basic_tests() -> Result<()>  {
    let db = Connection::open("./data/OVEMB150205_12.mzDB")?;

    let version_opt = get_mzdb_version(&db).location(here!())?;
    //assert_eq!(version_opt.unwrap(), 2, "invalid number of version for mzdb");

    let param_tree_chromatogram_res = get_param_tree_chromatogram_res(&db).location(here!())?;

    let spectrum_id:i64 = 2;
    let get_param_tree_spectrum_res = get_param_tree_spectrum(&db, spectrum_id).location(here!())?;

    let get_param_tree_mzdb_res =get_param_tree_mzdb(&db).location(here!())?;

    let get_pwiz_mzdb_version_res = get_pwiz_mzdb_version(&db).location(here!())?;

    let get_last_time_res=get_last_time(&db).location(here!())?;
    assert_eq!(get_last_time_res.unwrap(), 240.86351, "invalid number of last time for time ");
    /*if get_last_time.is_some() {
        println!("version is {}", get_last_time.unwrap());
    } */
    let get_max_ms_level_res= get_max_ms_level(&db).location(here!())?;
    assert_eq!(get_max_ms_level_res.unwrap(), 2, "invalid max number of ms level for run slice ");

    let get_bounding_boxes_count_from_sequence= get_table_records_count(&db, "bounding_box").location(here!())?;

    let ms_level=1;
    let (begin_mz, end_mz) = db.prepare(
        format!("SELECT min(begin_mz), max(end_mz) FROM run_slice WHERE ms_level={}", ms_level).as_str()
    )?.query_and_then([], |row|  {
        let min_value: RusqliteResult<f32> = row.get(0);
        let max_value: RusqliteResult<f32> = row.get(1);

        let tuple1 = if min_value.is_err() {
            Err(anyhow!("can't get min begin_mz because: {}", min_value.err().unwrap().to_string()))
        } else if max_value.is_err() {
            Err(anyhow!("can't get max end_mz because: {}", max_value.err().unwrap().to_string()))
        } else {
            Result::Ok((min_value.unwrap(), max_value.unwrap()))
        };
        // With multi-try
        /*let tuple = min_value.and_try(max_value).map_err(|err| {
            anyhow::Error::msg("can't get min or max value")
        });*/
        tuple1
    })?.next().ok_or(anyhow::Error::msg("no record returned"))??;

    let run_slice_id = 1;
    let get_bounding_boxes_count_res= get_run_slice_bounding_boxes_count(&db, run_slice_id).location(here!())?;
    assert_eq!(get_bounding_boxes_count_res.unwrap(), 15, "invalid number of bounding boxes for run slice {}", run_slice_id);

    let get_cycles_count_res = get_last_cycle_number(&db).location(here!())?;
    assert_eq!(get_cycles_count_res.unwrap(),158, "invalid number of last cycle from spectrum");

    let get_data_encodings_count_from_sequence= get_table_records_count(&db, "data_encoding").location(here!())?;
    assert_eq!(get_data_encodings_count_from_sequence.unwrap(),1, "invalid number of table records count for data_encoding");

    let get_spectra_count_from_sequence= get_table_records_count(&db, "spectrum").location(here!())?;
    assert_eq!(get_spectra_count_from_sequence.unwrap(),1193, "invalid number of table records count for spectra");

    let get_spectra_count_res = get_spectra_count_single_ms_level(&db, ms_level).location(here!())?;
    assert_eq!(get_spectra_count_res.unwrap(),158, "invalid number spectra count");

    let get_run_slices_count_from_sequence= get_table_records_count(&db, "run_slice").location(here!())?;
    assert_eq!(get_run_slices_count_from_sequence.unwrap(),161, "invalid number of table records count for run_slice");

    let name="shared_param_tree";
    let get_table_records_count_res = get_table_records_count(&db, name).location(here!())?;
    assert_eq!(get_table_records_count_res.unwrap(),1, "invalid number of table records count for shared_param_tree");

    let bytes = get_bounding_box_data(&db ,1 as i64).location(here!())?;
    //assert_eq!(bytes.unwrap(),1, "");

    let first_id =1;
    let get_bounding_box_first_spectrum_id_res= get_bounding_box_first_spectrum_id (&db,first_id).location(here!())?;
    assert_eq!(get_bounding_box_first_spectrum_id_res.unwrap(),1, "invalid first bounding box for spectrum_id = {}",first_id);

    let bb_r_tree_id=22;
    let get_bounding_box_min_mz_res = get_bounding_box_min_mz(&db, bb_r_tree_id).location(here!())?;
    assert_eq!(get_bounding_box_min_mz_res.unwrap(),490.0, "invalid min_mz for bounding_box_rtree.id = {}", bb_r_tree_id);

    let get_bounding_box_min_time_res= get_bounding_box_min_time(&db, bb_r_tree_id).location(here!())?;
    assert_eq!(get_bounding_box_min_time_res.unwrap(),0.19280000030994415, "invalid min_time for bounding_box_rtree.id = {}", bb_r_tree_id);

    let bb_id= 161;
    let get_run_slice_id_res= get_run_slice_id(&db,bb_id).location(here!())?;
    assert_eq!(get_run_slice_id_res.unwrap(),158, "invalid number of run slice for bounding box id = {}",bb_id);

    let get_ms_level_from_run_slice_id_manually_res= get_ms_level_from_run_slice_id(&db, run_slice_id).location(here!())?;
    assert_eq!(get_ms_level_from_run_slice_id_manually_res.unwrap(),1, "invalid number of ms_level for run_slice_id = {}",run_slice_id);

    let get_bounding_box_ms_level_res= get_bounding_box_ms_level (&db,bb_id).location(here!())?;
    assert_eq!(get_bounding_box_ms_level_res.unwrap(),1, "invalid number for bb_id = {}",bb_id);

    let b_id=1;
    let get_data_encoding_id_res= get_data_encoding_id ( &db,b_id).location(here!())?;
    assert_eq!(get_data_encoding_id_res.unwrap(),1, "invalid data encoding id for bounding box id = {} ",b_id);

    let get_data_encoding_count_res = get_data_encoding_count (&db).location(here!())?;
    assert_eq!(get_data_encoding_count_res.unwrap(),1, "invalid number of id for data_encoding");

    let data_encodings = list_data_encodings(&db).location(here!())?;
    //assert_eq!(data_encodings.unwrap(),1, "");

    let mut data_encoding_by_id = HashMap::with_capacity(data_encodings.len());
    for de in data_encodings {
        data_encoding_by_id.insert(de.id, de);
    }

    let get_spectra_data_encoding_ids= list_get_spectra_data_encoding_ids(&db).location(here!())?;

    println!(
        "values = {:?} ",

        get_data_encoding_count_res
    );

    let de_cache = DataEncodingsCache::new(
        data_encoding_by_id,
        get_spectra_data_encoding_ids
    );

    let spec_id = 1;
    let first_spec_de_opt = de_cache.get_data_encoding_by_spectrum_id(&spec_id);
    assert_eq!(first_spec_de_opt.is_some(), true, "can't find data encoding for spectrum {}", spec_id);

    let first_spec_de = first_spec_de_opt.unwrap();
    assert_eq!(first_spec_de.mode, DataMode::CENTROID, "invalid data encoding mode for spectrum {}", spec_id);
    assert_eq!(first_spec_de.byte_order, ByteOrder::LITTLE_ENDIAN, "invalid byte oreer for spectrum {}", spec_id);
    assert_eq!(first_spec_de.peak_encoding, PeakEncoding::HIGH_RES_PEAK, "invalid peak encoding for spectrum {}", spec_id);

    let entity_cache = create_entity_cache(&db).location(here!())?;
    let spectrum = get_spectrum(&db, 1, &entity_cache).location(here!())?;

    //let xml = get_processing_method_param_tree(&db)?;

    println!(
        "mzDB version = {:?}",

        spectrum
    );
    return Ok(());
}