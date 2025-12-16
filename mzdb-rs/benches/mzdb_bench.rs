//! Benchmarks for mzdb-rs
//!
//! Run with: cargo bench -p mzdb-rs

use criterion::{black_box, criterion_group, criterion_main, Criterion, BenchmarkId};
use mzdb::cache::create_entity_cache;
use mzdb::iterator::for_each_spectrum;
use mzdb::queries::{get_spectrum, get_mzdb_version, list_data_encodings};
use rusqlite::Connection;
use std::path::PathBuf;

/// Get path to test fixture
fn test_db_path() -> PathBuf {
    let mut path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    path.push("data");
    path.push("small.mzDB");
    path
}
//const TEST_DB_PATH: &str = "./data/small.mzDB";

fn bench_open_and_cache(c: &mut Criterion) {
    c.bench_function("open_db_and_create_cache", |b| {
        b.iter(|| {
            let db = Connection::open(black_box(test_db_path())).unwrap();
            let _cache = create_entity_cache(&db).unwrap();
        });
    });
}

fn bench_get_spectrum(c: &mut Criterion) {
    let db = Connection::open(test_db_path()).unwrap();
    let cache = create_entity_cache(&db).unwrap();
    
    let mut group = c.benchmark_group("get_spectrum");
    
    for spectrum_id in [1, 10, 50, 100].iter() {
        if *spectrum_id <= cache.spectrum_headers.len() as i64 {
            group.bench_with_input(
                BenchmarkId::from_parameter(spectrum_id),
                spectrum_id,
                |b, &id| {
                    b.iter(|| {
                        let _ = get_spectrum(&db, black_box(id), &cache).unwrap();
                    });
                },
            );
        }
    }
    
    group.finish();
}

fn bench_iterate_all_spectra(c: &mut Criterion) {
    let db = Connection::open(test_db_path()).unwrap();
    let cache = create_entity_cache(&db).unwrap();
    
    c.bench_function("iterate_all_spectra", |b| {
        b.iter(|| {
            let mut count = 0;
            for_each_spectrum(&db, &cache, None, |_s| {
                count += 1;
                Ok(())
            }).unwrap();
            black_box(count)
        });
    });
}

fn bench_iterate_ms1_only(c: &mut Criterion) {
    let db = Connection::open(test_db_path()).unwrap();
    let cache = create_entity_cache(&db).unwrap();
    
    c.bench_function("iterate_ms1_only", |b| {
        b.iter(|| {
            let mut count = 0;
            for_each_spectrum(&db, &cache, Some(1), |_s| {
                count += 1;
                Ok(())
            }).unwrap();
            black_box(count)
        });
    });
}

fn bench_metadata_queries(c: &mut Criterion) {
    let db = Connection::open(test_db_path()).unwrap();
    
    let mut group = c.benchmark_group("metadata_queries");
    
    group.bench_function("get_mzdb_version", |b| {
        b.iter(|| {
            let _ = get_mzdb_version(black_box(&db)).unwrap();
        });
    });
    
    group.bench_function("list_data_encodings", |b| {
        b.iter(|| {
            let _ = list_data_encodings(black_box(&db)).unwrap();
        });
    });
    
    group.finish();
}

criterion_group!(
    benches,
    bench_open_and_cache,
    bench_get_spectrum,
    bench_iterate_all_spectra,
    bench_iterate_ms1_only,
    bench_metadata_queries,
);

criterion_main!(benches);
