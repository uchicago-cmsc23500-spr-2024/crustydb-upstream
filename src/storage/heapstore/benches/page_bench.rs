use criterion::{black_box, Criterion};

use common::testutil::get_random_vec_of_byte_vec;
use heapstore::testutil::bench_page_insert;

pub fn page_benchmark(c: &mut Criterion) {
    let to_insert = get_random_vec_of_byte_vec(40, 80, 100);
    c.bench_function("page insert medium", |b| {
        b.iter(|| bench_page_insert(black_box(&to_insert)))
    });

    let to_insert = get_random_vec_of_byte_vec(10, 350, 400);
    c.bench_function("page insert large recs", |b| {
        b.iter(|| bench_page_insert(black_box(&to_insert)))
    });
}
