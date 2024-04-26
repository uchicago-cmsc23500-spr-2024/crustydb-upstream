use criterion::{criterion_group, criterion_main};

mod page_bench;
mod sm_bench;

criterion_group!(benches, page_bench::page_benchmark, sm_bench::sm_ins_bench);
criterion_main!(benches);
