use criterion::{black_box, Criterion};
//<strip only="pg">
use common::storage_trait::StorageTrait;
use common::testutil::get_random_vec_of_byte_vec;
use heapstore::storage_manager::StorageManager;
use heapstore::testutil::bench_sm_insert;
//</strip>

pub fn sm_ins_bench(c: &mut Criterion) {
    let to_insert = get_random_vec_of_byte_vec(1000, 80, 100);
    //<strip only="pg">
    let sm = StorageManager::new_test_sm();
    let cid = 1;
    sm.create_table(cid).unwrap();
    c.bench_function("sm insert 1k", |b| {
        b.iter(|| bench_sm_insert(&sm, black_box(&to_insert)))
    });
    //</strip>
}
