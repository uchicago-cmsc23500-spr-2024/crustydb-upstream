extern crate common;
extern crate heapstore as sm;
extern crate log;
use std::fs;

use common::prelude::*;
use common::storage_trait::StorageTrait;
use common::testutil::*;
use rand::{thread_rng, Rng};
use sm::storage_manager::StorageManager;

const RO: Permissions = Permissions::ReadOnly;

#[test]
fn sm_inserts() {
    let sm = StorageManager::new_test_sm();
    let t = TransactionId::new();
    let num_vals: Vec<usize> = vec![10, 50, 75, 100, 500, 1000];
    for i in num_vals {
        let vals1 = get_random_vec_of_byte_vec(i, 50, 100);
        let cid = i as ContainerId;
        sm.create_table(cid).unwrap();
        sm.insert_values(cid, vals1.clone(), t);
        let check_vals: Vec<Vec<u8>> = sm.get_iterator(cid, t, RO).map(|(a, _)| a).collect();
        assert!(
            compare_unordered_byte_vecs(&vals1, check_vals),
            "Insert of size {} should be equal",
            i
        );
    }
}

#[test]
fn sm_insert_delete() {
    let mut rng = thread_rng();
    let sm = StorageManager::new_test_sm();
    let t = TransactionId::new();
    let mut vals1 = get_random_vec_of_byte_vec(100, 50, 100);
    let cid = 1;
    sm.create_table(cid).unwrap();
    let mut val_ids = sm.insert_values(cid, vals1.clone(), t);
    for _ in 0..10 {
        let idx_to_del = rng.gen_range(0..vals1.len());
        sm.delete_value(val_ids[idx_to_del], t).unwrap();
        let check_vals: Vec<Vec<u8>> = sm.get_iterator(cid, t, RO).map(|(a, _)| a).collect();
        assert!(!compare_unordered_byte_vecs(&vals1, check_vals.clone()));
        vals1.swap_remove(idx_to_del);
        val_ids.swap_remove(idx_to_del);
        assert!(compare_unordered_byte_vecs(&vals1, check_vals));
    }
}

#[test]
#[should_panic]
fn sm_no_container() {
    let sm = StorageManager::new_test_sm();
    let t = TransactionId::new();
    let vals1 = get_random_vec_of_byte_vec(100, 50, 100);
    sm.insert_values(1, vals1, t);
}

#[test]
fn sm_test_shutdown() {
    let path = gen_random_test_sm_dir();
    let sm = StorageManager::new(&path.clone());
    let t = TransactionId::new();

    let vals1 = get_random_vec_of_byte_vec(100, 50, 100);
    let cid = 1;
    sm.create_table(cid).unwrap();
    let _val_ids = sm.insert_values(cid, vals1.clone(), t);
    sm.shutdown();

    let sm2 = StorageManager::new(&path.clone());
    let check_vals: Vec<Vec<u8>> = sm2.get_iterator(cid, t, RO).map(|(a, _)| a).collect();
    assert!(compare_unordered_byte_vecs(&vals1, check_vals));
    sm2.reset().unwrap();
    fs::remove_dir_all(path).unwrap();
}
