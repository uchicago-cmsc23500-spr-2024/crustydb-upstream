use crate::heap_page::HeapPage;
use crate::heapfile::HeapFile;
use crate::page::Page;
use crate::storage_manager::StorageManager;
use common::ids::TransactionId;
use common::ids::{ContainerId, PageId, SlotId};
use common::storage_trait::StorageTrait;
use common::testutil::*;
use std::sync::Arc;

#[allow(dead_code)]
pub(crate) fn fill_hf_sm(
    sm: &StorageManager,
    container_id: ContainerId,
    num_pages: PageId,
    vals_per_page: PageId,
    min_size: usize,
    max_size: usize,
) {
    let tid = TransactionId::new();
    for i in 0..num_pages {
        let (p, _slots) = get_random_page(i, vals_per_page, min_size, max_size);
        sm.write_page(container_id, &p, tid).unwrap();
    }
    sm.clear_cache();
}

#[allow(dead_code)]
pub(crate) fn fill_hf(
    hf: Arc<HeapFile>,
    num_pages: PageId,
    vals_per_page: PageId,
    min_size: usize,
    max_size: usize,
) {
    for i in 0..num_pages {
        let (p, _slots) = get_random_page(i, vals_per_page, min_size, max_size);
        hf.write_page_to_file(&p).unwrap();
    }
}

pub(crate) fn get_random_page(
    id: PageId,
    vals_per_page: PageId,
    min_size: usize,
    max_size: usize,
) -> (Page, Vec<SlotId>) {
    let to_insert = get_random_vec_of_byte_vec(vals_per_page as usize, min_size, max_size);
    let mut res = Vec::new();
    let mut page = Page::new(id);
    for i in to_insert {
        res.push(page.add_value(&i).unwrap());
    }
    (page, res)
}

pub fn bench_page_insert(vals: &[Vec<u8>]) {
    let mut p = Page::new(0);
    for i in vals {
        p.add_value(i).unwrap();
    }
}

pub fn bench_sm_insert(sm: &StorageManager, to_insert: &[Vec<u8>]) {
    let cid = 1;
    let tid = TransactionId::new();
    for x in to_insert {
        sm.insert_value(cid, x.to_vec(), tid);
    }
}
