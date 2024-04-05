use crate::heap_page::HeapPage;
use crate::page::Page;
use common::ids::TransactionId;
use common::ids::{ContainerId, PageId, SlotId};
use common::storage_trait::StorageTrait;
use common::testutil::*;
use std::sync::Arc;

#[allow(dead_code)]
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

#[allow(dead_code)]
pub fn bench_page_insert(vals: &[Vec<u8>]) {
    let mut p = Page::new(0);
    for i in vals {
        p.add_value(i).unwrap();
    }
}
