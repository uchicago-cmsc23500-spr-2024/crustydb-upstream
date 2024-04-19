// These tests are for evaluating a buffer pool implementation,
// but is oblivious to the BP implementation. It uses pub(crate) functions
// in the heapstore.storage_manager to evaluate if a BP is working.

#[cfg(test)]
mod test {
    use crate::heap_page::HeapPage;
    use crate::storage_manager::StorageManager;
    use crate::testutil::*;
    use common::ids::{PageId, Permissions, TransactionId, ValueId};
    use common::storage_trait::StorageTrait;
    use common::testutil::*;
    use common::PAGE_SLOTS;
    use std::sync::Arc;
    use std::thread;

    #[test]
    fn test_bp_a_get() {
        let sm = StorageManager::new_test_sm();
        if sm.buffer_pool.is_none() {
            // Skip these tests no BP

            return;
        }
        let hfid = 1;
        sm.create_table(hfid).unwrap();
        let byte_1 = get_random_byte_vec(40);
        let tid = TransactionId::new();
        let val_id = sm.insert_value(hfid, byte_1.clone(), tid);
        sm.clear_cache();

        let _p = sm.get_page(
            val_id.container_id,
            val_id.page_id.unwrap(),
            tid,
            Permissions::ReadOnly,
            false,
        );
        sm.get_page(
            val_id.container_id,
            val_id.page_id.unwrap(),
            tid,
            Permissions::ReadOnly,
            false,
        );
        #[cfg(feature = "profile")]
        {
            let (rc, _wc) = sm.get_hf_read_write_count(hfid);
            assert_eq!(1, rc);
        }
        let byte_check = sm.get_value(val_id, tid, Permissions::ReadOnly).unwrap();

        assert_eq!(byte_check, byte_1);

        sm.clear_cache();
        sm.get_page(
            val_id.container_id,
            val_id.page_id.unwrap(),
            tid,
            Permissions::ReadOnly,
            false,
        );

        #[cfg(feature = "profile")]
        {
            let (rc, _wc) = sm.get_hf_read_write_count(hfid);
            assert_eq!(2, rc);
        }
    }

    #[test]
    fn test_bp_evict() {
        //Create a temp file
        let sm = StorageManager::new_test_sm();
        if sm.buffer_pool.is_none() {
            // Skip these tests no BP

            return;
        }
        let hfid = 1;
        sm.create_table(hfid).unwrap();
        let tid = TransactionId::new();
        let to_fill = PAGE_SLOTS + 1;
        fill_hf_sm(&sm, hfid, to_fill as PageId, 10, 100, 100);
        for i in 0..PAGE_SLOTS {
            let id = ValueId {
                container_id: hfid,
                segment_id: None,
                page_id: Some(i as PageId),
                slot_id: None,
            };
            sm.get_page(
                id.container_id,
                id.page_id.unwrap(),
                tid,
                Permissions::ReadOnly,
                false,
            );
        }

        #[cfg(feature = "profile")]
        {
            let (rc, _wc) = sm.get_hf_read_write_count(hfid);
            assert_eq!(PAGE_SLOTS as u16, rc);
        }
        //re read, make sure no extra reads
        for i in 0..PAGE_SLOTS {
            let id = ValueId {
                container_id: hfid,
                segment_id: None,
                page_id: Some(i as PageId),
                slot_id: None,
            };
            sm.get_page(
                id.container_id,
                id.page_id.unwrap(),
                tid,
                Permissions::ReadOnly,
                false,
            );
        }
        #[cfg(feature = "profile")]
        {
            let (rc, _wc) = sm.get_hf_read_write_count(hfid);
            assert_eq!(PAGE_SLOTS as u16, rc);
        }

        let evict_id = ValueId {
            container_id: hfid,
            segment_id: None,
            page_id: Some(PAGE_SLOTS as PageId),
            slot_id: None,
        };

        sm.get_page(
            evict_id.container_id,
            evict_id.page_id.unwrap(),
            tid,
            Permissions::ReadOnly,
            false,
        );
        #[cfg(feature = "profile")]
        {
            let (rc, _wc) = sm.get_hf_read_write_count(hfid);
            assert_eq!((PAGE_SLOTS + 1) as u16, rc);
        }

        //re read
        sm.get_page(
            evict_id.container_id,
            evict_id.page_id.unwrap(),
            tid,
            Permissions::ReadOnly,
            false,
        );
        #[cfg(feature = "profile")]
        {
            let (rc, _wc) = sm.get_hf_read_write_count(hfid);
            assert_eq!((PAGE_SLOTS + 1) as u16, rc);
        }
    }

    #[test]
    fn test_bp_write() {
        let sm = StorageManager::new_test_sm();
        if sm.buffer_pool.is_none() {
            // Skip these tests no BP

            return;
        }
        let hfid = 1;
        sm.create_table(hfid).unwrap();
        let byte_1 = get_random_byte_vec(40);
        let tid = TransactionId::new();
        let val_id = sm.insert_value(hfid, byte_1, tid);
        sm.clear_cache();

        let mut p = sm
            .get_page(
                val_id.container_id,
                val_id.page_id.unwrap(),
                tid,
                Permissions::ReadOnly,
                false,
            )
            .unwrap();
        sm.get_page(
            val_id.container_id,
            val_id.page_id.unwrap(),
            tid,
            Permissions::ReadOnly,
            false,
        );

        #[cfg(feature = "profile")]
        {
            let (rc, _wc) = sm.get_hf_read_write_count(hfid);
            assert_eq!(1, rc);
        }

        let byte_2 = get_random_byte_vec(40);
        p.add_value(&byte_2);
        let p1_bytes = p.to_bytes();
        sm.write_page(val_id.container_id, &p, tid).unwrap();

        #[cfg(feature = "profile")]
        {
            let (_rc, wc) = sm.get_hf_read_write_count(hfid);
            assert_eq!(2, wc);
        }

        let p2 = sm
            .get_page(
                val_id.container_id,
                val_id.page_id.unwrap(),
                tid,
                Permissions::ReadOnly,
                false,
            )
            .unwrap();
        assert_eq!(p1_bytes[..], p2.to_bytes()[..]);
    }

    #[test]
    fn test_bp_multi() {
        init();
        let sm = StorageManager::new_test_sm();
        if sm.buffer_pool.is_none() {
            // Skip these tests no BP

            return;
        }
        let hfid = 1;
        sm.create_table(hfid).unwrap();
        let byte_1 = get_random_byte_vec(40);
        let tid = TransactionId::new();
        let val_id = sm.insert_value(hfid, byte_1, tid);
        sm.clear_cache();

        let v2 = val_id;

        let s2 = Arc::new(sm);
        let s1 = Arc::clone(&s2);
        let handle = thread::spawn(move || {
            s2.get_page(
                val_id.container_id,
                val_id.page_id.unwrap(),
                tid,
                Permissions::ReadOnly,
                false,
            )
            .unwrap();
        });
        s1.get_page(
            v2.container_id,
            v2.page_id.unwrap(),
            tid,
            Permissions::ReadOnly,
            false,
        )
        .unwrap();

        handle.join().unwrap();
        #[cfg(feature = "profile")]
        {
            let (rc, _wc) = s1.get_hf_read_write_count(hfid);

            assert_eq!(1, rc);
        }
    }
}
