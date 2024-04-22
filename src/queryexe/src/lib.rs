#[macro_use]
extern crate log;

pub mod mutator;
pub mod opiterator;
pub mod query;
pub mod stats;
pub mod testutil;

use common::prelude::*;
pub use index::IndexManager;
use index::StorageTrait;
pub use storage::{StorageManager, STORAGE_DIR};
pub use txn_manager::mock_tm::MockTransactionManager as TransactionManager;

/// This is a wrapper for the managers, which are components responsible
/// for various parts of the system (e.g. storage, indices, etc).
/// This is used to pass around the managers easily.
/// A manager may have more than one implementation, such as having different
/// storage manager implementations for different storage backends.
pub struct Managers {
    pub sm: &'static StorageManager,
    pub tm: &'static TransactionManager,
    pub im: &'static IndexManager,
    pub stats: &'static stats::ReservoirStatManager,
}

impl Managers {
    pub fn new(
        sm: &'static StorageManager,
        tm: &'static TransactionManager,
        im: &'static IndexManager,
        stats: &'static stats::ReservoirStatManager,
    ) -> Self {
        Self { sm, tm, im, stats }
    }

    pub fn shutdown(&self) {
        self.sm.shutdown();
        info!("TODO Storage manager shutdown -- add shutdown for other managers");
        //self.tm.shutdown();
    }

    pub fn reset(&self) -> Result<(), CrustyError> {
        info!("TODO Storage manager reset -- add reset for other managers");
        self.sm.reset()
    }
}
