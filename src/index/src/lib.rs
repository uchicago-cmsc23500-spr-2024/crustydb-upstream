/// Re-export Storage manager here for this crate to use. This allows us to change
/// the storage manager by changing one use statement.
pub use common::storage_trait::StorageTrait;
pub use storage::StorageManager;
pub use txn_manager::mock_tm::MockTransactionManager as TransactionManager;

pub use index_manager::IndexManager;
pub use tree::TreeIndex;

mod index_manager;
mod tree;
