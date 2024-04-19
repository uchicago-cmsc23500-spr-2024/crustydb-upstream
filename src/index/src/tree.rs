use crate::StorageManager;
use crate::TransactionManager;
use common::Attribute;

#[allow(dead_code)] //TODO: remove this
pub struct TreeIndex {
    is_primary: bool,
    is_unique: bool,
    name: String,
    attributes: Vec<Attribute>,
    sm: &'static StorageManager,
    tm: &'static TransactionManager,
}

impl TreeIndex {
    pub fn new(
        is_primary: bool,
        is_unique: bool,
        name: String,
        attributes: Vec<Attribute>,
        sm: &'static StorageManager,
        tm: &'static TransactionManager,
    ) -> Self {
        Self {
            is_primary,
            is_unique,
            name,
            attributes,
            sm,
            tm,
        }
    }
}
