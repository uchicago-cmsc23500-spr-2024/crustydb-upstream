use crate::ids::ContainerId;
use crate::TableSchema;

/// Table implementation.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct TableInfo {
    pub c_id: ContainerId,
    /// Table name.
    pub name: String,
    /// Table schema.
    pub schema: TableSchema,
}

impl TableInfo {
    pub fn new(c_id: ContainerId, name: String, schema: TableSchema) -> Self {
        TableInfo { c_id, name, schema }
    }
}
