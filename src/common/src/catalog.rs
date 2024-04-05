use crate::ids::ContainerId;
use crate::table::TableInfo;
use crate::TableSchema;
use serde::Serialize;
use std::collections::HashMap;
use std::sync::{Arc, Mutex, RwLock};

#[derive(Serialize)]
struct ContainerIdGenerator {
    next_id: ContainerId,
    table_to_id: HashMap<String, ContainerId>,
}

impl ContainerIdGenerator {
    fn new() -> Self {
        ContainerIdGenerator {
            next_id: 0,
            table_to_id: HashMap::new(),
        }
    }

    /// Note that ContainerId is not unique across multiple databases.
    fn get_table_id(&mut self, table_name: &str) -> ContainerId {
        match self.table_to_id.get(table_name) {
            Some(c_id) => *c_id,
            None => {
                let c_id = self.next_id;
                self.next_id += 1;
                self.table_to_id.insert(table_name.to_string(), c_id);
                c_id
            }
        }
    }
}

#[derive(Serialize)]
pub struct Catalog {
    container_id_generator: Mutex<ContainerIdGenerator>,
    tables: RwLock<HashMap<ContainerId, TableInfo>>,
}

impl Catalog {
    pub fn new() -> CatalogRef {
        Arc::new(Catalog {
            container_id_generator: Mutex::new(ContainerIdGenerator::new()),
            tables: RwLock::new(HashMap::new()),
        })
    }

    pub fn get_table_id(&self, name: &str) -> ContainerId {
        let mut generator = self.container_id_generator.lock().unwrap();
        generator.get_table_id(name)
    }

    pub fn add_table(&self, table_info: TableInfo) -> Option<()> {
        let mut tables = self.tables.write().unwrap();
        match tables.get(&table_info.c_id) {
            Some(_) => None,
            None => {
                tables.insert(table_info.c_id, table_info);
                Some(())
            }
        }
    }

    pub fn get_table(&self, c_id: ContainerId) -> Option<TableInfo> {
        let tables = self.tables.read().unwrap();
        tables.get(&c_id).cloned()
    }

    pub fn get_table_names(&self) -> Vec<String> {
        let tables = self.tables.read().unwrap();
        tables.values().map(|info| info.name.clone()).collect()
    }

    pub fn get_table_schema(&self, c_id: ContainerId) -> Option<TableSchema> {
        let tables = self.tables.read().unwrap();
        tables.get(&c_id).map(|info| info.schema.clone())
    }

    pub fn is_valid_table(&self, c_id: ContainerId) -> bool {
        let tables = self.tables.read().unwrap();
        tables.contains_key(&c_id)
    }

    pub fn is_valid_column(&self, c_id: ContainerId, column_name: &str) -> bool {
        let tables = self.tables.read().unwrap();
        if let Some(table_info) = tables.get(&c_id) {
            table_info.schema.contains(column_name)
        } else {
            false
        }
    }
}

pub type CatalogRef = Arc<Catalog>;
