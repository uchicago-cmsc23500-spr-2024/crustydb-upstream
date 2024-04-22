use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::fs;
use std::hash::{Hash, Hasher};
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};

use crate::{StorageManager, StorageTrait};
use common::catalog::{Catalog, CatalogRef};
use common::ids::{AtomicTimeStamp, StateMeta};
use common::physical_plan::PhysicalPlan;
use common::prelude::*;
use common::table::TableInfo;
use common::traits::stat_manager_trait::StatManagerTrait;
use common::{Attribute, QueryResult};
use queryexe::query::get_attr;
use queryexe::Managers;
use sqlparser::ast::ColumnDef;
use sqlparser::ast::TableConstraint;

use crate::query_registrar::QueryRegistrar;
use crate::sql_parser::{ParserResponse, SQLParser};

use std::sync::atomic::AtomicU32;

#[derive(Serialize)]
pub struct DatabaseState {
    pub id: u64,
    pub name: String,

    pub catalog: CatalogRef,

    #[serde(skip_serializing)]
    pub managers: &'static Managers,
    // XTX maybe add the string vec here
    // The list of things stored
    container_vec: Arc<RwLock<HashMap<ContainerId, StateMeta>>>,

    // Time for operations based on timing (typically inserts)
    pub atomic_time: AtomicTimeStamp,

    #[serde(skip_serializing)]
    query_registrar: QueryRegistrar,
}

#[allow(dead_code)]
impl DatabaseState {
    pub fn get_database_id(db_name: &str) -> u64 {
        let mut s = DefaultHasher::new();
        db_name.hash(&mut s);
        s.finish()
    }

    pub fn create_db(
        base_dir: &Path,
        db_name: &str,
        managers: &'static Managers,
    ) -> Result<Self, CrustyError> {
        let db_path = base_dir.join(db_name);
        if db_path.exists() {
            DatabaseState::load(db_path, managers)
        } else {
            DatabaseState::new_from_name(db_name, managers)
        }
    }

    pub fn new_from_name(db_name: &str, managers: &'static Managers) -> Result<Self, CrustyError> {
        let db_name: String = String::from(db_name);
        let db_id = DatabaseState::get_database_id(&db_name);
        debug!(
            "Creating new DatabaseState; name: {} id: {}",
            db_name, db_id
        );
        let db_state = DatabaseState {
            id: db_id,
            name: db_name,
            catalog: Catalog::new(),
            managers,
            container_vec: Arc::new(RwLock::new(HashMap::new())),
            atomic_time: AtomicU32::new(0),
            query_registrar: QueryRegistrar::new(),
        };
        Ok(db_state)
    }

    pub fn load(_filename: PathBuf, _managers: &'static Managers) -> Result<Self, CrustyError> {
        unimplemented!("Load the database from sm")
    }

    pub fn get_current_time(&self) -> LogicalTimeStamp {
        self.atomic_time.load(std::sync::atomic::Ordering::SeqCst)
    }

    pub fn get_table_names(&self) -> Result<Vec<String>, CrustyError> {
        let tables = self.catalog.get_table_names();
        Ok(tables)
    }

    pub fn get_registered_query_names(&self) -> Result<String, CrustyError> {
        self.query_registrar.get_registered_query_names()
    }

    /// Load in database.
    ///
    /// # Arguments
    ///
    /// * `db` - Name of database to load in.
    /// * `id` - Thread id to get the lock.
    pub fn load_database_from_file(
        _file: fs::File,
        _storage_manager: &StorageManager,
    ) -> Result<CatalogRef, CrustyError> {
        unimplemented!("Reconstruct the catalog from sm")
    }

    /// Creates a new table.
    ///
    /// # Arguments
    ///
    /// * `name` - Name of the new table.
    /// * `cols` - Table columns.
    pub fn create_table(
        &self,
        table_name: &str,
        columns: &[ColumnDef],
        constraints: &[TableConstraint],
    ) -> Result<QueryResult, CrustyError> {
        // Constraints aren't implemented yet

        let table_id = self.catalog.get_table_id(table_name);
        let pks = match SQLParser::get_pks(columns, constraints) {
            Ok(pks) => pks,
            Err(ParserResponse::SQLConstraintError(s)) => return Err(CrustyError::CrustyError(s)),
            _ => unreachable!(),
        };

        let mut attributes: Vec<Attribute> = Vec::new();
        for col in columns {
            let constraint = if pks.contains(&col.name) {
                common::Constraint::PrimaryKey
            } else {
                common::Constraint::None
            };
            let attr = Attribute {
                name: format!("{}.{}", table_name, col.name.value.clone()),
                dtype: get_attr(&col.data_type)?,
                constraint,
            };
            attributes.push(attr);
        }
        let schema = TableSchema::new(attributes);
        debug!("Creating table with schema: {:?}", schema);

        let table_info = TableInfo::new(table_id, table_name.to_string(), schema.clone());
        self.managers.sm.create_container(
            table_id,
            Some(table_name.to_string()),
            common::ids::StateType::BaseTable,
            None,
        )?;
        let res = self.catalog.add_table(table_info);
        if res.is_none() {
            // TODO: This check should be done in the sm.
            return Err(CrustyError::CrustyError(format!(
                "Table {} already exists",
                table_name
            )));
        }
        self.managers.stats.register_container(table_id, schema)?;

        let qr = QueryResult::MessageOnly(format!("Table {} created", table_name));

        Ok(qr)
    }

    pub fn reset(&self) -> Result<(), CrustyError> {
        self.query_registrar.reset()?;
        let mut containers = self.container_vec.write().unwrap();
        containers.clear();
        drop(containers);
        Ok(())
    }

    /// Register a new query.
    ///
    /// # Arguments
    ///
    /// * `query_name` - Query name to register.
    /// * `query_plan` - Query plan to register.
    pub fn register_query(
        &self,
        query_name: String,
        json_path: String,
        query_plan: Arc<PhysicalPlan>,
    ) -> Result<(), CrustyError> {
        self.query_registrar
            .register_query(query_name, json_path, query_plan)
    }

    /// Update metadata for beginning to run a registered query.
    ///
    /// # Arguments
    ///
    /// * `query_name` - Name of the query.
    /// * `start_timestamp` - Optional start timestamp.
    /// * `end_timestamp` - End timestamp.
    pub fn begin_query(
        &self,
        query_name: &str,
        start_timestamp: Option<LogicalTimeStamp>,
        end_timestamp: LogicalTimeStamp,
    ) -> Result<Arc<PhysicalPlan>, CrustyError> {
        self.query_registrar
            .begin_query(query_name, start_timestamp, end_timestamp)
    }

    /// Update metadata at end of a query.
    ///
    /// # Arguments
    ///
    /// * `query_name` - Name of the query.
    pub fn finish_query(&self, query_name: &str) -> Result<(), CrustyError> {
        self.query_registrar.finish_query(query_name)
    }
}
