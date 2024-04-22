use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use common::ids::LogicalTimeStamp;
use common::physical_plan::PhysicalPlan;
use common::CrustyError;

pub struct QueryRegistrar {
    query_plans: Arc<RwLock<HashMap<String, Arc<PhysicalPlan>>>>,
    query_filenames: Arc<RwLock<HashMap<String, String>>>,
    query_watermarks: Arc<RwLock<HashMap<String, LogicalTimeStamp>>>,
    in_progress_queries: Arc<RwLock<HashMap<String, LogicalTimeStamp>>>,
}

impl QueryRegistrar {
    pub fn new() -> Self {
        QueryRegistrar {
            query_plans: Arc::new(RwLock::new(HashMap::new())),
            query_filenames: Arc::new(RwLock::new(HashMap::new())),
            query_watermarks: Arc::new(RwLock::new(HashMap::new())),
            in_progress_queries: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub fn reset(&self) -> Result<(), CrustyError> {
        let mut in_prog = self.in_progress_queries.write().unwrap();
        if !in_prog.is_empty() {
            Err(CrustyError::CrustyError(String::from(
                "Queries are in progress cannot drop/reset",
            )))
        } else {
            let mut plans = self.query_plans.write().unwrap();
            let mut files = self.query_filenames.write().unwrap();
            let mut watermarks = self.query_watermarks.write().unwrap();
            plans.clear();
            drop(plans);
            files.clear();
            drop(files);
            watermarks.clear();
            drop(watermarks);
            in_prog.clear();
            drop(in_prog);
            Ok(())
        }
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
        self.query_plans
            .write()
            .unwrap()
            .insert(query_name.clone(), query_plan);
        self.query_filenames
            .write()
            .unwrap()
            .insert(query_name.clone(), json_path);
        self.query_watermarks.write().unwrap().insert(query_name, 0);
        Ok(())
    }

    /// Begin running a registered query.
    ///
    /// # Arguments
    ///
    /// * `query_name` - Query to run.
    /// * `start_timestamp` - Starting timestamp.
    /// * `end_timestamp` - New query watermark if completed.
    pub fn begin_query(
        &self,
        query_name: &str,
        start_timestamp: Option<LogicalTimeStamp>,
        end_timestamp: LogicalTimeStamp,
    ) -> Result<Arc<PhysicalPlan>, CrustyError> {
        assert!(start_timestamp.unwrap_or(0) <= end_timestamp);
        if self
            .in_progress_queries
            .read()
            .unwrap()
            .contains_key(query_name)
        {
            return Err(CrustyError::CrustyError(format!(
                "Query \"{}\" already in progress.",
                query_name
            )));
        }

        match self.query_plans.read().unwrap().get(query_name) {
            Some(physical_plan) => {
                self.in_progress_queries
                    .write()
                    .unwrap()
                    .insert(query_name.to_string(), end_timestamp);
                Ok(Arc::clone(physical_plan))
            }
            None => Err(CrustyError::CrustyError(format!(
                "Query \"{}\" has not been registered.",
                query_name
            ))),
        }
    }

    /// Finish running a registered query.
    ///
    /// # Arguments
    ///
    /// * `query_name` - Query finished.
    pub fn finish_query(&self, query_name: &str) -> Result<(), CrustyError> {
        if !self
            .in_progress_queries
            .read()
            .unwrap()
            .contains_key(query_name)
        {
            Err(CrustyError::CrustyError(format!(
                "Query \"{}\" is not in progress.",
                query_name
            )))
        } else {
            match self
                .in_progress_queries
                .write()
                .unwrap()
                .remove_entry(query_name)
            {
                Some((_, new_watermark)) => {
                    self.query_watermarks
                        .write()
                        .unwrap()
                        .insert(query_name.to_string(), new_watermark);
                    Ok(())
                }
                None => unreachable!(),
            }
        }
    }

    pub fn get_registered_query_names(&self) -> Result<String, CrustyError> {
        let mut registered_query_names_and_paths = Vec::new();
        for (query_name, json_path) in self.query_filenames.read().unwrap().iter() {
            registered_query_names_and_paths
                .push(format!("{} loaded from {}", query_name, json_path));
        }
        let registered_query_names_and_paths = registered_query_names_and_paths.join("\n");
        if registered_query_names_and_paths.is_empty() {
            Ok(String::from("No registered queries"))
        } else {
            Ok(registered_query_names_and_paths)
        }
    }
}
