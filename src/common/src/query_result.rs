use crate::CrustyError;
use crate::TableSchema;
use crate::Tuple;

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize, Clone)]
pub struct PagingInfo {
    pub current_page: u32,
    pub total_pages: u32,
    pub has_next_page: bool,
    pub page_size: u32,
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize, Clone)]
pub enum QueryResult {
    MessageOnly(String),
    Select {
        schema: TableSchema,
        result: Vec<Tuple>,
        paging_info: Option<PagingInfo>,
    },
    Insert {
        inserted: usize,
        table_name: String,
    },
}

impl QueryResult {
    pub fn new_select_result(
        schema: &TableSchema,
        result: Vec<Tuple>,
        paging_info: Option<PagingInfo>,
    ) -> Self {
        QueryResult::Select {
            schema: schema.clone(),
            result,
            paging_info, // Set the passed paging info
        }
    }

    pub fn new_message_only_result(message: String) -> Self {
        QueryResult::MessageOnly(message)
    }

    pub fn new_insert_result(inserted: usize, table_name: String) -> Self {
        QueryResult::Insert {
            inserted,
            table_name,
        }
    }

    pub fn get_tuples(&self) -> Option<&Vec<Tuple>> {
        match self {
            QueryResult::Select { result, .. } => Some(result),
            _ => None,
        }
    }

    pub fn get_schema(&self) -> Option<&TableSchema> {
        match self {
            QueryResult::Select { schema, .. } => Some(schema),
            _ => None,
        }
    }

    /// Merge results
    pub fn merge_results(&mut self, other: QueryResult) -> Result<(), CrustyError> {
        match (self, other) {
            (
                QueryResult::Select {
                    schema: schema1,
                    result: result1,
                    paging_info: _,
                },
                QueryResult::Select {
                    schema: schema2,
                    result: mut result2,
                    paging_info: _,
                },
            ) => {
                if *schema1 != schema2 {
                    return Err(CrustyError::CrustyError(
                        "Schema mismatch when merging results.".to_string(),
                    ));
                }
                result1.append(&mut result2);
                Ok(())
            }
            _ => Err(CrustyError::CrustyError(
                "Cannot merge non-select results.".to_string(),
            )),
        }
    }
}
