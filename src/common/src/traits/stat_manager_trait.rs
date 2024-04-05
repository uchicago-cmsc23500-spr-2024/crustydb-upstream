use std::path::Path;

use crate::{ast_expr::AstExpr, prelude::*};

pub trait StatManagerTrait {
    fn new(storage_path: &Path, mem_budget: usize) -> Self;

    fn shutdown(&self) -> Result<(), CrustyError>;

    fn deleted_record(&self, value_id: &ValueId) -> Result<(), CrustyError>;

    fn register_container(&self, c_id: ContainerId, schema: TableSchema)
        -> Result<(), CrustyError>;

    fn updated_record(
        &self,
        tuple: &Tuple,
        value_id: &ValueId,
        old_value_id: Option<&ValueId>,
    ) -> Result<(), CrustyError>;

    fn new_record(&self, tuple: &Tuple, value_id: ValueId) -> Result<(), CrustyError>;

    fn estimate_count_and_sel(
        &self,
        c_id: ContainerId,
        predicate: AstExpr,
    ) -> Result<(usize, f64), CrustyError>;

    fn estimate_join_count_and_sel(
        &self,
        left_c_id: ContainerId,
        right_c_id: ContainerId,
        eqs: Vec<(AstExpr, AstExpr)>,
        filter: Option<AstExpr>,
    ) -> Result<(usize, f64), CrustyError>;

    fn get_container_record_count(&self, c_id: ContainerId) -> Result<usize, CrustyError>;
}
