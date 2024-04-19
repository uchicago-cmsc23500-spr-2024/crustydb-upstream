use std::path::Path;

use common::ast_expr::AstExpr;
use common::ids::TupleAssignments;
use common::prelude::*;
use common::traits::transaction_manager_trait::{IsolationLevel, TransactionManagerTrait};

#[derive(Default)]
pub struct MockTransactionManager {}

impl MockTransactionManager {
    pub fn new() -> Self {
        Self {}
    }
}

impl TransactionManagerTrait for MockTransactionManager {
    fn new(_storage_path: &Path) -> Self {
        Self {}
    }

    fn shutdown(&mut self) -> Result<(), CrustyError> {
        Ok(())
    }

    fn set_isolation_level(&self, _lvl: IsolationLevel) -> Result<(), CrustyError> {
        Ok(())
    }

    fn start_transaction(&self, _tid: TransactionId) -> Result<(), CrustyError> {
        Ok(())
    }

    fn read_record(
        &self,
        _tuple: &Tuple,
        _value_id: &ValueId,
        _tid: &TransactionId,
    ) -> Result<(), CrustyError> {
        Ok(())
    }

    fn pre_update_record(
        &self,
        _tuple: &mut Tuple,
        _value_id: &ValueId,
        _tid: &TransactionId,
        _changes: &TupleAssignments,
    ) -> Result<(), CrustyError> {
        Ok(())
    }

    fn post_update_record(
        &self,
        _tuple: &mut Tuple,
        _value_id: &ValueId,
        _old_value_id: &ValueId,
        _tid: &TransactionId,
        _changes: &TupleAssignments,
    ) -> Result<(), CrustyError> {
        Ok(())
    }

    fn pre_insert_record(
        &self,
        _tuple: &mut Tuple,
        _tid: TransactionId,
    ) -> Result<(), CrustyError> {
        Ok(())
    }

    fn post_insert_record(
        &self,
        _tuple: &mut Tuple,
        _value_id: ValueId,
        _tid: TransactionId,
    ) -> Result<(), CrustyError> {
        Ok(())
    }

    fn read_predicate(&self, _predicate: AstExpr, _tid: TransactionId) -> Result<(), CrustyError> {
        Ok(())
    }

    fn validate_txn(&self, _tid: TransactionId) -> Result<(), CrustyError> {
        Ok(())
    }

    fn rollback_txn(&self, _tid: TransactionId) -> Result<(), CrustyError> {
        Ok(())
    }

    fn commit_txn(&self, _tid: TransactionId) -> Result<(), CrustyError> {
        Ok(())
    }
}
