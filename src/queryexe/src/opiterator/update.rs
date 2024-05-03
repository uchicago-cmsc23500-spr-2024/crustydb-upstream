use super::OpIterator;
use crate::{StorageManager, TransactionManager};
use common::ids::TupleAssignments;
use common::prelude::*;
use common::storage_trait::StorageTrait;
use common::traits::transaction_manager_trait::TransactionManagerTrait;

/// Sequential scan operator
pub struct Update {
    schema: TableSchema,
    open: bool,
    storage_manager: &'static StorageManager,
    transaction_manager: &'static TransactionManager,
    _container_id: ContainerId,
    tid: TransactionId,
    assignments: TupleAssignments,
    child: Box<dyn OpIterator>,
    count: usize,
}

impl Update {
    /// Constructor for the sequential scan operator.
    ///
    /// # Arguments
    ///
    /// * `table` - Table to scan over.
    /// * `table_alias` - Table alias given by the user.
    /// * `tid` - Transaction used to read the table.
    pub fn new(
        storage_manager: &'static StorageManager,
        transaction_manager: &'static TransactionManager,
        container_id: &ContainerId,
        tid: TransactionId,
        assignments: TupleAssignments,
        child: Box<dyn OpIterator>,
    ) -> Self {
        Self {
            schema: child.get_schema().clone(),
            open: false,
            storage_manager,
            transaction_manager,
            _container_id: *container_id,
            tid,
            assignments,
            child,
            count: 0,
        }
    }
}

impl OpIterator for Update {
    fn configure(&mut self, _will_rewind: bool) {
        unimplemented!()
    }

    fn open(&mut self) -> Result<(), CrustyError> {
        self.open = true;
        self.child.open()
    }

    fn next(&mut self) -> Result<Option<Tuple>, CrustyError> {
        if !self.open {
            panic!("Operator has not been opened")
        }
        let next = self.child.next()?;
        if let Some(mut tuple) = next {
            let id = match tuple.value_id {
                Some(id) => id,
                None => {
                    return Err(CrustyError::CrustyError(
                        "No value id set for record. Cannot update".to_string(),
                    ));
                }
            };

            //TODO determine should check for constaints and maintain indexes

            // Update values
            self.transaction_manager.pre_update_record(
                &mut tuple,
                &id,
                &self.tid,
                &self.assignments,
            )?;
            for (field_idx, new_value) in &self.assignments {
                tuple.set_field(*field_idx, new_value.clone());
            }
            // Persist change
            let res = self
                .storage_manager
                .update_value(tuple.to_bytes(), id, self.tid);
            //Check result
            match res {
                Ok(new_value_id) => {
                    // notify txn manager
                    self.transaction_manager.post_update_record(
                        &mut tuple,
                        &new_value_id,
                        &id,
                        &self.tid,
                        &self.assignments,
                    )?;
                    if new_value_id != id {
                        // The record moved. Update index if not using PK
                        debug!("record moved on update");
                    }
                    // update indexes for values that changed
                    self.count += 1;
                }
                Err(e) => {
                    return Err(e);
                }
            }
            return Ok(Some(tuple));
        }
        Ok(next)
    }

    fn close(&mut self) -> Result<(), CrustyError> {
        self.child.close()?;
        self.open = false;
        Ok(())
    }

    fn rewind(&mut self) -> Result<(), CrustyError> {
        unimplemented!();
    }

    fn get_schema(&self) -> &TableSchema {
        &self.schema
    }
}

#[cfg(test)]
#[allow(unused_must_use)]
mod test {
    //use super::*;
    //use common::ids::TransactionId;
}
