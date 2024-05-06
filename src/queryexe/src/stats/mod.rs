use std::collections::hash_map::Entry::{Occupied, Vacant};
use std::sync::Mutex;
use std::{collections::HashMap, path::PathBuf, sync::RwLock};

use common::ast_expr::{bind_expr, AstExpr};
use common::{ids::ContainerId, traits::stat_manager_trait::StatManagerTrait, Tuple};
use common::{prelude::*, BooleanOp, MathOp};
use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};

const SAMPLE_SIZE: usize = 1000;

struct ContainerSamples {
    samples: Vec<Tuple>,
    record_count: usize,
    schema: TableSchema,
    // A key map should be added, but this needs a catalog
}

pub struct ReservoirStatManager {
    storage_path: PathBuf,
    _mem_budget_mb: usize,
    samples: RwLock<HashMap<ContainerId, ContainerSamples>>,
    rng: Mutex<rand::rngs::SmallRng>,
}

impl StatManagerTrait for ReservoirStatManager {
    fn new(storage_path: &std::path::Path, mem_budget: usize) -> Self {
        ReservoirStatManager {
            storage_path: storage_path.to_path_buf(),
            _mem_budget_mb: mem_budget,
            samples: RwLock::new(HashMap::new()),
            rng: Mutex::new(SmallRng::from_entropy()),
        }
    }

    fn shutdown(&self) -> Result<(), CrustyError> {
        todo!("Write to disk and shutdown. {:?}", self.storage_path);
    }

    fn register_container(
        &self,
        c_id: ContainerId,
        schema: TableSchema,
    ) -> Result<(), CrustyError> {
        let mut samples = self.samples.write().unwrap();
        match samples.entry(c_id) {
            Vacant(e) => {
                e.insert(ContainerSamples {
                    samples: Vec::new(),
                    record_count: 0,
                    schema,
                });
            }
            Occupied(_) => {}
        }
        Ok(())
    }

    fn deleted_record(&self, _value_id: &ValueId) -> Result<(), CrustyError> {
        todo!()
    }

    fn updated_record(
        &self,
        _tuple: &Tuple,
        _value_id: &ValueId,
        _old_value_id: Option<&ValueId>,
    ) -> Result<(), CrustyError> {
        todo!()
    }

    fn new_record(&self, tuple: &Tuple, value_id: ValueId) -> Result<(), CrustyError> {
        let mut samples = self.samples.write().unwrap();
        if !samples.contains_key(&value_id.container_id) {
            return Err(CrustyError::CrustyError(
                "Container not found/registered".to_string(),
            ));
        }

        //TODO(ae) not storing the value_id so cannot be updated.
        let container_samples = samples.get_mut(&value_id.container_id).unwrap();
        if container_samples.samples.len() < SAMPLE_SIZE {
            container_samples.samples.push(tuple.clone());
        } else {
            let r = self
                .rng
                .lock()
                .unwrap()
                .gen_range(0..container_samples.record_count);
            if r < SAMPLE_SIZE {
                container_samples.samples[r] = tuple.clone();
            }
        }
        container_samples.record_count += 1;
        Ok(())
    }

    fn get_container_record_count(&self, c_id: ContainerId) -> Result<usize, CrustyError> {
        let samples = self.samples.read().unwrap();
        let container_samples = samples.get(&c_id).unwrap();
        Ok(container_samples.record_count)
    }

    /// Given a container and a predicate, estimate the number of records that satisfy the predicate.
    /// This is done by evaluating the predicate on the sample and then scaling the result by the
    /// total number of records in the container.
    fn estimate_count_and_sel(
        &self,
        c_id: ContainerId,
        predicate: AstExpr,
    ) -> Result<(usize, f64), CrustyError> {
        let samples = self.samples.read().unwrap();
        let container_samples = samples
            .get(&c_id)
            .ok_or(CrustyError::CrustyError("Container not found".to_string()))?;

        if container_samples.samples.is_empty() {
            return Ok((0, 0.0));
        }

        let schema = self.get_container_schema(&c_id)?;
        let bound_predicate = bind_expr(predicate, &schema)?;

        let mut matching_count = 0;
        for tuple in &container_samples.samples {
            let eval_result = self.eval_astexpr(&bound_predicate, tuple)?;
            if let Field::Bool(true) = eval_result {
                matching_count += 1;
            }
        }

        let total_records = container_samples.record_count;
        let sample_size = container_samples.samples.len();

        let selectivity = matching_count as f64 / sample_size as f64;
        let estimated_count = selectivity * total_records as f64;
        Ok((estimated_count.round() as usize, selectivity))
    }

    fn estimate_join_count_and_sel(
        &self,
        left_c_id: ContainerId,
        right_c_id: ContainerId,
        eqs: Vec<(AstExpr, AstExpr)>,
        filter: Option<AstExpr>,
    ) -> Result<(usize, f64), CrustyError> {
        let samples = self.samples.read().unwrap();
        let left_container_samples = samples.get(&left_c_id).ok_or(CrustyError::CrustyError(
            "Container 1 not found".to_string(),
        ))?;
        let right_container_samples = samples.get(&right_c_id).ok_or(CrustyError::CrustyError(
            "Container 2 not found".to_string(),
        ))?;

        if left_container_samples.samples.is_empty() || right_container_samples.samples.is_empty() {
            return Ok((0, 0.0));
        }

        let left_schema = self.get_container_schema(&left_c_id)?;
        let right_schema = self.get_container_schema(&right_c_id)?;
        let mut matching_count = 0;
        let mut total_pairs = 0; // left_container_samples.samples.len() * right_container_samples.samples.len();

        for left_tuple in &left_container_samples.samples {
            for right_tuple in &right_container_samples.samples {
                let merged_tuple = left_tuple.merge(right_tuple);

                if eqs.iter().all(|(left_expr, right_expr)| {
                    let bound_left = bind_expr(left_expr.clone(), &left_schema).unwrap();
                    let bound_right = bind_expr(right_expr.clone(), &right_schema).unwrap();
                    let left_result = self.eval_astexpr(&bound_left, left_tuple).unwrap();
                    let right_result = self.eval_astexpr(&bound_right, right_tuple).unwrap();
                    left_result == right_result
                }) {
                    if let Some(filter_expr) = &filter {
                        let merged_schema = left_schema.merge(&right_schema);
                        let bound_filter = bind_expr(filter_expr.clone(), &merged_schema)?;
                        if let Field::Bool(true) =
                            self.eval_astexpr(&bound_filter, &merged_tuple)?
                        {
                            matching_count += 1;
                        }
                    } else {
                        matching_count += 1;
                    }
                }
                total_pairs += 1;
            }
        }

        let total_records =
            left_container_samples.record_count * right_container_samples.record_count;

        let selectivity = matching_count as f64 / total_pairs as f64;
        let estimated_count = selectivity * total_records as f64;
        Ok((estimated_count.round() as usize, selectivity))
    }
}

impl ReservoirStatManager {
    fn get_container_schema(&self, c_id: &ContainerId) -> Result<TableSchema, CrustyError> {
        let samples = self.samples.read().unwrap();
        let container_samples = samples.get(c_id).unwrap();
        Ok(container_samples.schema.clone())
    }

    #[allow(clippy::only_used_in_recursion)]
    fn eval_astexpr(&self, expr: &AstExpr, tuple: &Tuple) -> Result<Field, CrustyError> {
        match expr {
            AstExpr::Literal(field) => Ok(field.clone()),
            AstExpr::Ident(_) => Err(CrustyError::CrustyError(
                "Unexpected Ident in AST evaluation".to_string(),
            )),
            AstExpr::Alias(_, expr) => self.eval_astexpr(expr, tuple),
            AstExpr::Math(op, left, right) => {
                let left_val = self.eval_astexpr(left, tuple)?;
                let right_val = self.eval_astexpr(right, tuple)?;
                match op {
                    MathOp::Add => left_val + right_val,
                    MathOp::Sub => left_val - right_val,
                    MathOp::Mul => left_val * right_val,
                    MathOp::Div => left_val / right_val,
                }
            }
            AstExpr::Boolean(op, left, right) => {
                let left_val = self.eval_astexpr(left, tuple)?;
                let right_val = self.eval_astexpr(right, tuple)?;
                match op {
                    BooleanOp::Eq => Ok(Field::Bool(left_val == right_val)),
                    BooleanOp::Neq => Ok(Field::Bool(left_val != right_val)),
                    BooleanOp::Gt => Ok(Field::Bool(left_val > right_val)),
                    BooleanOp::Gte => Ok(Field::Bool(left_val >= right_val)),
                    BooleanOp::Lt => Ok(Field::Bool(left_val < right_val)),
                    BooleanOp::Lte => Ok(Field::Bool(left_val <= right_val)),
                    BooleanOp::And => match (left_val, right_val) {
                        (Field::Bool(a), Field::Bool(b)) => Ok(Field::Bool(a && b)),
                        _ => Err(CrustyError::CrustyError(
                            "Non-bool operands for And operation".to_string(),
                        )),
                    },
                    BooleanOp::Or => match (left_val, right_val) {
                        (Field::Bool(a), Field::Bool(b)) => Ok(Field::Bool(a || b)),
                        _ => Err(CrustyError::CrustyError(
                            "Non-bool operands for Or operation".to_string(),
                        )),
                    },
                }
            }
            AstExpr::Agg(_, _) => Err(CrustyError::CrustyError(
                "Unexpected Agg in AST evaluation".to_string(),
            )),
            AstExpr::ColIdx(idx) => tuple
                .get_field(*idx)
                .cloned()
                .ok_or(CrustyError::CrustyError(
                    "Column index out of bounds".to_string(),
                )),
        }
    }
}

/*
#[cfg(test)]
mod tests {
    use super::*;
    use common::ids::ValueId;
    use common::testutil::*;
    use common::traits::stat_manager_trait::StatManagerTrait;
    use common::Tuple;

    fn gen_test_stat_manager() -> ReservoirStatManager {
        let storage_path = gen_random_test_sm_dir();
        let mem_budget = 1000;
        ReservoirStatManager::new(&storage_path, mem_budget)
    }

    #[test]
    fn test_reservoir_stat_manager() {
        let stat_manager = gen_test_stat_manager();
        let value_id = ValueId::new(1);
        let tuple = Tuple::new(vec![]);
        let schema = TableSchema::new(vec![]);
        stat_manager.register_container(1, schema).unwrap();
        let result = stat_manager.new_record(&tuple, value_id);
        assert_eq!(result, Ok(()));

        let err_expected = stat_manager.new_record(&tuple, ValueId::new(2));
        assert_eq!(
            err_expected,
            Err(CrustyError::CrustyError(
                "Container not found/registered".to_string()
            ))
        );
    }

    #[test]
    fn test_small_single_container() {
        let stat_manager = gen_test_stat_manager();
        let c_id = 1;
        let value_id = ValueId::new(c_id);
        let tuple_count = 1000;
        let (table, mut tuples) = gen_test_table_and_tuples(c_id, tuple_count);
        let mut count = 0;
        stat_manager.register_container(c_id, table.schema).unwrap();
        // Check that the first 1000 records fill the reservoir
        for tuple in &tuples {
            let result = stat_manager.new_record(tuple, value_id);
            count += 1;
            assert_eq!(result, Ok(()));
            assert_eq!(stat_manager.get_container_record_count(c_id), Ok(count));
        }
        {
            let samples = stat_manager.samples.read().unwrap();
            let container_samples = samples.get(&value_id.container_id).unwrap();
            assert_eq!(container_samples.samples.len(), SAMPLE_SIZE);
        }

        // Add new records and check size
        tuples = gen_test_tuples(tuple_count);
        for tuple in &tuples {
            let result = stat_manager.new_record(tuple, value_id);
            count += 1;
            assert_eq!(result, Ok(()));
            assert_eq!(stat_manager.get_container_record_count(c_id), Ok(count));
        }
        {
            let samples = stat_manager.samples.read().unwrap();
            let container_samples = samples.get(&value_id.container_id).unwrap();
            assert_eq!(container_samples.samples.len(), SAMPLE_SIZE);
        }
    }

    #[test]
    fn test_estimated_record_count_simple() {
        let stat_manager = gen_test_stat_manager();
        let c_id = 1;
        let tuple_count = 1;
        let (table, tuples) = gen_test_table_and_tuples(c_id, tuple_count);
        stat_manager.register_container(c_id, table.schema).unwrap();
        let tuple = tuples.get(0).unwrap();
        let f1 = tuple.get_field(0).unwrap();
        let predicate = AstExpr::Boolean(
            BooleanOp::Gt,
            Box::new(AstExpr::Ident("ia1".to_string())),
            Box::new(AstExpr::Literal(f1.to_owned())),
        );
        let estimated_count_res = stat_manager.estimate_count_and_sel(2, predicate.clone());
        assert_eq!(
            estimated_count_res,
            Err(CrustyError::CrustyError("Container not found".to_string()))
        );

        let estimated_count_res = stat_manager.estimate_count_and_sel(c_id, predicate.clone());
        assert_eq!(estimated_count_res, Ok((0, 0.0)));
        stat_manager.new_record(tuple, ValueId::new(c_id)).unwrap();
        let (estimated_count, _) = stat_manager
            .estimate_count_and_sel(c_id, predicate)
            .unwrap();
        assert_eq!(estimated_count == 0 || estimated_count == 1, true);
    }

    #[test]
    fn test_estimated_record_count_predicate() {
        let stat_manager = gen_test_stat_manager();
        let c_id = 1;
        let value_id = ValueId::new(c_id);
        let tuple_count = 10000;
        let (table, tuples) = gen_test_table_and_tuples(c_id, tuple_count);
        stat_manager.register_container(c_id, table.schema).unwrap();

        // ia1 should be uniformly distributed 0-9
        let mut predicate = AstExpr::Boolean(
            BooleanOp::Eq,
            Box::new(AstExpr::Ident("ia1".to_string())),
            Box::new(AstExpr::Literal(Field::Int(1))),
        );
        for tuple in &tuples {
            stat_manager.new_record(tuple, value_id).unwrap();
        }
        let (mut estimated_count, mut est_sel) = stat_manager
            .estimate_count_and_sel(c_id, predicate)
            .unwrap();
        info!("Estimated count: {} Est Sel: {}", estimated_count, est_sel);
        // Being rough here, but should be around 1000
        assert_eq!(estimated_count >= 700 && estimated_count <= 1300, true);
        assert_eq!(est_sel >= 0.07 && est_sel <= 0.13, true);

        predicate = AstExpr::Boolean(
            BooleanOp::Gt,
            Box::new(AstExpr::Ident("ia1".to_string())),
            Box::new(AstExpr::Literal(Field::Int(2))),
        );

        // Being rough here, but should be around 7000
        (estimated_count, est_sel) = stat_manager
            .estimate_count_and_sel(c_id, predicate)
            .unwrap();
        info!(
            "Estimated count: {} estimate_sel {}",
            estimated_count, est_sel
        );
        assert_eq!(estimated_count >= 6500 && estimated_count <= 7500, true);
        assert_eq!(est_sel >= 0.65 && est_sel <= 0.75, true);
    }

    #[test]
    fn test_estimated_record_count() {
        let stat_manager = gen_test_stat_manager();
        let c_id = 1;
        let value_id = ValueId::new(c_id);
        let tuple_count = 1000;
        let (table, tuples) = gen_test_table_and_tuples(c_id, tuple_count);
        stat_manager.register_container(c_id, table.schema).unwrap();

        for tuple in &tuples {
            stat_manager.new_record(tuple, value_id).unwrap();
        }

        let predicate = AstExpr::Boolean(
            BooleanOp::Gt,
            Box::new(AstExpr::Ident("ia1".to_string())),
            Box::new(AstExpr::Literal(Field::Int(1))),
        );

        let (estimated_count, _) = stat_manager
            .estimate_count_and_sel(c_id, predicate)
            .unwrap();
        assert!(estimated_count <= tuple_count.try_into().unwrap());
    }

    #[test]
    fn test_estimate_join_selectivity() {
        let stat_manager = gen_test_stat_manager();

        let left_c_id = 1;
        let left_tuple_count = 10000;

        let right_c_id = 2;
        let right_tuple_count = 10000;

        let (left_table, left_tuples) = gen_test_table_and_tuples(left_c_id, left_tuple_count);
        stat_manager
            .register_container(left_c_id, left_table.schema.clone())
            .unwrap();
        for tuple in &left_tuples {
            stat_manager
                .new_record(tuple, ValueId::new(left_c_id))
                .unwrap();
        }

        let (right_table, right_tuples) = gen_test_table_and_tuples(right_c_id, right_tuple_count);
        stat_manager
            .register_container(right_c_id, right_table.schema.clone())
            .unwrap();
        for tuple in &right_tuples {
            stat_manager
                .new_record(tuple, ValueId::new(right_c_id))
                .unwrap();
        }

        // ia1: 0~999, ia2: 0~99, so the prob for eqs should be 0.001
        let eqs = vec![(
            AstExpr::Ident("ia3".to_string()),
            AstExpr::Ident("ia2".to_string()),
        )];

        // this filter prob is 0.5, so the total prob should be 0.0005
        let filter = Some(AstExpr::Boolean(
            BooleanOp::Gt,
            Box::new(AstExpr::Ident("ia1".to_string())),
            Box::new(AstExpr::Literal(Field::Int(4))),
        ));

        // Estimate join selectivity
        let (estimated_count, est_sel) = stat_manager
            .estimate_join_count_and_sel(left_c_id, right_c_id, eqs, filter)
            .unwrap();
        info!(
            "Estimated count: {}, Estimate_sel: {}",
            estimated_count, est_sel
        );

        assert_eq!(estimated_count >= 35000 && estimated_count <= 65000, true);
        assert_eq!(est_sel >= 0.00035 && est_sel <= 0.00065, true);
    }
}
*/
