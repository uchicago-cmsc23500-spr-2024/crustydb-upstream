use std::collections::BTreeSet;

use common::ast_expr::AstExpr;
use common::catalog::CatalogRef;
use common::logical_plan::*;
use common::CrustyError;

pub struct Optimizer {}

impl Optimizer {
    pub fn new() -> Self {
        Self {}
    }
}

impl Default for Optimizer {
    fn default() -> Self {
        Self::new()
    }
}

/// Rule-based Optimizer for logical plans.
pub trait OptimizerRule {
    /// Apply the rule to the logical plan.
    ///
    /// # Arguments
    ///
    /// * `plan` - Logical plan to apply the rule to.
    /// * `catalog` - Catalog to use for the rule.
    fn apply(&self, plan: LogicalPlan, catalog: &CatalogRef) -> Result<LogicalPlan, CrustyError>;
}

pub fn extract_columns_vec(expr: &Vec<AstExpr>, accum: &mut BTreeSet<String>) {
    for e in expr {
        extract_columns(e, accum);
    }
}

pub fn extract_columns(expr: &AstExpr, accum: &mut BTreeSet<String>) {
    match expr {
        AstExpr::Literal(_) => {
            // Do nothing
        }
        AstExpr::Ident(idx) => {
            accum.insert(idx.clone());
        }
        AstExpr::Alias(_, e) => {
            extract_columns(e, accum);
        }
        AstExpr::Math(_, l, r) => {
            extract_columns(l, accum);
            extract_columns(r, accum);
        }
        AstExpr::Boolean(_, l, r) => {
            extract_columns(l, accum);
            extract_columns(r, accum);
        }
        AstExpr::Agg(_, e) => {
            extract_columns(e, accum);
        }
        AstExpr::ColIdx(_) => {
            panic!("ColIdx should not be used before bind");
        }
    }
}
