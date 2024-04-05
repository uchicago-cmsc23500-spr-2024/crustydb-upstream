use serde::{Deserialize, Serialize};

use std::fmt::Debug;

use crate::ast_expr::AstExpr;
use crate::ids::ContainerId;
use crate::Field;

/// Scan node.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ScanNode {
    pub container_id: ContainerId,
    pub filter: Option<AstExpr>,
    pub projection: Option<Vec<AstExpr>>,
}

/// Projection node.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ProjectNode {
    /// Identifiers for which columns to keep.
    pub identifiers: Vec<AstExpr>,
}

/// Aggregation node.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct AggregateNode {
    /// Fields to aggregate.
    pub fields: Vec<AstExpr>,
    /// Fields to groupby.
    pub group_by: Vec<AstExpr>,
    /// Filter to apply after aggregation.
    pub having: Option<AstExpr>,
}

/// Join node.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct JoinNode {
    /// Equal predicates (e.g. t0.a == t1.b)
    pub eqs: Vec<(AstExpr, AstExpr)>, // (left, right)
    /// Other predicates (e.g. t0.a > t1.b, t0.c < 5)
    pub filter: Option<AstExpr>,
}

/// CrossProduct node.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct CrossProductNode {
    /// Predicates to apply after the cross-product.
    pub filter: Option<AstExpr>,
}

/// Filter node.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct FilterNode {
    /// Predicate to filter by.
    pub predicate: AstExpr,
}

/// Orderby node.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct SortNode {
    pub fields: Vec<(AstExpr, bool)>, // (field, asc)
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct UpdateNode {
    /// Table to filter.
    pub alias: String,
    /// Table to update
    pub container_id: ContainerId,
    /// Values to assign. Literals only (eg x = 4, not x = x+4 or x = y)
    pub assignments: Vec<(AstExpr, Field)>,
}
