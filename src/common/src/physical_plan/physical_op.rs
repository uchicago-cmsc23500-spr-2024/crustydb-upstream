use crate::ast_expr::AstExpr;
use crate::operation::BooleanOp;
use crate::prelude::*;

/// Physical Scan Operator
/// Same as Logical
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct PhysicalScanNode {
    pub container_id: ContainerId,
    pub filter: Option<AstExpr>,
    pub projection: Option<Vec<AstExpr>>,
}

/// Physical Project Operator
/// Same as Logical
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct PhysicalProjectNode {
    pub identifiers: Vec<AstExpr>,
}

/// Physical Update Operator
/// Same as Logical
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct PhysicalUpdateNode {
    /// Table to filter.
    pub alias: String,
    /// Table to update
    pub container_id: ContainerId,
    /// Values to assign. Literals only (eg x = 4, not x = x+4 or x = y)
    pub assignments: Vec<(AstExpr, Field)>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct PhysicalHashAggregateNode {
    /// Fields to aggregate.
    pub fields: Vec<AstExpr>,
    /// Fields to groupby.
    pub group_by: Vec<AstExpr>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct PhysicalCrossProductNode {}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct PhysicalNestedLoopJoinNode {
    pub left: AstExpr,
    pub right: AstExpr,
    pub op: BooleanOp,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct PhysicalHashJoinNode {
    /// Left side of the operator.
    pub left: AstExpr,
    /// Right side of the operator.
    pub right: AstExpr,
    /// Predicate operator.
    pub op: BooleanOp,
}

/// Physical Filter Operator
/// Same as Logical for now, but may want to add extra information
/// Like what order to perform the checks in a composite filter
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct PhysicalFilterNode {
    /// Predicate to filter by.
    pub predicate: AstExpr,
}

/// Materialized View Node
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct MaterializedViewNode {
    /// the ID of the materialized view
    pub materialized_view_state_id: ContainerId,
}

/// Physical Sort Node
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct PhysicalSortNode {
    pub fields: Vec<(AstExpr, bool)>, // (field, asc)
}

/// Physical Sort Merge Join Node
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct PhysicalSortMergeJoinNode {
    /// Left side of the operator.
    pub left_expr: Vec<(AstExpr, bool)>,
    /// Right side of the operator.
    pub right_expr: Vec<(AstExpr, bool)>,
}
