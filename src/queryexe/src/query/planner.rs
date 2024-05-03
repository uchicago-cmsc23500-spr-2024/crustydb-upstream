use crate::opiterator::{
    Aggregate, CrossJoin, Filter, HashEqJoin, NestedLoopJoin, OpIterator, Project, SeqScan,
};
use crate::Managers;
use common::ast_expr::bind_expr;
use common::bytecode_expr::ByteCodes;
use common::catalog::CatalogRef;
use common::error::c_err;
use common::logical_plan::*;
use common::physical_plan::*;
use common::prelude::*;
use common::Attribute;
use common::{ast_expr::AstExpr, bytecode_expr::ByteCodeExpr};
use common::{BooleanOp, MathOp};

/// Converts a logical operator into a physical operator
///
/// # Arguments
///
/// * `logical_op` - the logical operator to convert to a physical operator
/// * `physical_plan` - the physical plan to which the converted logical op will be added
/// * `catalog` - the catalog in which containers can be created during this conversion
fn logical_op_to_physical_op(
    logical_op: LogicalOp,
    physical_plan: &mut PhysicalPlan,
    _catalog: &CatalogRef,
) -> Result<PhysicalOp, CrustyError> {
    match logical_op {
        LogicalOp::ReadDeltas(_) | LogicalOp::WriteDeltas(_) | LogicalOp::Update(_) => {
            unimplemented!()
        }
        LogicalOp::Scan(ScanNode {
            container_id,
            filter,
            projection,
        }) => {
            physical_plan.add_base_table(container_id);
            Ok(PhysicalOp::Scan(PhysicalScanNode {
                container_id,
                filter,
                projection,
            }))
        }
        LogicalOp::Project(ProjectNode { identifiers }) => {
            Ok(PhysicalOp::Project(PhysicalProjectNode { identifiers }))
        }
        LogicalOp::Aggregate(AggregateNode {
            fields,
            group_by,
            having: _,
        }) => {
            // Creating a hash-table with the storage manager. Only needed if persisting the hash table for views.
            //<strip milestone="silent">
            // TODO add name?
            // TODO (jun) handle having
            //</strip>
            // Create hash aggregate node.
            Ok(PhysicalOp::HashAggregate(PhysicalHashAggregateNode {
                fields,
                group_by,
            }))
        }
        LogicalOp::CrossProduct(CrossProductNode { filter: _ }) => {
            // TODO: (Jun) Add filter support
            Ok(PhysicalOp::CrossProduct(PhysicalCrossProductNode {}))
        }
        LogicalOp::Join(JoinNode { eqs, filter: _ }) => {
            // TODO: (Jun) Add filter support
            let left = eqs[0].0.clone();
            let right = eqs[0].1.clone();
            let op = BooleanOp::Eq;

            if matches!(left, AstExpr::Ident(_))
                && matches!(right, AstExpr::Ident(_))
                && matches!(op, BooleanOp::Eq)
            {
                // If it is a simple join, we assume we can use a hash join
                Ok(PhysicalOp::HashJoin(PhysicalHashJoinNode {
                    left,
                    right,
                    op,
                }))
            } else {
                Ok(PhysicalOp::NestedLoopJoin(PhysicalNestedLoopJoinNode {
                    left,
                    right,
                    op,
                }))
            }
        }
        LogicalOp::Filter(FilterNode { predicate }) => {
            Ok(PhysicalOp::Filter(PhysicalFilterNode { predicate }))
        }
        LogicalOp::Sort(SortNode { fields }) => Ok(PhysicalOp::Sort(PhysicalSortNode { fields })),
    }
}

/// Converts a logical plan into a physical plan
///
/// # Arguments
///
/// * `logical_plan` - the logical plan to convert to a physical plan
/// * `catalog` - the catalog in which containers can be created during this conversion  
pub fn logical_plan_to_physical_plan(
    logical_plan: LogicalPlan,
    catalog: &CatalogRef,
) -> Result<PhysicalPlan, CrustyError> {
    let mut physical_plan = PhysicalPlan::new();
    for (_, node) in logical_plan.node_references() {
        let logical_op = node.data();
        let physical_op =
            logical_op_to_physical_op(logical_op.clone(), &mut physical_plan, catalog)?;
        physical_plan.add_node(physical_op);
    }

    for edge in logical_plan.edge_references() {
        physical_plan.add_edge(edge.source(), edge.target())
    }

    Ok(physical_plan)
}

pub fn is_computed_from(expr: &AstExpr, schema: &TableSchema) -> bool {
    match expr {
        AstExpr::Ident(name) => schema.contains(name.as_str()),
        AstExpr::Alias(_, child) => is_computed_from(child, schema),
        AstExpr::Literal(_) => true,
        AstExpr::Math(_, l, r) => is_computed_from(l, schema) && is_computed_from(r, schema),
        AstExpr::Boolean(_, l, r) => is_computed_from(l, schema) && is_computed_from(r, schema),
        AstExpr::Agg(_, e) => is_computed_from(e, schema),
        AstExpr::ColIdx(_i) => {
            panic!("Cannot call is_computed_from on a ColIdx")
        }
    }
}

/// Function to transform a logical expression (AST) to physical expression (Bytecode)
pub fn convert_ast_to_bytecode(
    expr: AstExpr,
    schema: &TableSchema,
) -> Result<ByteCodeExpr, CrustyError> {
    let bound_expr = bind_expr(expr, schema)?;
    let mut bytecode_expr = ByteCodeExpr::new();
    convert_ast_to_bytecode_inner(&bound_expr, &mut bytecode_expr)?;
    Ok(bytecode_expr)
}

fn convert_ast_to_bytecode_inner(
    expr: &AstExpr,
    bytecode_expr: &mut ByteCodeExpr,
) -> Result<(), CrustyError> {
    match expr {
        AstExpr::Ident(_) | AstExpr::Alias(_, _) | AstExpr::Agg(_, _) => {
            return Err(c_err(
                "Ident, Alias, and Agg should have been handled in conversion to logical plan or bound expression",
            ));
        }
        AstExpr::Literal(l) => {
            let i = bytecode_expr.add_literal(l.clone());
            bytecode_expr.add_code(ByteCodes::PushLit as usize);
            bytecode_expr.add_code(i);
        }
        AstExpr::Math(op, l, r) => {
            // (a+b)-(c+d) Bytecode will be [a][b][+][c][d][+][-]
            // i, Stack
            // 0, [a]
            // 1, [a][b]
            // 2, [a+b]
            // 3, [a+b][c]
            // 4, [a+b][c][d]
            // 5, [a+b][c+d]
            // 6, [a+b-c-d]
            convert_ast_to_bytecode_inner(l, bytecode_expr)?;
            convert_ast_to_bytecode_inner(r, bytecode_expr)?;
            match op {
                MathOp::Add => bytecode_expr.add_code(ByteCodes::Add as usize),
                MathOp::Sub => bytecode_expr.add_code(ByteCodes::Sub as usize),
                MathOp::Mul => bytecode_expr.add_code(ByteCodes::Mul as usize),
                MathOp::Div => bytecode_expr.add_code(ByteCodes::Div as usize),
            }
        }
        AstExpr::Boolean(op, l, r) => {
            convert_ast_to_bytecode_inner(l, bytecode_expr)?;
            convert_ast_to_bytecode_inner(r, bytecode_expr)?;
            match op {
                BooleanOp::Eq => bytecode_expr.add_code(ByteCodes::Eq as usize),
                BooleanOp::Neq => bytecode_expr.add_code(ByteCodes::Neq as usize),
                BooleanOp::Gt => bytecode_expr.add_code(ByteCodes::Gt as usize),
                BooleanOp::Gte => bytecode_expr.add_code(ByteCodes::Gte as usize),
                BooleanOp::Lt => bytecode_expr.add_code(ByteCodes::Lt as usize),
                BooleanOp::Lte => bytecode_expr.add_code(ByteCodes::Lte as usize),
                BooleanOp::And => bytecode_expr.add_code(ByteCodes::And as usize),
                BooleanOp::Or => bytecode_expr.add_code(ByteCodes::Or as usize),
            }
        }
        AstExpr::ColIdx(i) => {
            bytecode_expr.add_code(ByteCodes::PushField as usize);
            bytecode_expr.add_code(*i);
        }
    }
    Ok(())
}

/// Converts a physical_plan to an op_iterator.
///
/// # Arguments
///
/// * `catalog` - Catalog of the database containing the metadata about the tables and such.
/// * `physical_plan` - Translated physical plan of the query.
/// * `tid` - Id of the transaction that this executor is running.
pub fn physical_plan_to_op_iterator(
    managers: &'static Managers,
    catalog: &CatalogRef,
    physical_plan: &PhysicalPlan,
    tid: TransactionId,
    _timestamp: LogicalTimeStamp,
) -> Result<Box<dyn OpIterator>, CrustyError> {
    let start = physical_plan
        .root()
        .ok_or_else(|| CrustyError::ExecutionError(String::from("No root node")))?;
    physical_plan_to_op_iterator_helper(managers, catalog, physical_plan, start, tid)
}

/// Recursive helper function to parse physical plan into opiterator.
///
/// Function first converts all of the current nodes children to an opiterator before converting self to an opiterator.
///
/// # Arguments
///
/// * `catalog` - Catalog of the database containing the metadata about the tables and such.
/// * `physical plan` - physical plan of the query.
/// * `tid` - Id of the transaction that this executor is running.
fn physical_plan_to_op_iterator_helper(
    managers: &'static Managers,
    catalog: &CatalogRef,
    physical_plan: &PhysicalPlan,
    start: OpIndex,
    tid: TransactionId,
) -> Result<Box<dyn OpIterator>, CrustyError> {
    let err = CrustyError::ExecutionError(String::from("Malformed logical plan"));

    // Recursively convert the children in node of physical plan to opiterator.
    let mut children = physical_plan
        .edges(start)
        .map(|n| physical_plan_to_op_iterator_helper(managers, catalog, physical_plan, n, tid));

    // Converts the current node in physical plan to an opiterator.
    let op = physical_plan
        .get_operator(start)
        .ok_or_else(|| err.clone())?;
    let result: Result<Box<dyn OpIterator>, CrustyError> = match op {
        PhysicalOp::Scan(PhysicalScanNode {
            container_id,
            filter,
            projection,
        }) => {
            let in_schema = catalog.get_table_schema(*container_id).unwrap();
            let out_schema = if let Some(p) = projection {
                // If projection is specified, we need to create a new schema
                let mut attrs = Vec::new();
                for e in p {
                    attrs.push(e.to_attr(&in_schema));
                }
                TableSchema::new(attrs)
            } else {
                in_schema.clone()
            };
            let filter = filter
                .as_ref()
                .map(|f| convert_ast_to_bytecode(f.clone(), &in_schema))
                .transpose()?;
            let projection = projection
                .as_ref()
                .map(|p| {
                    p.iter()
                        .map(|e| convert_ast_to_bytecode(e.clone(), &in_schema))
                        .collect::<Result<Vec<ByteCodeExpr>, CrustyError>>()
                })
                .transpose()?;
            let scan_iter =
                SeqScan::new(managers, &out_schema, container_id, tid, filter, projection);
            Ok(Box::new(scan_iter))
        }
        PhysicalOp::Project(PhysicalProjectNode { identifiers }) => {
            let child = children.next().ok_or_else(|| err.clone())??;
            let input_schema = child.get_schema();
            let attrs = identifiers
                .iter()
                .map(|i| i.to_attr(input_schema))
                .collect::<Vec<Attribute>>();
            let schema = TableSchema::new(attrs);
            let project_iter = Project::new(
                identifiers
                    .iter()
                    .map(|e| convert_ast_to_bytecode(e.clone(), input_schema).unwrap())
                    .collect(),
                schema,
                child,
            );
            Ok(Box::new(project_iter))
        }
        PhysicalOp::HashAggregate(PhysicalHashAggregateNode {
            fields, group_by, ..
        }) => {
            let child = children.next().ok_or_else(|| err.clone())??;
            let input_schema = child.get_schema();
            let mut attrs = Vec::new(); // Attributes for the new schema

            let mut group_by_expr = Vec::new();
            for e in group_by {
                group_by_expr.push(convert_ast_to_bytecode(e.clone(), input_schema)?);
                attrs.push(e.to_attr(input_schema));
            }
            let mut ops = Vec::new();
            let mut agg_expr = Vec::new();
            for e in fields {
                attrs.push(e.to_attr(input_schema));
                match e {
                    AstExpr::Agg(op, e) => {
                        ops.push(*op);
                        agg_expr.push(convert_ast_to_bytecode(*e.clone(), input_schema)?);
                    }
                    _ => return Err(c_err("Unexpected expression in HashAggregateNode")),
                }
            }
            let schema = TableSchema::new(attrs);
            let agg = Aggregate::new(managers, group_by_expr, agg_expr, ops, schema, child);
            Ok(Box::new(agg))
        }
        PhysicalOp::CrossProduct(PhysicalCrossProductNode {}) => {
            let left_child = children.next().ok_or_else(|| err.clone())??;
            let left_schema = left_child.get_schema();
            let right_child = children.next().ok_or_else(|| err.clone())??;
            let right_schema = right_child.get_schema();
            let schema = left_schema.merge(right_schema);
            let cross_iter = CrossJoin::new(schema, left_child, right_child);
            Ok(Box::new(cross_iter))
        }
        PhysicalOp::NestedLoopJoin(PhysicalNestedLoopJoinNode { left, right, op }) => {
            let left_child = children.next().ok_or_else(|| err.clone())??;
            let left_schema = left_child.get_schema();
            let right_child = children.next().ok_or_else(|| err.clone())??;
            let right_schema = right_child.get_schema();
            let schema = left_schema.merge(right_schema);

            let join_iter = if is_computed_from(left, left_schema)
                && is_computed_from(right, right_schema)
            {
                NestedLoopJoin::new(
                    *op,
                    convert_ast_to_bytecode(left.clone(), left_schema)?,
                    convert_ast_to_bytecode(right.clone(), right_schema)?,
                    left_child,
                    right_child,
                    schema,
                )
            } else if is_computed_from(right, left_schema) && is_computed_from(left, right_schema) {
                NestedLoopJoin::new(
                    *op,
                    convert_ast_to_bytecode(right.clone(), left_schema)?,
                    convert_ast_to_bytecode(left.clone(), right_schema)?,
                    left_child,
                    right_child,
                    schema,
                )
            } else {
                Err(c_err("NestedLoopJoin failed to find a joinable expression"))?
            };
            Ok(Box::new(join_iter))
        }
        PhysicalOp::HashJoin(PhysicalHashJoinNode {
            left, right, op: _, ..
        }) => {
            let left_child = children.next().ok_or_else(|| err.clone())??;
            let left_schema = left_child.get_schema();
            let right_child = children.next().ok_or_else(|| err.clone())??;
            let right_schema = right_child.get_schema();

            let join_iter = if is_computed_from(left, left_schema)
                && is_computed_from(right, right_schema)
            {
                let schema = left_schema.merge(right_schema);
                HashEqJoin::new(
                    managers,
                    schema,
                    convert_ast_to_bytecode(left.clone(), left_schema)?,
                    convert_ast_to_bytecode(right.clone(), right_schema)?,
                    left_child,
                    right_child,
                )
            } else if is_computed_from(right, left_schema) && is_computed_from(left, right_schema) {
                let schema = right_schema.merge(left_schema);
                HashEqJoin::new(
                    managers,
                    schema,
                    convert_ast_to_bytecode(right.clone(), left_schema)?,
                    convert_ast_to_bytecode(left.clone(), right_schema)?,
                    right_child,
                    left_child,
                )
            } else {
                Err(c_err("HashJoin failed to find a joinable expression"))?
            };

            Ok(Box::new(join_iter))
        }
        PhysicalOp::Filter(PhysicalFilterNode { predicate, .. }) => {
            let child = children.next().ok_or_else(|| err.clone())??;
            let input_schema = child.get_schema();
            let filter_iter = Filter::new(
                convert_ast_to_bytecode(predicate.clone(), input_schema)?,
                input_schema.clone(),
                child,
            );
            Ok(Box::new(filter_iter))
        }
        PhysicalOp::Sort(PhysicalSortNode { fields: _ }) => {
            panic!("Sort not implemented")
        }
        PhysicalOp::SortMergeJoin(PhysicalSortMergeJoinNode {
            left_expr: _,
            right_expr: _,
        }) => {
            panic!("SortMergeJoin not implemented")
        }
        PhysicalOp::MaterializedView(_) => unimplemented!(),
        PhysicalOp::Update(PhysicalUpdateNode {
            alias: _,
            container_id: _,
            assignments: _,
        }) => {
            /*
            let child = children.next().ok_or_else(|| err.clone())??;
            let mut field_idents = Vec::new();
            let mut fields = Vec::new();
            for (fi, f) in assignments.iter() {
                field_idents.push(fi.clone());
                fields.push(f.clone());
            }
            // The above code should use the following but an issue with field trait
            //let (field_idents, fields): (Vec<FieldIdentifier>, Vec<Field>) = assignments.into_iter().map(|(a, b)| (a, b)).unzip();
            let (indices, _) =
                Self::get_field_indices_names(&field_idents, child.get_schema())?;
            debug!(
                " Creating update opIterator.  assignments: {{assignments}} indices: {:?}",
                indices,
            );
            let update = Update::new(
                managers,
                transaction_manager,
                container_id,
                tid,
                indices.into_iter().zip(fields).collect(),
                child,
            );
            Ok(Box::new(update))
            */
            unimplemented!()
        }
    };

    if children.next().is_some() {
        Err(err)
    } else {
        result
    }
}
