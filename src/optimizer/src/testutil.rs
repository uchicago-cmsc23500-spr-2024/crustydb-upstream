use common::{
    ast_expr::AstExpr,
    ids::ContainerId,
    logical_plan::{LogicalPlan, OpIndex},
    BooleanOp, Field,
};

#[allow(unused)]
pub fn add_simple_scan_node(
    lp: &mut LogicalPlan,
    container_id: ContainerId,
    filter_strs: Option<Vec<(&str, &str, &str)>>,
    projection: Option<Vec<&str>>,
) -> OpIndex {
    let filter = filter_strs.map(|filters| {
        filters
            .into_iter()
            .map(|(left, operator, right)| {
                let left_expr = match left.parse::<i64>() {
                    Ok(val) => AstExpr::Literal(Field::Int(val)),
                    Err(_) => AstExpr::Ident(left.to_string()),
                };

                let right_expr = match right.parse::<i64>() {
                    Ok(val) => AstExpr::Literal(Field::Int(val)),
                    Err(_) => AstExpr::Ident(right.to_string()),
                };

                let op = match operator {
                    ">=" => BooleanOp::Gte,
                    "<=" => BooleanOp::Lte,
                    ">" => BooleanOp::Gt,
                    "<" => BooleanOp::Lt,
                    "==" => BooleanOp::Eq,
                    "!=" => BooleanOp::Neq,
                    _ => panic!("Unsupported operator: {}", operator),
                };

                AstExpr::Boolean(op, Box::new(left_expr), Box::new(right_expr))
            })
            .reduce(|acc, expr| AstExpr::Boolean(BooleanOp::And, Box::new(acc), Box::new(expr)))
            .unwrap()
    });

    let projection = projection.map(|idents| {
        idents
            .into_iter()
            .map(|ident| AstExpr::Ident(ident.to_string()))
            .collect()
    });

    lp.add_scan_node(container_id, filter, projection)
}

#[allow(unused)]
pub fn add_simple_filter_node(
    lp: &mut LogicalPlan,
    left: &str,
    operator: &str,
    right: &str,
    child_idx: Option<OpIndex>,
) -> OpIndex {
    let left_expr = match left.parse::<i64>() {
        Ok(val) => AstExpr::Literal(Field::Int(val)),
        Err(_) => AstExpr::Ident(left.to_string()),
    };

    let right_expr = match right.parse::<i64>() {
        Ok(val) => AstExpr::Literal(Field::Int(val)),
        Err(_) => AstExpr::Ident(right.to_string()),
    };

    let op = match operator {
        ">=" => BooleanOp::Gte,
        "<=" => BooleanOp::Lte,
        ">" => BooleanOp::Gt,
        "<" => BooleanOp::Lt,
        "==" => BooleanOp::Eq,
        "!=" => BooleanOp::Neq,
        _ => panic!("Unsupported operator: {}", operator),
    };

    let filter_predicate = AstExpr::Boolean(op, Box::new(left_expr), Box::new(right_expr));
    lp.add_filter_node(filter_predicate, child_idx)
}

#[allow(unused)]
pub fn add_simple_projection_node(
    lp: &mut LogicalPlan,
    identifiers: Vec<&str>,
    child_idx: Option<OpIndex>,
) -> OpIndex {
    let ast_identifiers = identifiers
        .into_iter()
        .map(|ident| AstExpr::Ident(ident.to_string()))
        .collect();
    lp.add_projection_node(ast_identifiers, child_idx)
}

#[allow(unused)]
pub fn add_simple_join_node(
    lp: &mut LogicalPlan,
    eqs: Vec<(&str, &str)>,
    filter_strs: Option<Vec<(&str, &str, &str)>>,
    left_idx: OpIndex,
    right_idx: OpIndex,
) -> OpIndex {
    let ast_eqs = eqs
        .into_iter()
        .map(|(left, right)| {
            (
                AstExpr::Ident(left.to_string()),
                AstExpr::Ident(right.to_string()),
            )
        })
        .collect();

    let filter = filter_strs.map(|filters| {
        filters
            .into_iter()
            .map(|(left, operator, right)| {
                let left_expr = match left.parse::<i64>() {
                    Ok(val) => AstExpr::Literal(Field::Int(val)),
                    Err(_) => AstExpr::Ident(left.to_string()),
                };

                let right_expr = match right.parse::<i64>() {
                    Ok(val) => AstExpr::Literal(Field::Int(val)),
                    Err(_) => AstExpr::Ident(right.to_string()),
                };

                let op = match operator {
                    ">=" => BooleanOp::Gte,
                    "<=" => BooleanOp::Lte,
                    ">" => BooleanOp::Gt,
                    "<" => BooleanOp::Lt,
                    "==" => BooleanOp::Eq,
                    "!=" => BooleanOp::Neq,
                    _ => panic!("Unsupported operator: {}", operator),
                };

                AstExpr::Boolean(op, Box::new(left_expr), Box::new(right_expr))
            })
            .reduce(|acc, expr| AstExpr::Boolean(BooleanOp::And, Box::new(acc), Box::new(expr)))
            .unwrap_or_else(|| AstExpr::Literal(Field::Null))
    });

    lp.add_join_node(ast_eqs, filter, left_idx, right_idx)
}

#[allow(unused)]
pub fn add_simple_cross_product_node(
    lp: &mut LogicalPlan,
    filter_strs: Option<Vec<(&str, &str, &str)>>,
    left_idx: OpIndex,
    right_idx: OpIndex,
) -> OpIndex {
    let filter = filter_strs.map(|filters| {
        filters
            .into_iter()
            .map(|(left, operator, right)| {
                let left_expr = match left.parse::<i64>() {
                    Ok(val) => AstExpr::Literal(Field::Int(val)),
                    Err(_) => AstExpr::Ident(left.to_string()),
                };

                let right_expr = match right.parse::<i64>() {
                    Ok(val) => AstExpr::Literal(Field::Int(val)),
                    Err(_) => AstExpr::Ident(right.to_string()),
                };

                let op = match operator {
                    ">=" => BooleanOp::Gte,
                    "<=" => BooleanOp::Lte,
                    ">" => BooleanOp::Gt,
                    "<" => BooleanOp::Lt,
                    "==" => BooleanOp::Eq,
                    "!=" => BooleanOp::Neq,
                    _ => panic!("Unsupported operator: {}", operator),
                };

                AstExpr::Boolean(op, Box::new(left_expr), Box::new(right_expr))
            })
            .reduce(|acc, expr| AstExpr::Boolean(BooleanOp::And, Box::new(acc), Box::new(expr)))
            .unwrap_or_else(|| AstExpr::Literal(Field::Null))
    });

    lp.add_cross_product_node(filter, left_idx, right_idx)
}
