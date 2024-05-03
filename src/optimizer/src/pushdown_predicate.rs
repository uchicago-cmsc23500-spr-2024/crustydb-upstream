use std::collections::BTreeSet;

use common::ast_expr::AstExpr;
use common::catalog::CatalogRef;
use common::logical_plan::{
    AggregateNode, CrossProductNode, JoinNode, LogicalOp, LogicalPlan, OpIndex, ScanNode,
};
use common::BooleanOp;
use common::CrustyError;

use crate::optimizer::{extract_columns, extract_columns_vec, OptimizerRule};

pub struct PredicatePushdown {}

impl PredicatePushdown {
    fn pushdown(
        lp: &LogicalPlan,
        start: usize,
        predicates: &mut Vec<AstExpr>,
        catalog: &CatalogRef,
    ) -> LogicalPlan {
        let op = lp.get_operator(start).unwrap();
        match op {
            LogicalOp::Project(p) => Self::pushdown_through_node(
                lp,
                start,
                predicates,
                catalog,
                LogicalOp::Project(p.clone()),
            ),
            LogicalOp::Filter(f) => {
                // Separate the predicates in the Filter node by 'AND' and append to 'predicates'.
                Self::separate_predicates_by_and(&f.predicate, predicates);

                // Since Filter node can have only one child,
                // we can directly push down the predicates to the child and delete the Filter node.
                let child_idx = lp.edges(start).next().unwrap();

                Self::pushdown(lp, child_idx, predicates, catalog)
            }
            LogicalOp::Join(j) => {
                let children: Vec<_> = lp.edges(start).collect();
                if children.len() != 2 {
                    panic!("Join should have exactly two children");
                }

                let (left_child, right_child) = (children[0], children[1]);

                let mut left_predicates: Vec<AstExpr> = Vec::new();
                let mut right_predicates: Vec<AstExpr> = Vec::new();
                let mut new_eqs: Vec<(AstExpr, AstExpr)> = j.eqs.clone(); // remaining eqs after pushdown (e.g. LEFT_TABLE == RIGHT_TABLE)
                let mut remaining_predicates: Vec<AstExpr> = Vec::new(); // remaining predicates after pushdown but not eqs

                if let Some(filter) = &j.filter {
                    Self::separate_predicates_by_and(filter, predicates);
                }

                for predicate in predicates.drain(..) {
                    if Self::can_be_pushed_down(&predicate, lp, left_child, catalog) {
                        left_predicates.push(predicate);
                    } else if Self::can_be_pushed_down(&predicate, lp, right_child, catalog) {
                        right_predicates.push(predicate);
                    } else {
                        match &predicate {
                            AstExpr::Boolean(BooleanOp::Eq, left, right) => {
                                if Self::can_be_pushed_down(left, lp, left_child, catalog)
                                    && Self::can_be_pushed_down(right, lp, right_child, catalog)
                                {
                                    new_eqs.push((*left.clone(), *right.clone()));
                                } else if Self::can_be_pushed_down(left, lp, right_child, catalog)
                                    && Self::can_be_pushed_down(right, lp, left_child, catalog)
                                {
                                    new_eqs.push((*right.clone(), *left.clone()));
                                } else {
                                    remaining_predicates.push(predicate);
                                }
                            }
                            _ => remaining_predicates.push(predicate),
                        }
                    }
                }

                // Push down predicates to the left and right children
                let mut new_left_lp = Self::pushdown(lp, left_child, &mut left_predicates, catalog);
                let new_right_lp = Self::pushdown(lp, right_child, &mut right_predicates, catalog);

                // Reconstruct the JoinNode with any remaining predicates
                let new_join_node = LogicalOp::Join(JoinNode {
                    eqs: new_eqs,
                    filter: if !remaining_predicates.is_empty() {
                        Some(Self::combine_predicates_with_and(&remaining_predicates).unwrap())
                    } else {
                        None
                    },
                });
                new_left_lp.merge(new_join_node, new_right_lp);
                new_left_lp
            }
            LogicalOp::CrossProduct(cp) => {
                // Similar to Join
                let children: Vec<_> = lp.edges(start).collect();
                if children.len() != 2 {
                    panic!("CrossProduct should have exactly two children");
                }

                let (left_child, right_child) = (children[0], children[1]);

                if let Some(filter) = &cp.filter {
                    Self::separate_predicates_by_and(filter, predicates);
                }

                // All of predicates are separated into below 4 categories
                let mut left_predicates: Vec<AstExpr> = Vec::new();
                let mut right_predicates: Vec<AstExpr> = Vec::new();
                let mut new_eqs: Vec<(AstExpr, AstExpr)> = Vec::new();
                let mut remaining_predicates: Vec<AstExpr> = Vec::new();

                for predicate in predicates.drain(..) {
                    if Self::can_be_pushed_down(&predicate, lp, left_child, catalog) {
                        left_predicates.push(predicate);
                    } else if Self::can_be_pushed_down(&predicate, lp, right_child, catalog) {
                        right_predicates.push(predicate);
                    } else {
                        match &predicate {
                            AstExpr::Boolean(BooleanOp::Eq, left, right) => {
                                if Self::can_be_pushed_down(left, lp, left_child, catalog)
                                    && Self::can_be_pushed_down(right, lp, right_child, catalog)
                                {
                                    new_eqs.push((*left.clone(), *right.clone()));
                                } else if Self::can_be_pushed_down(left, lp, right_child, catalog)
                                    && Self::can_be_pushed_down(right, lp, left_child, catalog)
                                {
                                    new_eqs.push((*right.clone(), *left.clone()));
                                } else {
                                    remaining_predicates.push(predicate);
                                }
                            }
                            _ => remaining_predicates.push(predicate),
                        }
                    }
                }

                // Push down predicates to left and right children
                let mut new_left_lp = Self::pushdown(lp, left_child, &mut left_predicates, catalog);
                let new_right_lp = Self::pushdown(lp, right_child, &mut right_predicates, catalog);

                // Decide to keep as CrossProduct or convert to Join depending on the new_eqs
                let new_node = if !new_eqs.is_empty() {
                    LogicalOp::Join(JoinNode {
                        eqs: new_eqs,
                        filter: Self::combine_predicates_with_and(&remaining_predicates),
                    })
                } else {
                    LogicalOp::CrossProduct(CrossProductNode {
                        filter: Self::combine_predicates_with_and(&remaining_predicates),
                    })
                };
                new_left_lp.merge(new_node, new_right_lp);
                new_left_lp
            }
            LogicalOp::Scan(s) => {
                if let Some(existing_filter) = &s.filter {
                    Self::separate_predicates_by_and(existing_filter, predicates);
                }
                let new_filter_condition = Self::combine_predicates_with_and(predicates);

                let scan_node = LogicalOp::Scan(ScanNode {
                    container_id: s.container_id,
                    filter: new_filter_condition,
                    projection: s.projection.clone(),
                });

                let mut new_lp = LogicalPlan::new();
                let _scan_idx = new_lp.add_node(scan_node);
                new_lp
            }
            LogicalOp::Aggregate(a) => {
                let child = lp.edges(start).next().unwrap();

                if let Some(having) = &a.having {
                    Self::separate_predicates_by_and(having, predicates);
                }

                print!("\n{:?}\n", predicates);

                let mut having_predicates: Vec<AstExpr> = Vec::new();
                let mut pushing_predicates: Vec<AstExpr> = Vec::new();
                for predicate in predicates.drain(..) {
                    if Self::can_be_pushed_down(&predicate, lp, child, catalog) {
                        pushing_predicates.push(predicate);
                    } else {
                        having_predicates.push(predicate);
                    }
                }

                let mut new_lp = Self::pushdown(lp, child, &mut pushing_predicates, catalog);
                let new_aggregate_node = LogicalOp::Aggregate(AggregateNode {
                    fields: a.fields.clone(),
                    group_by: a.group_by.clone(),
                    having: Self::combine_predicates_with_and(&having_predicates),
                });
                let old_root = new_lp.root().unwrap();
                let new_agg_idx = new_lp.add_node(new_aggregate_node);
                new_lp.add_edge(new_agg_idx, old_root);
                new_lp
            }
            LogicalOp::Sort(s) => Self::pushdown_through_node(
                lp,
                start,
                predicates,
                catalog,
                LogicalOp::Sort(s.clone()),
            ),
            LogicalOp::Update(_) | LogicalOp::ReadDeltas(_) | LogicalOp::WriteDeltas(_) => {
                unimplemented!("Predicate pushdown not implemented for this operator");
            }
        }
    }
}

// Utility functions for PredicatePushdown
impl PredicatePushdown {
    fn combine_predicates_with_and(predicates: &[AstExpr]) -> Option<AstExpr> {
        if predicates.is_empty() {
            None
        } else {
            Some(
                predicates
                    .iter()
                    .cloned()
                    .reduce(|a, b| AstExpr::Boolean(BooleanOp::And, Box::new(a), Box::new(b)))
                    .unwrap(),
            )
        }
    }

    fn separate_predicates_by_and(predicate: &AstExpr, separated_predicates: &mut Vec<AstExpr>) {
        match predicate {
            AstExpr::Boolean(BooleanOp::And, left, right) => {
                Self::separate_predicates_by_and(left, separated_predicates);
                Self::separate_predicates_by_and(right, separated_predicates);
            }
            _ => separated_predicates.push(predicate.clone()),
        }
    }

    fn can_be_pushed_down(
        predicate: &AstExpr,
        lp: &LogicalPlan,
        child_index: usize,
        catalog: &CatalogRef,
    ) -> bool {
        let mut columns = BTreeSet::new();
        extract_columns(predicate, &mut columns);

        let child_columns = Self::extract_columns_from_plan(lp, child_index, catalog);

        // Check if all columns in the predicate exist in the child's plan
        columns.is_subset(&child_columns)
    }

    fn extract_columns_from_plan(
        lp: &LogicalPlan,
        index: OpIndex,
        catalog: &CatalogRef,
    ) -> BTreeSet<String> {
        let mut columns = BTreeSet::new();
        if let Some(op) = lp.get_operator(index) {
            match op {
                LogicalOp::Scan(scan) => {
                    if let Some(projection) = &scan.projection {
                        extract_columns_vec(projection, &mut columns);
                    } else {
                        // Retrieve all columns from the table schema
                        match catalog.get_table_schema(scan.container_id) {
                            Some(schema) => {
                                for attr in schema.attributes() {
                                    columns.insert(attr.name.clone());
                                }
                            }
                            None => panic!(
                                "Failed to retrieve table schema for container_id: {}",
                                scan.container_id
                            ),
                        }
                    }
                }
                LogicalOp::Project(project) => {
                    // Extract columns from Project node
                    extract_columns_vec(&project.identifiers, &mut columns);
                }
                LogicalOp::Join(_join) => {
                    // Extract columns from Join node
                    for child_index in lp.edges(index) {
                        columns.extend(Self::extract_columns_from_plan(lp, child_index, catalog));
                    }
                }
                LogicalOp::Aggregate(agg) => {
                    // Extract columns from Aggregate node
                    extract_columns_vec(&agg.fields, &mut columns);
                    extract_columns_vec(&agg.group_by, &mut columns);
                }
                LogicalOp::Filter(_filter) => {
                    // Extract columns from Filter node
                    for child_index in lp.edges(index) {
                        columns.extend(Self::extract_columns_from_plan(lp, child_index, catalog));
                    }
                }
                LogicalOp::CrossProduct(_) => {
                    // CrossProduct does not directly use columns, but its children might
                    // Process children of CrossProduct
                    for child_index in lp.edges(index) {
                        columns.extend(Self::extract_columns_from_plan(lp, child_index, catalog));
                    }
                }
                LogicalOp::Sort(_sort) => {
                    // Extract columns from Sort node
                    for child_index in lp.edges(index) {
                        columns.extend(Self::extract_columns_from_plan(lp, child_index, catalog));
                    }
                }
                LogicalOp::ReadDeltas(_) | LogicalOp::WriteDeltas(_) | LogicalOp::Update(_) => {
                    unimplemented!("Predicate pushdown not implemented for this operator");
                }
            }
        }
        columns
    }

    fn pushdown_through_node(
        lp: &LogicalPlan,
        start: usize,
        predicates: &mut Vec<AstExpr>,
        catalog: &CatalogRef,
        new_op: LogicalOp,
    ) -> LogicalPlan {
        let child = lp.edges(start).next().unwrap();
        let mut new_lp = Self::pushdown(lp, child, predicates, catalog);
        let old_root = new_lp.root().unwrap();
        let new_node_idx = new_lp.add_node(new_op);
        new_lp.add_edge(new_node_idx, old_root);
        new_lp
    }
}

impl OptimizerRule for PredicatePushdown {
    fn apply(&self, plan: LogicalPlan, catalog: &CatalogRef) -> Result<LogicalPlan, CrustyError> {
        let mut predicates = Vec::new();
        let new_plan = Self::pushdown(&plan, plan.root().unwrap(), &mut predicates, catalog);
        Ok(new_plan)
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::testutil::{
        add_simple_filter_node, add_simple_join_node, add_simple_projection_node,
        add_simple_scan_node,
    };
    use common::AggOp;
    use queryexe::testutil::TestSetup;

    /// Tests the predicate pushdown optimizer for a scenario without a join.
    ///
    /// ## Original SQL Query
    /// ```
    /// SELECT table0.a, table0.b
    /// FROM table0
    /// WHERE table0.a >= 1 AND table0.b < 5;
    /// ```
    ///
    /// ### Original Plan Tree
    /// ```
    /// Project Node (Identifiers: ["table0.a", "table0.b"])
    /// └───Filter Node (Predicate: table0.a >= 1)
    ///     └───Filter Node (Predicate: table0.b < 5)
    ///         └───Scan Node (Container ID: [ID for table0], Filter: None, Projection: None)
    /// ```
    ///
    /// ## Expected SQL Query After Predicate Pushdown
    /// The expected SQL after applying predicate pushdown optimization. The filters are pushed closer to the scan node.
    /// ```
    /// SELECT table0.a, table0.b
    /// FROM (
    ///     SELECT *
    ///     FROM table0
    ///     WHERE table0.b < 5 AND table0.a >= 1
    /// ) AS subtable0;
    /// ```
    ///
    /// ### Expected Plan Tree After Predicate Pushdown
    /// ```
    /// Project Node (Identifiers: ["table0.a", "table0.b"])
    /// └───Scan Node (Container ID: [ID for table0], Filter: table0.b < 5 AND table0.a >= 1, Projection: None)
    /// ```
    #[test]
    fn test_pushdown_predicate_without_join() {
        let catalog = TestSetup::new_with_content().catalog;
        let c_id = catalog.get_table_id("table0");

        // Construct the logical plan
        let mut lp = LogicalPlan::new();
        lp.add_scan_node(c_id, None, None);
        add_simple_filter_node(&mut lp, "table0.a", ">=", "1", None);
        add_simple_filter_node(&mut lp, "table0.b", "<", "5", None);
        add_simple_projection_node(&mut lp, vec!["table0.a", "table0.b"], None);

        // Apply the predicate pushdown optimizer
        let optimized_lp =
            PredicatePushdown::pushdown(&lp, lp.root().unwrap(), &mut Vec::new(), &catalog);

        // Construct the expected logical plan after applying predicate pushdown
        let mut expected_lp = LogicalPlan::new();
        add_simple_scan_node(
            &mut expected_lp,
            c_id,
            Some(vec![("table0.b", "<", "5"), ("table0.a", ">=", "1")]),
            None,
        );
        add_simple_projection_node(&mut expected_lp, vec!["table0.a", "table0.b"], None);

        // Compare the expected and actual logical plans
        assert_eq!(optimized_lp.to_json(), expected_lp.to_json());
    }

    /// Tests the predicate pushdown optimizer for a join scenario with multiple filters.
    ///
    /// ## Original SQL Query
    /// ```
    /// SELECT table0.a, table0.b, table1.a, table1.c
    /// FROM table0
    /// JOIN table1 ON table0.a = table1.a
    /// WHERE table0.x != table1.y
    ///      AND table0.b > 10
    ///      AND table1.c < 20
    ///      AND table0.b = table1.b;
    /// ```
    ///
    /// ### Original Plan Tree
    /// ```
    /// Project Node (Identifiers: ["table0.a", "table0.b", "table1.a", "table1.c"])
    /// └───Filter Node (Predicate: table0.b = table1.b)
    ///     └───Filter Node (Predicate: table1.c < 20)
    ///         └───Filter Node (Predicate: table0.b > 10)
    ///             └───Filter Node (Predicate: table0.x != table1.y)
    ///                 └───Join Node (Eq Conditions: ["table0.a", "table1.a"], Filter: None)
    ///                     ├───Scan Node (Container ID: [ID for table0], Filter: None, Projection: None)
    ///                     └───Scan Node (Container ID: [ID for table1], Filter: None, Projection: None)
    /// ```
    ///
    /// ## Expected SQL Query After Predicate Pushdown
    /// The expected SQL after applying predicate pushdown optimization. The filters are pushed closer to the scan node and join node.
    /// ```
    /// SELECT table0.a, table0.b, table1.a, table1.c
    /// FROM (
    ///     SELECT *
    ///     FROM table0
    ///     WHERE table0.b > 10
    /// ) AS subtable0
    /// JOIN (
    ///     SELECT *
    ///     FROM table1
    ///     WHERE table1.c < 20
    /// ) AS subtable1 ON subtable0.a = subtable1.a AND subtable0.b = subtable1.b
    /// WHERE subtable0.x != subtable1.y;
    /// ```
    ///
    /// ### Expected Plan Tree After Predicate Pushdown
    /// ```
    /// Project Node (Identifiers: ["table0.a", "table0.b", "table1.a", "table1.c"])
    /// └───Join Node (Eq Conditions: ["table0.a", "table1.a"], ["table0.b", "table1.b"], Filter: table0.x != table1.y)
    ///     ├───Scan Node (Container ID: [ID for table0], Filter: table0.b > 10, Projection: None)
    ///     └───Scan Node (Container ID: [ID for table1], Filter: table1.c < 20, Projection: None)
    /// ```
    #[test]
    fn test_predicate_pushdown_with_join() {
        let catalog = TestSetup::new_with_content().catalog;
        let c_id0 = catalog.get_table_id("table0");
        let c_id1 = catalog.get_table_id("table1");

        // Construct the logical plan
        let mut lp = LogicalPlan::new();
        let scan_idx0 = lp.add_scan_node(c_id0, None, None);
        let scan_idx1 = lp.add_scan_node(c_id1, None, None);
        add_simple_join_node(
            &mut lp,
            vec![("table1.a", "table0.a")],
            None,
            scan_idx0,
            scan_idx1,
        );
        add_simple_filter_node(&mut lp, "table0.x", "!=", "table1.y", None);
        add_simple_filter_node(&mut lp, "table0.b", ">", "10", None);
        add_simple_filter_node(&mut lp, "table1.c", "<", "20", None);
        add_simple_filter_node(&mut lp, "table0.b", "==", "table1.b", None);
        add_simple_projection_node(
            &mut lp,
            vec!["table0.a", "table0.b", "table1.a", "table1.c"],
            None,
        );

        // Apply the predicate pushdown optimizer
        let optimized_lp =
            PredicatePushdown::pushdown(&lp, lp.root().unwrap(), &mut Vec::new(), &catalog);

        // Construct the expected logical plan after applying predicate pushdown
        let mut expected_lp = LogicalPlan::new();
        let escan_idx1 = add_simple_scan_node(
            &mut expected_lp,
            c_id1,
            Some(vec![("table1.c", "<", "20")]),
            None,
        );
        let escan_idx0 = add_simple_scan_node(
            &mut expected_lp,
            c_id0,
            Some(vec![("table0.b", ">", "10")]),
            None,
        );
        add_simple_join_node(
            &mut expected_lp,
            vec![("table1.a", "table0.a"), ("table1.b", "table0.b")],
            Some(vec![("table0.x", "!=", "table1.y")]),
            escan_idx0,
            escan_idx1,
        );
        add_simple_projection_node(
            &mut expected_lp,
            vec!["table0.a", "table0.b", "table1.a", "table1.c"],
            None,
        );

        // Compare the expected and actual logical plans
        assert_eq!(optimized_lp.to_json(), expected_lp.to_json());
    }

    /// Tests the predicate pushdown optimizer for a scenario with multiple cross products and filters.
    ///
    /// ## Original SQL Query
    /// ```
    /// SELECT table0.a, table1.b, table2.c
    /// FROM table0, table1, table2
    /// WHERE table0.a > 10
    ///   AND table1.b < 20
    ///   AND table2.c = 30
    ///   AND table0.a = table1.a
    ///   AND table0.b != table1.b
    ///   AND table0.a = table2.a
    ///   AND table1.b != table2.b;
    /// ```
    ///
    /// ### Original Plan Tree
    /// ```
    /// Project Node (Identifiers: ["table0.a", "table1.b", "table2.c"])
    /// └───Filter Node (Predicate: table0.a = table2.a)
    ///     └───Filter Node (Predicate: table0.a = table1.a)
    ///         └───Filter Node (Predicate: table1.b != table2.b)
    ///             └───Filter Node (Predicate: table0.b != table1.b)
    ///                 └───Filter Node (Predicate: table2.c = 30)
    ///                     └───Filter Node (Predicate: table1.b < 20)
    ///                         └───Filter Node (Predicate: table0.a > 10)
    ///                             └───Cross Product Node
    ///                                 └───Cross Product Node
    ///                                     ├───Scan Node (Container ID: [ID for table0], Filter: None, Projection: None)
    ///                                     ├───Scan Node (Container ID: [ID for table1], Filter: None, Projection: None)
    ///                                     └───Scan Node (Container ID: [ID for table2], Filter: None, Projection: None)
    /// ```
    ///
    /// ## Expected SQL Query After Predicate Pushdown
    /// The expected SQL after applying predicate pushdown optimization. The filters are pushed closer to the scan nodes and combined in join conditions.
    /// ```
    /// SELECT subtable0.a, subtable1.b, subtable2.c
    /// FROM (
    ///     SELECT a, b
    ///     FROM table0
    ///     WHERE a > 10
    /// ) AS subtable0
    /// INNER JOIN (
    ///     SELECT a, b
    ///     FROM table1
    ///     WHERE b < 20
    /// ) AS subtable1 ON subtable0.a = subtable1.a AND subtable0.b != subtable1.b
    /// INNER JOIN (
    ///     SELECT a, c
    ///     FROM table2
    ///     WHERE c = 30
    /// ) AS subtable2 ON subtable0.a = subtable2.a AND subtable1.b != subtable2.b;
    /// ```
    ///
    /// ### Expected Plan Tree After Predicate Pushdown
    /// ```
    /// Project Node (Identifiers: ["table0.a", "table1.b", "table2.c"])
    /// └───Join Node (Eq Conditions: ["table0.a", "table2.a"], ["table1.b", "table2.b"], Filter: None)
    ///     ├───Join Node (Eq Conditions: ["table0.a", "table1.a"], ["table0.b", "table1.b"], Filter: None)
    ///     │   ├───Scan Node (Container ID: [ID for table0], Filter: table0.a > 10, Projection: None)
    ///     │   └───Scan Node (Container ID: [ID for table1], Filter: table1.b < 20, Projection: None)
    ///     └───Scan Node (Container ID: [ID for table2], Filter: table2.c = 30, Projection: None)
    /// ```
    #[test]
    fn test_complex_predicate_pushdown_with_multiple_cross_product() {
        let catalog = TestSetup::new_with_content().catalog;
        let c_id0 = catalog.get_table_id("table0");
        let c_id1 = catalog.get_table_id("table1");
        let c_id2 = catalog.get_table_id("table2");

        // Construct the logical plan
        let mut lp = LogicalPlan::new();
        let scan_idx0 = lp.add_scan_node(c_id0, None, None);
        let scan_idx1 = lp.add_scan_node(c_id1, None, None);
        let scan_idx2 = lp.add_scan_node(c_id2, None, None);

        let cross_product_idx1 = lp.add_cross_product_node(None, scan_idx0, scan_idx1);
        lp.add_cross_product_node(None, cross_product_idx1, scan_idx2);

        add_simple_filter_node(&mut lp, "table0.a", ">", "10", None);
        add_simple_filter_node(&mut lp, "table1.b", "<", "20", None);
        add_simple_filter_node(&mut lp, "table2.c", "==", "30", None);
        add_simple_filter_node(&mut lp, "table0.b", "!=", "table1.b", None);
        add_simple_filter_node(&mut lp, "table1.b", "!=", "table2.b", None);
        add_simple_filter_node(&mut lp, "table0.a", "==", "table1.a", None);
        add_simple_filter_node(&mut lp, "table0.a", "==", "table2.a", None);

        add_simple_projection_node(&mut lp, vec!["table0.a", "table1.b", "table2.c"], None);

        // Apply the predicate pushdown optimizer
        let optimized_lp =
            PredicatePushdown::pushdown(&lp, lp.root().unwrap(), &mut Vec::new(), &catalog);

        // Construct the expected logical plan after applying predicate pushdown
        let mut expected_lp = LogicalPlan::new();
        let expected_scan_idx2 = add_simple_scan_node(
            &mut expected_lp,
            c_id2,
            Some(vec![("table2.c", "==", "30")]),
            None,
        );
        let expected_scan_idx1 = add_simple_scan_node(
            &mut expected_lp,
            c_id1,
            Some(vec![("table1.b", "<", "20")]),
            None,
        );
        let expected_scan_idx0 = add_simple_scan_node(
            &mut expected_lp,
            c_id0,
            Some(vec![("table0.a", ">", "10")]),
            None,
        );

        let expected_join_idx1 = add_simple_join_node(
            &mut expected_lp,
            vec![("table1.a", "table0.a")],
            Some(vec![("table0.b", "!=", "table1.b")]),
            expected_scan_idx0,
            expected_scan_idx1,
        );
        add_simple_join_node(
            &mut expected_lp,
            vec![("table2.a", "table0.a")],
            Some(vec![("table1.b", "!=", "table2.b")]),
            expected_join_idx1,
            expected_scan_idx2,
        );
        add_simple_projection_node(
            &mut expected_lp,
            vec!["table0.a", "table1.b", "table2.c"],
            None,
        );

        // Compare the expected and actual logical plans
        assert_eq!(optimized_lp.to_json(), expected_lp.to_json());
    }

    /// Tests the predicate pushdown optimizer for a scenario with an aggregate function.
    ///
    /// ## Original SQL Query
    /// ```
    /// SELECT SUM(table0.a), AVG(table0.a), table0.b
    /// FROM table0
    /// WHERE table0.c > 0
    /// GROUP BY table0.b
    /// HAVING AVG(table0.a) < 10 AND table0.b >= 0;
    /// ```
    ///
    /// ### Original Plan Tree
    /// ```
    /// Project Node (Identifiers: ["SUM(table0.a)", "AVG(table0.a)", "table0.b"])
    /// └───Filter Node (Predicate: AVG(table0.a) < 10)
    ///     └───Filter Node (Predicate: table0.b >= 0)
    ///         └───Aggregate Node (Fields: [SUM(table0.a), AVG(table0.a)], Group By: [table0.b])
    ///             └───Filter Node (Predicate: table0.c > 0)
    ///                 └───Scan Node (Container ID: [ID for table0], Filter: None, Projection: None)
    /// ```
    ///
    /// ## Expected SQL Query After Predicate Pushdown
    /// The expected SQL after applying predicate pushdown optimization. The filters are pushed closer to the scan node and the aggregate node.
    /// ```
    /// SELECT SUM(table0.a), AVG(table0.a), table0.b
    /// FROM (
    ///     SELECT *
    ///     FROM table0
    ///     WHERE table0.c > 0 AND table0.b >= 0
    /// ) AS subtable0
    /// GROUP BY table0.b
    /// HAVING AVG(table0.a) < 10;
    /// ```
    ///
    /// ### Expected Plan Tree After Predicate Pushdown
    /// ```
    /// Project Node (Identifiers: ["SUM(table0.a)", "AVG(table0.a)", "table0.b"])
    /// └───Aggregate Node (Fields: [SUM(table0.a), AVG(table0.a)], Group By: [table0.b], Having: AVG(table0.a) < 10)
    ///     └───Scan Node (Container ID: [ID for table0], Filter: table0.c > 0 AND table0.b >= 0, Projection: None)
    /// ```
    /// This test demonstrates the optimizer's ability to push filters both below and above the aggregate node in the logical plan.
    #[test]
    fn test_predicate_pushdown_with_aggregate() {
        let catalog = TestSetup::new_with_content().catalog;
        let c_id0 = catalog.get_table_id("table0");

        // Construct the logical plan
        let mut lp = LogicalPlan::new();
        lp.add_scan_node(c_id0, None, None);
        add_simple_filter_node(&mut lp, "table0.c", ">", "0", None);
        lp.add_agg_node(
            vec![
                AstExpr::Agg(AggOp::Sum, Box::new(AstExpr::Ident("table0.a".to_string()))),
                AstExpr::Agg(AggOp::Avg, Box::new(AstExpr::Ident("table0.a".to_string()))),
            ],
            vec![AstExpr::Ident("table0.b".to_string())],
            None,
            None,
        );
        add_simple_filter_node(&mut lp, "AVG(table0.a)", "<", "10", None);
        add_simple_filter_node(&mut lp, "table0.b", ">=", "0", None);
        add_simple_projection_node(
            &mut lp,
            vec!["SUM(table0.a)", "AVG(table0.a)", "table0.b"],
            None,
        );

        // Apply the predicate pushdown optimizer
        let optimized_lp =
            PredicatePushdown::pushdown(&lp, lp.root().unwrap(), &mut Vec::new(), &catalog);

        // Construct the expected logical plan after applying predicate pushdown
        let mut expected_lp = LogicalPlan::new();
        add_simple_scan_node(
            &mut expected_lp,
            c_id0,
            Some(vec![("table0.b", ">=", "0"), ("table0.c", ">", "0")]),
            None,
        );
        expected_lp.add_agg_node(
            vec![
                AstExpr::Agg(AggOp::Sum, Box::new(AstExpr::Ident("table0.a".to_string()))),
                AstExpr::Agg(AggOp::Avg, Box::new(AstExpr::Ident("table0.a".to_string()))),
            ],
            vec![AstExpr::Ident("table0.b".to_string())],
            Some(AstExpr::Boolean(
                BooleanOp::Lt,
                Box::new(AstExpr::Ident("AVG(table0.a)".to_string())),
                Box::new(AstExpr::Literal(common::Field::Int(10))),
            )),
            None,
        );
        add_simple_projection_node(
            &mut expected_lp,
            vec!["SUM(table0.a)", "AVG(table0.a)", "table0.b"],
            None,
        );

        // Compare the expected and actual logical plans
        assert_eq!(optimized_lp.to_json(), expected_lp.to_json());
    }
}
