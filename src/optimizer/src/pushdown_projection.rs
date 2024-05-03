use std::collections::BTreeSet;

use common::ast_expr::AstExpr;
use common::catalog::CatalogRef;
use common::logical_plan::LogicalOp;
use common::logical_plan::LogicalPlan;
use common::logical_plan::ScanNode;
use common::CrustyError;

use crate::optimizer::OptimizerRule;
use crate::optimizer::{extract_columns, extract_columns_vec};

pub struct ProjectionPushdown {}

impl ProjectionPushdown {
    fn pushdown(
        lp: &LogicalPlan,
        start: usize,
        col_names: &mut BTreeSet<String>,
        catalog: &CatalogRef,
    ) -> LogicalPlan {
        let op = lp.get_operator(start).unwrap();
        match op {
            LogicalOp::Project(p) => {
                extract_columns_vec(&p.identifiers, col_names);
                let child = lp.edges(start).next().unwrap();
                let mut new_lp = Self::pushdown(lp, child, col_names, catalog);
                let project_node = LogicalOp::Project(p.clone());
                let project_idx = new_lp.add_node(project_node);
                let old_root = new_lp.root().unwrap();
                new_lp.add_edge(project_idx, old_root);
                new_lp
            }
            LogicalOp::Filter(f) => {
                extract_columns(&f.predicate, col_names);
                let child = lp.edges(start).next().unwrap();
                let mut new_lp = Self::pushdown(lp, child, col_names, catalog);
                let filter_node = LogicalOp::Filter(f.clone());
                let filter_idx = new_lp.add_node(filter_node);
                let old_root = new_lp.root().unwrap();
                new_lp.add_edge(filter_idx, old_root);
                new_lp
            }
            LogicalOp::Aggregate(a) => {
                extract_columns_vec(&a.fields, col_names);
                extract_columns_vec(&a.group_by, col_names);
                let child = lp.edges(start).next().unwrap();
                let mut new_lp = Self::pushdown(lp, child, col_names, catalog);
                let agg_node = LogicalOp::Aggregate(a.clone());
                let agg_idx = new_lp.add_node(agg_node);
                let old_root = new_lp.root().unwrap();
                new_lp.add_edge(agg_idx, old_root);
                new_lp
            }
            LogicalOp::Join(j) => {
                for (l, r) in j.eqs.iter() {
                    extract_columns(l, col_names);
                    extract_columns(r, col_names);
                }
                if let Some(filter) = &j.filter {
                    extract_columns(filter, col_names);
                }
                let left = lp.edges(start).next().unwrap();
                let right = lp.edges(start).nth(1).unwrap();
                let mut new_lp_l = Self::pushdown(lp, left, col_names, catalog);
                let new_lp_r = Self::pushdown(lp, right, col_names, catalog);
                let join_node = LogicalOp::Join(j.clone());
                new_lp_l.merge(join_node, new_lp_r);
                new_lp_l
            }
            LogicalOp::CrossProduct(cp) => {
                let left = lp.edges(start).next().unwrap();
                let right = lp.edges(start).nth(1).unwrap();
                let mut new_lp_l = Self::pushdown(lp, left, col_names, catalog);
                let new_lp_r = Self::pushdown(lp, right, col_names, catalog);
                let cp_node = LogicalOp::CrossProduct(cp.clone());
                new_lp_l.merge(cp_node, new_lp_r);
                new_lp_l
            }
            LogicalOp::Sort(s) => {
                extract_columns_vec(
                    &s.fields.iter().map(|(f, _asc)| f.clone()).collect(),
                    col_names,
                );
                let child = lp.edges(start).next().unwrap();
                let mut new_lp = Self::pushdown(lp, child, col_names, catalog);
                let sort_node = LogicalOp::Sort(s.clone());
                let sort_idx = new_lp.add_node(sort_node);
                let old_root = new_lp.root().unwrap();
                new_lp.add_edge(sort_idx, old_root);
                new_lp
            }
            LogicalOp::Scan(s) => {
                let c_id = s.container_id;
                let schema = catalog.get_table_schema(c_id).unwrap();
                // Columns that are needed are passed in as col_names.
                // col_names might contain columns that are not derived from this scan
                // due to projection pushdown of JOINs. We need to filter those out.
                // We can ignore the current projection if there is a projection
                let mut new_projection = Vec::new();
                for col in col_names.iter() {
                    if schema.contains(col) {
                        new_projection.push(AstExpr::Ident(col.clone()));
                    }
                }
                let scan_node = LogicalOp::Scan(ScanNode {
                    container_id: c_id,
                    filter: s.filter.clone(),
                    projection: Some(new_projection),
                });
                let mut new_lp = LogicalPlan::new();
                let _ = new_lp.add_node(scan_node);
                new_lp
            }
            LogicalOp::Update(_) | LogicalOp::ReadDeltas(_) | LogicalOp::WriteDeltas(_) => {
                unimplemented!()
            }
        }
    }
}

impl OptimizerRule for ProjectionPushdown {
    fn apply(&self, plan: LogicalPlan, catalog: &CatalogRef) -> Result<LogicalPlan, CrustyError> {
        let mut col_names = BTreeSet::new();
        let new_plan = Self::pushdown(&plan, plan.root().unwrap(), &mut col_names, catalog);
        Ok(new_plan)
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use common::{
        logical_plan::{FilterNode, JoinNode, ProjectNode},
        Field,
    };
    use queryexe::testutil::TestSetup;

    #[test]
    fn test_pushdown_projection_without_join() {
        let catalog = TestSetup::new_with_content().catalog;
        let c_id = catalog.get_table_id("table0");

        let mut lp = LogicalPlan::new();

        let scan = LogicalOp::Scan(ScanNode {
            container_id: c_id,
            filter: None,
            projection: None,
        });
        let scan_idx = lp.add_node(scan);

        let filter = LogicalOp::Filter(FilterNode {
            predicate: AstExpr::Boolean(
                common::BooleanOp::Gte,
                Box::new(AstExpr::Ident("table0.a".to_string())),
                Box::new(AstExpr::Literal(Field::Int(1))),
            ),
        });
        let filter_idx = lp.add_node(filter);
        lp.add_edge(filter_idx, scan_idx);

        let projection = LogicalOp::Project(ProjectNode {
            identifiers: vec![
                AstExpr::Ident("table0.a".to_string()),
                AstExpr::Ident("table0.b".to_string()),
            ],
        });
        let projection_idx = lp.add_node(projection);
        lp.add_edge(projection_idx, filter_idx);

        let new_lp =
            ProjectionPushdown::pushdown(&lp, lp.root().unwrap(), &mut BTreeSet::new(), &catalog);

        let mut expected = LogicalPlan::new();
        let scan = LogicalOp::Scan(ScanNode {
            container_id: 0,
            filter: None,
            projection: Some(vec![
                AstExpr::Ident("table0.a".to_string()),
                AstExpr::Ident("table0.b".to_string()),
            ]),
        });
        let scan_idx = expected.add_node(scan);

        let filter = LogicalOp::Filter(FilterNode {
            predicate: AstExpr::Boolean(
                common::BooleanOp::Gte,
                Box::new(AstExpr::Ident("table0.a".to_string())),
                Box::new(AstExpr::Literal(Field::Int(1))),
            ),
        });
        let filter_idx = expected.add_node(filter);
        expected.add_edge(filter_idx, scan_idx);

        let projection = LogicalOp::Project(ProjectNode {
            identifiers: vec![
                AstExpr::Ident("table0.a".to_string()),
                AstExpr::Ident("table0.b".to_string()),
            ],
        });
        let projection_idx = expected.add_node(projection);
        expected.add_edge(projection_idx, filter_idx);

        assert_eq!(new_lp.to_json(), expected.to_json());
    }

    #[test]
    fn test_pushdown_projection_with_join() {
        let catalog = TestSetup::new_with_content().catalog;
        let c_id0 = catalog.get_table_id("table0");
        let c_id1 = catalog.get_table_id("table1");

        let mut lp = LogicalPlan::new();
        let scan0 = LogicalOp::Scan(ScanNode {
            container_id: c_id0,
            filter: None,
            projection: None,
        });
        let scan0_idx = lp.add_node(scan0);
        let scan1 = LogicalOp::Scan(ScanNode {
            container_id: c_id1,
            filter: None,
            projection: None,
        });
        let scan1_idx = lp.add_node(scan1);

        let join = LogicalOp::Join(JoinNode {
            eqs: vec![(
                AstExpr::Ident("table0.a".to_string()),
                AstExpr::Ident("table1.a".to_string()),
            )],
            filter: None,
        });
        let join_idx = lp.add_node(join);
        // Right table is always the first child to add
        lp.add_edge(join_idx, scan1_idx);
        lp.add_edge(join_idx, scan0_idx);

        let projection = LogicalOp::Project(ProjectNode {
            identifiers: vec![
                AstExpr::Ident("table0.a".to_string()),
                AstExpr::Ident("table0.b".to_string()),
                AstExpr::Ident("table1.a".to_string()),
                AstExpr::Ident("table1.b".to_string()),
            ],
        });

        let projection_idx = lp.add_node(projection);
        lp.add_edge(projection_idx, join_idx);

        let new_lp =
            ProjectionPushdown::pushdown(&lp, lp.root().unwrap(), &mut BTreeSet::new(), &catalog);

        let mut expected = LogicalPlan::new();
        let scan0 = LogicalOp::Scan(ScanNode {
            container_id: c_id0,
            filter: None,
            projection: Some(vec![
                AstExpr::Ident("table0.a".to_string()),
                AstExpr::Ident("table0.b".to_string()),
            ]),
        });
        let scan0_idx = expected.add_node(scan0);
        let scan1 = LogicalOp::Scan(ScanNode {
            container_id: c_id1,
            filter: None,
            projection: Some(vec![
                AstExpr::Ident("table1.a".to_string()),
                AstExpr::Ident("table1.b".to_string()),
            ]),
        });
        let scan1_idx = expected.add_node(scan1);

        let join = LogicalOp::Join(JoinNode {
            eqs: vec![(
                AstExpr::Ident("table0.a".to_string()),
                AstExpr::Ident("table1.a".to_string()),
            )],
            filter: None,
        });
        let join_idx = expected.add_node(join);
        // Right table is always the first child to add
        expected.add_edge(join_idx, scan1_idx);
        expected.add_edge(join_idx, scan0_idx);

        let projection = LogicalOp::Project(ProjectNode {
            identifiers: vec![
                AstExpr::Ident("table0.a".to_string()),
                AstExpr::Ident("table0.b".to_string()),
                AstExpr::Ident("table1.a".to_string()),
                AstExpr::Ident("table1.b".to_string()),
            ],
        });

        let projection_idx = expected.add_node(projection);
        expected.add_edge(projection_idx, join_idx);

        assert_eq!(new_lp.to_json(), expected.to_json());
    }
}
