pub use executor::Executor;
pub use translate_and_validate::TranslateAndValidate;
pub use translate_and_validate::{get_attr, get_name};
mod executor;
pub mod planner;
mod translate_and_validate;

// Notes on Query Optimization
//    -- See the optimization-skeleton branch for an example
//       implementation of an optimization flow based on SimpleDb
//
// Query optimization would likely be done using a module whose
// functions are run in-between calls to TranslateAndValidate and
// Executor
// In server.rs calling the optimization functions might look something like this:
// server::DBServer::run_query() {
//       ...
//      Some(Some(db)) => {
//      let lp = TranslateAndValidate::from_sql(query, db)?;
//      let annotated_lp = Optimizer::new(lp, db);
//      Ok(Executor::new(db, &annotated_lp)?.execute())
//     ...
// }
//
// Putting optimization there would give optimization functions access to
// both the catalog and logical plan (currently a graph of logical relations).
//
// Actually writing the optimizer would likely involve processing a
// common-old::logical_plan::LogicalPlan. The function
// executor::Executor::logical_plan_to_op_iterator gives an example of
// recursively processing a logical plan graph.
//
// Alternatively, SimpleDb does query optimization by processing lists of logical
// operators.
//
// If the SimpleDB approach ends up being easier, the branch
// *** optimization-skeleton *** refactors the current flow so that
// TranslateAndValidate produces lists of logical operators, rather
// than a graph. Then, a separate function on the branch (inside
// optimizer.rs) turns the lists of operators into a graph, similar
// to the way SimipleDB's LogicalPlan.physicalPlan() function works.
//
// Note: the Executor expects a logical plan as input, so the
// optimizer would have to output a graph based annotated structure
// similar to a logical plan in order to process the plan using the
// current version of the executor. Otherwise, some of the executor
// may need to be rewritten to accommodate whatever new intermediate
// representation is chosen.  Hopefully, though, it will be easy to
// just create an annotated version of the LogicalPlan struct that the
// executor can process almost exactly like it currently processes
// LogicalPlans

/* FIXME
#[cfg(test)]
mod test {
    use super::*;
    use crate::bufferpool::*;
    use crate::catalog::Catalog;
    use crate::database::Database;
    use crate::table::*;
    use crate::testutil::test_util::*;
    use crate::DBSERVER;
    use common::{Attribute, CrustyError, DataType, Field, TableSchema, Tuple};
    use sqlparser::ast::{Query, Statement};
    use sqlparser::dialect::GenericDialect;
    use sqlparser::parser::Parser;
    use std::collections::HashMap;
    use std::sync::{Arc, Once, RwLock};

    static SETUP: Once = Once::new();
    // Test values used in executor and translate_and_validate
    pub static TABLE_A: &str = "table_a";
    pub const A_WIDTH: usize = 2;
    pub static A_COLS: [&str; A_WIDTH] = ["one", "two"];
    pub static TABLE_A_CHECKSUM: i32 = 36;

    pub static TABLE_B: &str = "table_b";
    pub static B_COLS: [&str; 4] = ["B1", "B2", "B3", "B4"];

    pub static TABLE_C: &str = "table_c";
    pub const C_WIDTH: usize = 3;
    pub static C_COLS: [&str; C_WIDTH] = ["one", "two", "three"];

    pub fn test_db() -> Database {
        let db = connect_to_test_db();
        SETUP.call_once(|| {
            let attributes = A_COLS
                .iter()
                .map(|n| Attribute::new(n.to_string(), DataType::Int))
                .collect();
            let schema = TableSchema::new(attributes);
            let rows = create_tuple_list(vec![vec![1, 2], vec![3, 4], vec![5, 6], vec![7, 8]]);

            create_test_table(TABLE_A, schema, rows);

            let attributes = vec![
                Attribute::new(B_COLS[0].to_string(), DataType::Int),
                Attribute::new(B_COLS[1].to_string(), DataType::Int),
                Attribute::new(B_COLS[2].to_string(), DataType::Int),
                Attribute::new(B_COLS[3].to_string(), DataType::String),
            ];
            let schema = TableSchema::new(attributes);
            let rows = vec![
                Tuple::new(vec![
                    Field::Int(1),
                    Field::Int(1),
                    Field::Int(3),
                    Field::String("E".to_string()),
                ]),
                Tuple::new(vec![
                    Field::Int(2),
                    Field::Int(1),
                    Field::Int(3),
                    Field::String("G".to_string()),
                ]),
                Tuple::new(vec![
                    Field::Int(3),
                    Field::Int(1),
                    Field::Int(4),
                    Field::String("A".to_string()),
                ]),
                Tuple::new(vec![
                    Field::Int(4),
                    Field::Int(2),
                    Field::Int(4),
                    Field::String("G".to_string()),
                ]),
                Tuple::new(vec![
                    Field::Int(5),
                    Field::Int(2),
                    Field::Int(5),
                    Field::String("G".to_string()),
                ]),
                Tuple::new(vec![
                    Field::Int(6),
                    Field::Int(2),
                    Field::Int(5),
                    Field::String("G".to_string()),
                ]),
            ];

            create_test_table(TABLE_B, schema, rows);

            let attributes = C_COLS
                .iter()
                .map(|n| Attribute::new(n.to_string(), DataType::Int))
                .collect();
            let schema = TableSchema::new(attributes);
            let rows = create_tuple_list(vec![
                vec![1, 2, 3],
                vec![2, 3, 4],
                vec![3, 4, 5],
                vec![4, 5, 6],
                vec![5, 6, 7],
            ]);

            create_test_table(TABLE_C, schema, rows);
        });
        db
    }

    /// Mock database that always returns true for is_valid_table and is_valid_column
    /// Used to test sql to logical plan translation separately from validation
    pub struct MockDb {}

    impl Catalog for MockDb {
        fn get_tables(&self) -> Arc<RwLock<HashMap<u64, Arc<RwLock<Table>>>>> {
            let hm: Arc<RwLock<HashMap<u64, Arc<RwLock<Table>>>>> =
                Arc::new(RwLock::new(HashMap::new()));
            hm.clone()
        }

        fn get_table_ptr(&self, _table_id: u64) -> Result<Arc<RwLock<Table>>, CrustyError> {
            panic!("Mock db does not implement get_table_ptr")
        }
        fn is_valid_table(&self, _table_id: u64) -> bool {
            true
        }
        fn is_valid_column(&self, _table_id: u64, _col_names: &str) -> bool {
            true
        }
        fn get_table_schema(&self, _table_id: u64) -> Result<TableSchema, CrustyError> {
            panic!("Mock db does not implement get_table_schema")
        }
    }

    pub fn get_select_ast(sql: &str) -> Query {
        // should panic if not a select, causing test to fail
        let dialect = GenericDialect {};
        let parsed_sql = Parser::parse_sql(&dialect, sql.to_string()).unwrap();
        let statement = parsed_sql.get(0).unwrap();
        if let Statement::Query(q) = statement {
            return *q.clone();
        }
        panic!("Not a select statement")
    }

    mod integration {
        use super::*;
        use crate::transactions::TransactionId;

        #[test]
        fn test_aliases() {
            let db = test_db();
            let new_names = ["alpha", "beta"];
            let sql = format!(
                "Select {} as {}, {} as {} from {}",
                B_COLS[0], new_names[0], B_COLS[1], new_names[1], TABLE_B
            );
            let ast = get_select_ast(&sql);
            let lp = TranslateAndValidate::from_sql(&ast, &db).unwrap();
            let tid = TransactionId::new();
            let executor = Executor::new(&db, &lp, tid).unwrap();
            let attributes = executor.table_schema().attributes();
            for (attr, name) in attributes.zip(&new_names) {
                assert_eq!(name.to_string(), attr.name().to_string());
            }
        }

        pub fn exec_to_vec(mut executor: Executor) -> Vec<Vec<Field>> {
            let mut rows = Vec::new();
            executor.start().unwrap();
            while let Some(t) = executor.next().unwrap() {
                rows.push(t.field_vals().map(|f| f.clone()).collect());
            }
            rows
        }

        pub fn match_tuples(sql: &str, expected: Vec<Tuple>) {
            let db = test_db();
            let ast = get_select_ast(&sql);
            let lp = TranslateAndValidate::from_sql(&ast, &db).unwrap();
            let tid = TransactionId::new();
            let mut executor = Executor::new(&db, &lp, tid).unwrap();
            let mut i = 0;

            executor.start().unwrap();
            while let Some(t) = executor.next().unwrap() {
                assert_eq!(expected[i], t);
                i += 1;
            }
            executor.close().unwrap();
            DBSERVER.transaction_complete(tid, true).unwrap();
        }

        #[test]
        fn test_aggregates() {
            let sql = format!(
                "Select count({}), avg({}), max({}), min({}), sum({}) from {}",
                B_COLS[3], B_COLS[0], B_COLS[0], B_COLS[0], B_COLS[1], TABLE_B
            );
            let expected = create_tuple_list(vec![vec![6, 3, 6, 1, 9]]);
            match_tuples(&sql, expected);
        }

        #[test]
        fn test_group_by() {
            let db = test_db();
            let sql = format!(
                "Select {gb}, count({}), sum({}) from {} group by {gb}",
                B_COLS[0],
                B_COLS[0],
                TABLE_B,
                gb = B_COLS[1]
            );
            let ast = get_select_ast(&sql);
            let lp = TranslateAndValidate::from_sql(&ast, &db).unwrap();
            let tid = TransactionId::new();
            let executor = Executor::new(&db, &lp, tid).unwrap();
            let mut result = exec_to_vec(executor);
            DBSERVER.transaction_complete(tid, true).unwrap();
            result.sort();

            let expected = vec![
                vec![Field::Int(1), Field::Int(3), Field::Int(6)],
                vec![Field::Int(2), Field::Int(3), Field::Int(15)],
            ];
            assert_eq!(expected, result);
        }

        #[test]
        fn test_join() {
            // 2-way join
            let sql = format!(
                "Select * from {table_a} join {table_c} on {table_c}.{} = {table_a}.{}",
                C_COLS[0],
                A_COLS[0],
                table_a = TABLE_A,
                table_c = TABLE_C,
            );

            let expected = create_tuple_list(vec![
                vec![1, 2, 1, 2, 3],
                vec![3, 4, 3, 4, 5],
                vec![5, 6, 5, 6, 7],
            ]);
            match_tuples(&sql, expected);

            // 3-way join
            let sql = format!(
                "Select {a_col}, {b_col}, {c_col} from {table_a} join {table_c} on {c_col} = {a_col} join {table_b} on {c_col} = {b_col}",
                a_col = format!("{}.{}", TABLE_A, A_COLS[0]),
                b_col = B_COLS[0],
                c_col = format!("{}.{}", TABLE_C, C_COLS[0]),
                table_a = TABLE_A,
                table_b = TABLE_B,
                table_c = TABLE_C
            );
            let expected = create_tuple_list(vec![vec![1, 1, 1], vec![3, 3, 3], vec![5, 5, 5]]);
            match_tuples(&sql, expected);
        }

        #[test]
        fn test_where() {
            // where col, constant
            let sql = format!("Select * from {} where {} = {}", TABLE_C, C_COLS[0], 3);
            let expected = create_tuple_list(vec![vec![3, 4, 5]]);
            match_tuples(&sql, expected);

            // where constant, col
            let sql = format!("Select * from {} where {} > {}", TABLE_C, 3, C_COLS[0]);
            let expected = create_tuple_list(vec![vec![1, 2, 3], vec![2, 3, 4]]);
            match_tuples(&sql, expected);

            // string field in predicate
            let sql = format!("Select * from {} where {} = '{}'", TABLE_B, B_COLS[3], "A");
            let expected = vec![Tuple::new(vec![
                Field::Int(3),
                Field::Int(1),
                Field::Int(4),
                Field::String("A".to_string()),
            ])];
            match_tuples(&sql, expected);
        }
    }
}
*/
