use common::ast_expr::AstExpr;
use common::catalog::CatalogRef;
use common::datatypes::{default_decimal_precision, default_decimal_scale};
use common::operation::{AggOp, BooleanOp, MathOp};
use common::prelude::{ContainerId, Field};
use common::{logical_plan::*, Attribute};
use common::{CrustyError, DataType};
use sqlparser::ast::{
    self, Assignment, BinaryOperator, ExactNumberInfo, Expr, FunctionArg, FunctionArgExpr,
    GroupByExpr, JoinConstraint, JoinOperator, SelectItem, SetExpr, TableFactor, Value,
};

/// Retrieve the name from the command parser object.
///
/// # Argument
///
/// * `name` - Name object from the command parser.
pub fn get_name(name: &ast::ObjectName) -> Result<String, CrustyError> {
    if name.0.len() > 1 {
        Err(CrustyError::CrustyError(String::from(
            "Error no . names supported",
        )))
    } else {
        Ok(name.0[0].value.clone())
    }
}

/// Retrieve the dtype from the command parser object.
///
/// # Argument
///
/// * `dtype` - Name object from the command parser.
pub fn get_attr(dtype: &ast::DataType) -> Result<DataType, CrustyError> {
    match dtype {
        ast::DataType::Int(_) => Ok(DataType::Int),
        ast::DataType::Varchar(_) => Ok(DataType::String),
        ast::DataType::Char(_) => Ok(DataType::String),
        ast::DataType::Date => Ok(DataType::Date),
        ast::DataType::Decimal(exact_num_info) => match exact_num_info {
            ExactNumberInfo::PrecisionAndScale(p, s) => Ok(DataType::Decimal(*p as u32, *s as u32)),
            ExactNumberInfo::Precision(p) => {
                Ok(DataType::Decimal(*p as u32, default_decimal_scale()))
            }
            ExactNumberInfo::None => Ok(DataType::Decimal(
                default_decimal_precision(),
                default_decimal_scale(),
            )),
        },
        _ => Err(CrustyError::CrustyError(format!(
            "Unsupported data type {:?}",
            dtype
        ))),
    }
}

/// Translates input to a LogicalPlan
/// Validates the columns and tables referenced using the catalog
/// Shares lifetime 'a with catalog
pub struct TranslateAndValidate {
    /// Logical plan of operators encountered so far.
    plan: LogicalPlan,
    /// Catalog to validate the translations.
    catalog: CatalogRef,
    /// List of tables encountered. Used for field validation.
    tables: Vec<String>,
}

impl TranslateAndValidate {
    /// Creates a new TranslateAndValidate object.
    fn new(catalog: &CatalogRef) -> Self {
        Self {
            plan: LogicalPlan::new(),
            catalog: catalog.clone(),
            tables: Vec::new(),
        }
    }

    /// Given a column name, try to figure out what table it belongs to by looking through all of the tables.
    ///
    /// # Arguments
    ///
    /// * `identifiers` - a list of elements in a multi-part identifier e.g. table.column would be vec!["table", "column"]
    ///
    /// # Returns
    ///
    /// FieldIdent's of the form { table: table, column: table.column, alias: column }
    /// or { table: table, column: table.column} if the full identifier is passed.
    fn disambiguate_name(&self, identifiers: Vec<&str>) -> Result<AstExpr, CrustyError> {
        match identifiers.len() {
            1 => {
                let column_name = identifiers[0];
                // If name is found in one of the tables, use "table_name.column_name"
                // Otherwise, use "column_name" (this is the case when column_name is an alias)
                for table in &self.tables {
                    let table_id = self.catalog.get_table_id(table);
                    if self
                        .catalog
                        .is_valid_column(table_id, &format!("{}.{}", table, column_name))
                    {
                        let name = format!("{}.{}", table, column_name);
                        return Ok(AstExpr::Ident(name));
                    }
                }
                Ok(AstExpr::Ident(column_name.to_string()))
            }
            2 => {
                let table_name = identifiers[0];
                let column_name = identifiers[1];
                let combined = format!("{}.{}", table_name, column_name);
                let table_id = self.catalog.get_table_id(table_name);
                if self.catalog.is_valid_column(table_id, &combined) {
                    Ok(AstExpr::Ident(combined))
                } else {
                    Err(CrustyError::CrustyError(
                        "Invalid table column name".to_string(),
                    ))
                }
            }
            _ => Err(CrustyError::CrustyError("Invalid identifiers".to_string())),
        }
    }

    /// Translates a sqlparser::ast to a LogicalPlan.
    ///
    /// Validates the columns and tables referenced using the catalog.
    /// All table names referenced in from and join clauses are added to self.tables.
    ///
    /// # Arguments
    ///
    /// * `sql` - AST to transalte.
    /// * `catalog` - Catalog for validation.
    pub fn from_sql(
        sql: &sqlparser::ast::Query,
        catalog: &CatalogRef,
    ) -> Result<LogicalPlan, CrustyError> {
        let mut translator = TranslateAndValidate::new(catalog);
        translator.process_query(sql)?;
        Ok(translator.plan)
    }

    pub fn from_update(
        table_id: ContainerId,
        table_name: &str,
        assignments: &[Assignment],
        selection: &Option<AstExpr>,
        catalog: &CatalogRef,
    ) -> Result<LogicalPlan, CrustyError> {
        let mut translator = TranslateAndValidate::new(catalog);
        translator.process_update(table_id, table_name, assignments, selection)?;
        Ok(translator.plan)
    }

    fn process_update(
        &mut self,
        _table_id: ContainerId,
        _table_name: &str,
        _assignments: &[Assignment],
        _selection: &Option<AstExpr>,
    ) -> Result<(), CrustyError> {
        unimplemented!()
    }

    fn get_all_attributes(&self, from: &Vec<ast::TableWithJoins>) -> Vec<Attribute> {
        let mut attrs = Vec::new();
        for sel in from {
            let table = match &sel.relation {
                TableFactor::Table { name, .. } => name,
                _ => {
                    panic!("Should not reach here");
                }
            };
            let table_name = get_name(table).unwrap();
            let table_id = self.catalog.get_table_id(&table_name);
            let table_schema = self.catalog.get_table_schema(table_id).unwrap();
            attrs.extend(table_schema.attributes.clone());

            for join in &sel.joins {
                let table = match &join.relation {
                    TableFactor::Table { name, .. } => name,
                    _ => {
                        panic!("Should not reach here");
                    }
                };
                let table_name = get_name(table).unwrap();
                let table_id = self.catalog.get_table_id(&table_name);
                let table_schema = self.catalog.get_table_schema(table_id).unwrap();
                attrs.extend(table_schema.attributes.clone());
            }
        }
        attrs
    }

    fn process_query(&mut self, query: &sqlparser::ast::Query) -> Result<(), CrustyError> {
        let select = match query.body.as_ref() {
            SetExpr::Select(b) => b,
            _ => {
                return Err(CrustyError::CrustyError(
                    "Unsupported query type".to_string(),
                ));
            }
        };
        /*
        Example of a query with multiple tables:
        ############################
        1 -- SELECT *
        2 -- FROM table1
        3 --     INNER JOIN table2 ON table1.a = table2.a
        4 --     INNER JOIN table3 ON table2.b = table3.b
        5 -- WHERE table1.a = 1
        6 --     AND table2.b = 2
        7 --     AND table3.c = 3
        8 -- GROUP BY table1.a, table2.b, table3.c
        9 -- HAVING table1.a = 1
        10 - ORDER BY table1.a, table2.b, table3.c
        ############################
        */

        // We translate the query into a logical plan in the following order:
        // First we process the FROM clause.
        self.process_from_clause(&select.from)?;

        // Then we process the WHERE clause.
        self.process_where_clause(&select.selection)?;

        // Then we process the SELECT clause
        // - We add a project node to the plan.
        // - We need to identify the SELECT clause contains aggregate functions.
        let mut select_fields = Vec::new();
        let mut has_agg = false;

        for proj in &select.projection {
            match proj {
                SelectItem::Wildcard(_) => {
                    // push all the fields in the table to the fields vector
                    let attrs = self.get_all_attributes(&select.from);
                    for attr in attrs {
                        select_fields.push(AstExpr::Ident(attr.name));
                    }
                }
                SelectItem::UnnamedExpr(expr) => {
                    let ast_expr = self.expr_to_astexpr(expr)?;
                    has_agg = has_agg || ast_expr.has_agg();
                    select_fields.push(ast_expr);
                }
                SelectItem::ExprWithAlias { expr, alias } => {
                    let ast_expr = self.expr_to_astexpr(expr)?;
                    let alias_expr = AstExpr::Alias(alias.to_string(), Box::new(ast_expr.clone()));
                    has_agg = has_agg || alias_expr.has_agg();
                    select_fields.push(alias_expr);
                }
                _ => {
                    return Err(CrustyError::CrustyError(
                        "Unsupported select item".to_string(),
                    ));
                }
            }
        }

        // Before we add the projection node to the plan, we need to check if the
        // select fields contains all the fields in the order by clause.
        // If not, we need to project it with the fields in the order by clause.
        // After sorting, we add another projection node to the plan to remove the
        // fields in the order by clause.
        let mut order_by_fields = Vec::new();
        for orderby in &query.order_by {
            order_by_fields.push(self.expr_to_astexpr(&orderby.expr)?);
        }

        let mut fields = select_fields.clone();
        let mut order_by_added = false;
        for order_by_field in order_by_fields {
            let mut found = false;
            for field in &fields {
                if field.to_name() == order_by_field.to_name() {
                    // found the field in the select clause.
                    found = true;
                    break;
                }
            }
            if !found {
                // add the field to the select clause
                fields.push(order_by_field.clone());
                order_by_added = true;
            }
        }

        if has_agg {
            let mut group_by_expr = Vec::new();
            match &select.group_by {
                GroupByExpr::All => {
                    unimplemented!()
                }
                GroupByExpr::Expressions(exprs) => {
                    for expr in exprs {
                        group_by_expr.push(self.expr_to_astexpr(expr)?);
                    }
                }
            }
            let mut agg_expr = Vec::new();
            for expr in &mut fields {
                if expr.has_agg() {
                    Self::seperate_agg(expr, &mut agg_expr);
                } else {
                    // If the expression in the select clause is not an aggregate function,
                    // it should be either a literal or a expression over aggregate columns
                    // and group by columns.
                    // For example, if a query looks like:
                    // SELECT a + SUM(b), 1 FROM example GROUP BY a
                    // Then, the aggregation should return two columns: a, SUM(b)
                    // The following projection node will compute "a" + "SUM(b)" and 1.
                    // Hence, we don't need to do anything here.
                }
            }

            // Add aggregation node to the plan with keys in the group by clause
            // Output of the aggregation node contains the group by columns and the aggregate columns
            self.process_aggregate(group_by_expr, agg_expr)?;
            self.process_where_clause(&select.having)?;
        }

        // This projection will project the fields in the select clause or order by clause.
        // If the select clause contains aggregate functions, then the projection
        // will get rid of the group by columns not in the select clause because
        // the aggregation node will output the group by columns.
        // This projection also serves to compute expressions over aggregate columns.
        // For example, if a query looks like:
        // SELECT col1, SUM(col2) + SUM(col3) FROM table GROUP BY col1, col4
        // Then, aggregation node will output: col1, col4, SUM(col2), SUM(col3)
        // The projection node will output: col1, "SUM(col2)" + "SUM(col3)"
        self.process_projection(fields)?;

        // Add sort node to the plan
        let mut orderby_fields = Vec::new();
        for orderby in &query.order_by {
            let expr = AstExpr::Ident(self.expr_to_astexpr(&orderby.expr)?.to_name());
            let asc = match orderby.asc {
                Some(true) => true,
                Some(false) => false,
                None => true,
            };
            orderby_fields.push((expr, asc));
        }
        if !orderby_fields.is_empty() {
            // Add sort node to the top of the plan
            let op = SortNode {
                fields: orderby_fields,
            };
            let idx = self.plan.add_node(LogicalOp::Sort(op));
            self.plan
                .add_edge(idx, self.plan.root().expect("root should exist"));
        }

        // Add projection node to the plan to remove the fields in the order by clause
        if order_by_added {
            let final_projection_fields = select_fields
                .iter()
                .map(|f| AstExpr::Ident(f.to_name()))
                .collect();
            self.process_projection(final_projection_fields)?;
        }

        // check distinct
        if select.distinct.is_some() {
            unimplemented!()
        }

        Ok(())
    }

    // This function separates the aggregate expressions in an expression tree
    // and puts them in a vector. Aggregates are replaced with ColumnIdentifiers.
    // For example, if the expression tree is:
    //               +
    //            /     \
    //           /       \
    //      SUM(col1)  SUM(col2)
    // Then the vector will contain: [SUM(col1), SUM(col2)]
    // and the expression tree will be modified to:
    //               +
    //            /      \
    //           /        \
    //     "SUM(col1)"   "SUM(col2)"
    // The difference between the two trees is that the first tree contains
    // AstExpr::Agg nodes while the second tree contains AstExpr::Ident nodes
    // with the name of the aggregate function.
    fn seperate_agg(expr: &mut AstExpr, vec: &mut Vec<AstExpr>) {
        match expr {
            AstExpr::Agg(_, _) => {
                // In case where root of the expr is Agg
                vec.push(expr.clone());
                *expr = AstExpr::Ident(expr.to_name());
            }
            AstExpr::Math(_, l, r) | AstExpr::Boolean(_, l, r) => {
                Self::seperate_agg(l, vec);
                Self::seperate_agg(r, vec);
            }
            AstExpr::Alias(_, e) => Self::seperate_agg(e, vec),
            AstExpr::Ident(_) | AstExpr::ColIdx(_) | AstExpr::Literal(_) => {}
        }
    }

    fn process_from_clause(&mut self, from: &Vec<ast::TableWithJoins>) -> Result<(), CrustyError> {
        // - We identify the first table in the FROM clause as the left-most table.
        // - We then process the join clause and add the join node to the plan.
        let mut nodes: Vec<OpIndex> = Vec::new();

        for sel in from {
            let mut node = self.process_table_factor(&sel.relation)?;
            for join in &sel.joins {
                let join_node = self.process_join(join, node)?;
                node = join_node;
            }
            nodes.push(node);
        }

        // Handling cross product
        let _cross_product_node = self.process_cross_product(&nodes)?;

        Ok(())
    }

    fn process_cross_product(&mut self, nodes: &Vec<OpIndex>) -> Result<OpIndex, CrustyError> {
        if nodes.is_empty() {
            return self
                .plan
                .root()
                .ok_or(CrustyError::CrustyError("No root found".to_string()));
        }

        let mut left_node_idx = nodes[0];

        for node_idx in nodes.iter().skip(1) {
            let right_node_idx = *node_idx;

            let cross_product_node = LogicalOp::CrossProduct(CrossProductNode { filter: None });

            let idx = self.plan.add_node(cross_product_node);
            self.plan.add_edge(idx, left_node_idx);
            self.plan.add_edge(idx, right_node_idx);

            left_node_idx = idx;
        }

        Ok(left_node_idx)
    }

    fn process_where_clause(&mut self, where_clause: &Option<Expr>) -> Result<(), CrustyError> {
        // - We add a filter node to the plan.
        // - The filter node will be the parent of the current root.
        if let Some(expr) = where_clause {
            // identify the root node and schema
            let root = self.plan.root().unwrap();
            // convert the expression to a logical expression
            let ast_expr = self.expr_to_astexpr(expr)?;
            // create a filter node and add it to the plan
            let op = FilterNode {
                predicate: ast_expr,
            };
            let idx = self.plan.add_node(LogicalOp::Filter(op));
            // add an edge from the filter node to the root node
            self.plan.add_edge(idx, root);
        }
        Ok(())
    }

    fn process_aggregate(
        &mut self,
        group_by: Vec<AstExpr>,
        fields: Vec<AstExpr>,
    ) -> Result<(), CrustyError> {
        let root = self.plan.root().unwrap();

        let op: AggregateNode = AggregateNode {
            fields,
            group_by,
            having: None,
        };

        // Add aggregate node to the plan
        let idx = self.plan.add_node(LogicalOp::Aggregate(op));
        self.plan.add_edge(idx, root);

        Ok(())
    }

    fn process_projection(&mut self, proj: Vec<AstExpr>) -> Result<(), CrustyError> {
        let root = self.plan.root().unwrap();
        let op = ProjectNode { identifiers: proj };
        let idx = self.plan.add_node(LogicalOp::Project(op));
        self.plan.add_edge(idx, root);
        Ok(())
    }

    /// Creates a corresponding LogicalOp, adds it to self.plan, and returns the OpIndex.
    ///
    /// Helper function to process sqlparser::ast::TableFactor.
    ///
    /// # Arguments
    ///
    /// * `tf` - Table to process.
    fn process_table_factor(
        &mut self,
        tf: &sqlparser::ast::TableFactor,
    ) -> Result<OpIndex, CrustyError> {
        match tf {
            TableFactor::Table { name, .. } => {
                let name = get_name(name)?;
                let table_id = self.catalog.get_table_id(&name);
                if !self.catalog.is_valid_table(table_id) {
                    return Err(CrustyError::ValidationError(String::from(
                        "Invalid table name",
                    )));
                }
                self.tables.push(name.clone());
                let op = ScanNode {
                    container_id: table_id,
                    filter: None,
                    projection: None,
                };
                Ok(self.plan.add_node(LogicalOp::Scan(op)))
            }
            _ => Err(CrustyError::ValidationError(String::from(
                "Nested joins and derived tables not supported",
            ))),
        }
    }

    /// Parses sqlparser::ast::Join into a Join LogicalOp, adds the Op to
    /// logical plan, and returns OpIndex of the join node.
    ///
    /// # Arguments
    ///
    /// * `join` - The join node to parse.
    /// * `left_table_node` - Node containing the left table to join.
    fn process_join(
        &mut self,
        join: &sqlparser::ast::Join,
        left_node_idx: OpIndex,
    ) -> Result<OpIndex, CrustyError> {
        let right_node_idx = self.process_table_factor(&join.relation)?;
        let jc = match &join.join_operator {
            JoinOperator::Inner(jc) => jc,
            _ => {
                return Err(CrustyError::CrustyError(
                    "Unsupported join type".to_string(),
                ))
            }
        };

        if let JoinConstraint::On(expr) = jc {
            let ast_expr = self.expr_to_astexpr(expr)?;
            let join_node = match &ast_expr {
                AstExpr::Boolean(BooleanOp::Eq, l_expr, r_expr) => {
                    // TODO: (jun) No need this match after apply the predicate pushdown
                    LogicalOp::Join(JoinNode {
                        eqs: vec![(*l_expr.clone(), *r_expr.clone())],
                        filter: None,
                    })
                }
                _ => LogicalOp::Join(JoinNode {
                    eqs: vec![],
                    filter: Some(ast_expr),
                }),
            };
            let idx = self.plan.add_node(join_node);
            self.plan.add_edge(idx, left_node_idx);
            self.plan.add_edge(idx, right_node_idx);
            Ok(idx)
        } else {
            Err(CrustyError::CrustyError(
                "Unsupported join constraint".to_string(),
            ))
        }
    }

    /// Converts a sqparser::ast::Expr to a AstExpr.
    ///
    /// # Arguments
    ///
    /// * `ast` - Expression to be converted.
    fn expr_to_astexpr(&self, ast: &Expr) -> Result<AstExpr, CrustyError> {
        match ast {
            Expr::Identifier(name) => self.disambiguate_name(vec![&name.value]),
            Expr::CompoundIdentifier(names) => {
                self.disambiguate_name(names.iter().map(|s| s.value.as_str()).collect())
            }
            Expr::Value(value) => self.value_to_astexpr(value),
            Expr::TypedString { data_type, value } => {
                self.typed_string_to_astexpr(data_type, value)
            }
            Expr::BinaryOp { left, op, right } => self.binary_ast_to_astexpr(left, op, right),
            Expr::Function(fun) => self.function_ast_to_astexpr(fun),
            Expr::Nested(expr) => self.expr_to_astexpr(expr),
            _ => Err(CrustyError::CrustyError(format!(
                "Unsupported expression: {:?}",
                ast
            ))),
        }
    }

    fn value_to_astexpr(&self, value: &Value) -> Result<AstExpr, CrustyError> {
        match value {
            Value::Number(field, _long) => {
                // Determine if n is an integer or a float
                let parts = field.split('.').collect::<Vec<&str>>();
                match parts.len() {
                    1 => Ok(AstExpr::Literal(Field::from_str_to_int(field)?)),
                    2 => Ok(AstExpr::Literal(Field::from_str_to_decimal(
                        field,
                        default_decimal_precision(),
                        default_decimal_scale(),
                    )?)),
                    _ => Err(CrustyError::ValidationError(format!(
                        "Invalid number: {:?}",
                        value
                    ))),
                }
            }
            Value::SingleQuotedString(s) | Value::DoubleQuotedString(s) => {
                Ok(AstExpr::Literal(Field::String(s.clone())))
            }
            Value::Boolean(b) => Ok(AstExpr::Literal(Field::Bool(*b))),
            Value::Null => Ok(AstExpr::Literal(Field::Null)),
            _ => Err(CrustyError::ValidationError(format!(
                "Unsupported value type: {:?}",
                value
            ))),
        }
    }

    fn typed_string_to_astexpr(
        &self,
        data_type: &ast::DataType,
        value: &str,
    ) -> Result<AstExpr, CrustyError> {
        let dtype = get_attr(data_type)?;
        match dtype {
            DataType::Int => Ok(AstExpr::Literal(Field::from_str_to_int(value)?)),
            DataType::String => Ok(AstExpr::Literal(Field::String(value.to_string()))),
            DataType::Date => Ok(AstExpr::Literal(Field::from_str_to_date(value)?)),
            DataType::Decimal(p, s) => {
                Ok(AstExpr::Literal(Field::from_str_to_decimal(value, p, s)?))
            }
            DataType::Bool => Ok(AstExpr::Literal(Field::from_str_to_bool(value)?)),
            _ => Err(CrustyError::ValidationError(format!(
                "Unsupported data type: {:?}",
                dtype
            ))),
        }
    }

    fn binary_ast_to_astexpr(
        &self,
        left: &Expr,
        op: &BinaryOperator,
        right: &Expr,
    ) -> Result<AstExpr, CrustyError> {
        use BinaryOperator as AstOp;

        let left_expr = self.expr_to_astexpr(left)?;
        let right_expr = self.expr_to_astexpr(right)?;

        match op {
            AstOp::Plus | AstOp::Minus | AstOp::Multiply | AstOp::Divide => {
                let math_op = match op {
                    AstOp::Plus => MathOp::Add,
                    AstOp::Minus => MathOp::Sub,
                    AstOp::Multiply => MathOp::Mul,
                    AstOp::Divide => MathOp::Div,
                    _ => {
                        panic!("Should not reach here")
                    }
                };
                Ok(AstExpr::Math(
                    math_op,
                    Box::new(left_expr),
                    Box::new(right_expr),
                ))
            }
            AstOp::Eq | AstOp::NotEq | AstOp::Gt | AstOp::GtEq | AstOp::Lt | AstOp::LtEq => {
                let boolean_op = match op {
                    AstOp::Eq => BooleanOp::Eq,
                    AstOp::NotEq => BooleanOp::Neq,
                    AstOp::Gt => BooleanOp::Gt,
                    AstOp::GtEq => BooleanOp::Gte,
                    AstOp::Lt => BooleanOp::Lt,
                    AstOp::LtEq => BooleanOp::Lte,
                    _ => {
                        panic!("Should not reach here")
                    }
                };
                Ok(AstExpr::Boolean(
                    boolean_op,
                    Box::new(left_expr),
                    Box::new(right_expr),
                ))
            }
            AstOp::And | AstOp::Or => {
                let boolean_op = match op {
                    AstOp::And => BooleanOp::And,
                    AstOp::Or => BooleanOp::Or,
                    _ => {
                        panic!("Should not reach here")
                    }
                };
                Ok(AstExpr::Boolean(
                    boolean_op,
                    Box::new(left_expr),
                    Box::new(right_expr),
                ))
            }
            _ => Err(CrustyError::ValidationError(format!(
                "Unsupported binary operator: {:?}",
                op
            ))),
        }
    }

    fn function_ast_to_astexpr(&self, function: &ast::Function) -> Result<AstExpr, CrustyError> {
        let agg_op = match &get_name(&function.name)?.to_lowercase()[..] {
            "count" => Ok(AggOp::Count),
            "sum" => Ok(AggOp::Sum),
            "avg" => Ok(AggOp::Avg),
            "min" => Ok(AggOp::Min),
            "max" => Ok(AggOp::Max),
            _ => Err(CrustyError::ValidationError(format!(
                "Unsupported function: {}",
                function.name
            ))),
        }?;

        if function.args.len() != 1 {
            return Err(CrustyError::ValidationError(format!(
                "Function {} expects exactly one argument, got {}",
                function.name,
                function.args.len()
            )));
        }

        let function_arg_expr = match &function.args[0] {
            FunctionArg::Named { arg, .. } => arg,
            FunctionArg::Unnamed(expr) => expr,
        };

        let arg_expr = match function_arg_expr {
            FunctionArgExpr::Expr(expr) => expr,
            FunctionArgExpr::QualifiedWildcard(_) => {
                unimplemented!()
            }
            FunctionArgExpr::Wildcard => {
                if matches!(agg_op, AggOp::Count) {
                    // COUNT(*) will return the number of rows in the table
                    // and is equivalent to COUNT(1). So we return AstExpr::Literal(Field::Int(1)) here.
                    return Ok(AstExpr::Agg(
                        agg_op,
                        Box::new(AstExpr::Literal(Field::Int(1))),
                    ));
                } else {
                    return Err(CrustyError::ValidationError(format!(
                        "Function {} does not support wildcard",
                        function.name
                    )));
                }
            }
        };

        let arg_expr = self.expr_to_astexpr(arg_expr)?;

        Ok(AstExpr::Agg(agg_op, Box::new(arg_expr)))
    }
}
