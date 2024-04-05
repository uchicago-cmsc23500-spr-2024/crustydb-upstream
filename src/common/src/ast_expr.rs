use crate::datatypes::{default_decimal_precision, default_decimal_scale};
use crate::operation::{AggOp, BooleanOp, MathOp};
use crate::Attribute;
use crate::CrustyError;
use crate::DataType;
use crate::{Field, TableSchema};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum AstExpr {
    Literal(Field),
    Ident(String),
    Alias(String, Box<AstExpr>),
    Math(MathOp, Box<AstExpr>, Box<AstExpr>),
    Boolean(BooleanOp, Box<AstExpr>, Box<AstExpr>),
    Agg(AggOp, Box<AstExpr>),
    ColIdx(usize), // Used after binding. See `bind_expr`.
}

impl AstExpr {
    pub fn has_agg(&self) -> bool {
        match self {
            AstExpr::Literal(_) => false,
            AstExpr::Ident(_) => false,
            AstExpr::Alias(_, expr) => expr.has_agg(),
            AstExpr::Math(_, left, right) => left.has_agg() || right.has_agg(),
            AstExpr::Boolean(_, left, right) => left.has_agg() || right.has_agg(),
            AstExpr::Agg(_, _) => true,
            AstExpr::ColIdx(_) => false,
        }
    }

    pub fn is_agg(&self) -> bool {
        matches!(self, AstExpr::Agg(_, _))
    }

    pub fn to_attr(&self, schema: &TableSchema) -> Attribute {
        use AstExpr::*;
        match self {
            Literal(val) => {
                let s = self.to_name();
                match val {
                    Field::Int(_) => Attribute::new(s, DataType::Int),
                    Field::String(_) => Attribute::new(s, DataType::String),
                    Field::Decimal(_, _) => Attribute::new(
                        s,
                        DataType::Decimal(default_decimal_precision(), default_decimal_scale()),
                    ), // Placeholder values, update as needed
                    Field::Date(_) => Attribute::new(s, DataType::Date),
                    Field::Bool(_) => Attribute::new(s, DataType::Bool),
                    Field::Null => Attribute::new(s, DataType::Null),
                }
            }
            Ident(name) => {
                let i = schema.get_field_index(name).unwrap_or_else(|| {
                    panic!("Identifier {} not found in schema {:?}", name, schema)
                });
                schema.get_attribute(i).unwrap().clone()
            }
            Alias(name, e) => {
                let a = e.to_attr(schema);
                Attribute::new(name.clone(), a.dtype.clone())
            }
            Math(op, l, r) => {
                let l = l.to_attr(schema);
                let r = r.to_attr(schema);
                Attribute::new(format!("({} {} {})", l.name, op, r.name), l.dtype.clone())
            }
            Boolean(op, l, r) => {
                let l = l.to_attr(schema);
                let r = r.to_attr(schema);
                Attribute::new(format!("({} {} {})", l.name, op, r.name), DataType::Bool)
            }
            Agg(op, e) => {
                let a = e.to_attr(schema);
                match op {
                    AggOp::Avg => {
                        if matches!(&a.dtype, DataType::Decimal(_, _)) {
                            Attribute::new(format!("{}({})", op, a.name), a.dtype.clone())
                        } else {
                            Attribute::new(
                                format!("{}({})", op, a.name),
                                DataType::Decimal(
                                    default_decimal_precision(),
                                    default_decimal_scale(),
                                ),
                            )
                        }
                    }
                    _ => Attribute::new(format!("{}({})", op, a.name), a.dtype.clone()),
                }
            }
            ColIdx(i) => schema.get_attribute(*i).unwrap().clone(),
        }
    }

    pub fn to_name(&self) -> String {
        use AstExpr::*;
        match self {
            Literal(val) => format!("Literal({})", val),
            Ident(name) => name.clone(),
            Alias(name, _) => name.clone(),
            Math(op, l, r) => format!("({} {} {})", l.to_name(), op, r.to_name()),
            Boolean(op, l, r) => format!("({} {} {})", l.to_name(), op, r.to_name()),
            Agg(op, e) => format!("{}({})", op, e.to_name()),
            ColIdx(_i) => panic!("ColIdx should used after bind"),
        }
    }
}

// Transform all the identifiers in an expression to column indexes
// Also remove any aliases
pub fn bind_expr(ast: AstExpr, schema: &TableSchema) -> Result<AstExpr, CrustyError> {
    match ast {
        AstExpr::Agg(_, _) => {
            panic!("Should be removed when converting to logical plan");
        }
        AstExpr::Literal(_) => Ok(ast),
        AstExpr::Ident(name) => {
            let i = schema
                .get_field_index(&name)
                .unwrap_or_else(|| panic!("Identifier {} not found in schema {:?}", name, schema));
            Ok(AstExpr::ColIdx(i))
        }
        AstExpr::Alias(_, expr) => bind_expr(*expr, schema),
        AstExpr::Math(op, left, right) => {
            let left = bind_expr(*left, schema)?;
            let right = bind_expr(*right, schema)?;
            Ok(AstExpr::Math(op, Box::new(left), Box::new(right)))
        }
        AstExpr::Boolean(op, left, right) => {
            let left = bind_expr(*left, schema)?;
            let right = bind_expr(*right, schema)?;
            Ok(AstExpr::Boolean(op, Box::new(left), Box::new(right)))
        }
        AstExpr::ColIdx(_) => Ok(ast),
    }
}
