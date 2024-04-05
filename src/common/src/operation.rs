use serde::{Deserialize, Serialize};

#[derive(Debug, Copy, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum MathOp {
    Add,
    Sub,
    Mul,
    Div,
}

impl std::fmt::Display for MathOp {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        use MathOp::*;
        match self {
            Add => write!(f, "+"),
            Sub => write!(f, "-"),
            Mul => write!(f, "*"),
            Div => write!(f, "/"),
        }
    }
}

#[derive(Debug, Copy, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum BooleanOp {
    Eq,
    Neq,
    Gt,
    Gte,
    Lt,
    Lte,
    And,
    Or,
}

impl std::fmt::Display for BooleanOp {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        use BooleanOp::*;
        match self {
            Eq => write!(f, "="),
            Neq => write!(f, "!="),
            Gt => write!(f, ">"),
            Gte => write!(f, ">="),
            Lt => write!(f, "<"),
            Lte => write!(f, "<="),
            And => write!(f, "AND"),
            Or => write!(f, "OR"),
        }
    }
}

#[derive(Debug, Copy, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum AggOp {
    Avg,
    Count,
    Max,
    Min,
    Sum,
}

impl std::fmt::Display for AggOp {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        use AggOp::*;
        match self {
            Avg => write!(f, "AVG"),
            Count => write!(f, "COUNT"),
            Max => write!(f, "MAX"),
            Min => write!(f, "MIN"),
            Sum => write!(f, "SUM"),
        }
    }
}
