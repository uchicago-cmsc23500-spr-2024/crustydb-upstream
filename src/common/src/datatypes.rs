use crate::bytecode_expr::{And, FromBool, Or};
use crate::error::{c_err, CrustyError};
use crate::{Attribute, BooleanOp};
use chrono::{Duration, NaiveDate};
use std::ops::{Add, Div, Mul, Sub};

pub fn base_date() -> NaiveDate {
    NaiveDate::from_ymd_opt(1970, 1, 1).unwrap()
}

pub fn null_string() -> String {
    String::from("NULL")
}

pub fn default_decimal_precision() -> u32 {
    10
}

pub fn default_decimal_scale() -> u32 {
    4
}

/// Utilities
pub fn f_int(i: i64) -> Field {
    Field::Int(i)
}

pub fn f_str(s: &str) -> Field {
    Field::String(s.to_string())
}

pub fn f_decimal(f: f64) -> Field {
    let s = default_decimal_scale();
    let whole = (f * 10f64.powi(s as i32)) as i64;
    Field::Decimal(whole, s)
}

pub fn f_date(s: &str) -> Field {
    let date = NaiveDate::parse_from_str(s, "%Y-%m-%d").unwrap();
    let days = date.signed_duration_since(base_date()).num_days();
    Field::Date(days)
}

/// Enumerate the supported dtypes.
/// When adding a new dtype, make sure to add a corresponding field type.
#[derive(PartialEq, Eq, Serialize, Deserialize, Clone, Debug)]
pub enum DataType {
    Int,
    String,
    Decimal(u32, u32), // Precision, Scale : Precision is total number of digits, scale is number of digits after decimal
    Date,
    Bool,
    Null,
}

impl std::fmt::Display for DataType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DataType::Int => write!(f, "int"),
            DataType::String => write!(f, "string"),
            DataType::Decimal(p, s) => write!(f, "decimal({},{})", p, s),
            DataType::Date => write!(f, "date"),
            DataType::Bool => write!(f, "bool"),
            DataType::Null => write!(f, "null"),
        }
    }
}

impl DataType {
    /// Returns the size of the data type in bytes.
    /// Returns None if the size is variable.
    pub fn size(&self) -> Option<usize> {
        match self {
            DataType::Int => Some(8),
            DataType::String => None,
            DataType::Decimal(_, _) => Some(12),
            DataType::Date => Some(8),
            DataType::Bool => Some(1),
            DataType::Null => Some(1),
        }
    }
}

/// For each of the dtypes, make sure that there is a corresponding field type.
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Clone, Hash)]
pub enum Field {
    Int(i64),
    String(String),
    Decimal(i64, u32), // Whole, Scale : Whole is the integer part and fractional part combined, scale is number of digits after decimal
    Date(i64),         // Days relative to 1970-01-01
    Bool(bool),
    Null,
}

impl FromBool for Field {
    fn from_bool(b: bool) -> Self {
        Field::Bool(b)
    }
}

impl And for Field {
    fn and(&self, other: &Self) -> Self {
        match (self, other) {
            (Field::Bool(a), Field::Bool(b)) => Field::Bool(*a && *b),
            _ => panic!("Expected bool"),
        }
    }
}

impl Or for Field {
    fn or(&self, other: &Self) -> Self {
        match (self, other) {
            (Field::Bool(a), Field::Bool(b)) => Field::Bool(*a || *b),
            _ => panic!("Expected bool"),
        }
    }
}

impl Add for Field {
    type Output = Result<Self, CrustyError>;

    fn add(self, other: Self) -> Self::Output {
        match (self, other) {
            (Field::Int(a), Field::Int(b)) => Ok(Field::Int(a + b)),
            (Field::Decimal(a, s_l), Field::Decimal(b, s_r)) => {
                // We adjust to the larger scale
                let res_scale = if s_l > s_r { s_l } else { s_r };
                let adjusted_a = a * 10i64.pow(res_scale - s_l);
                let adjusted_b = b * 10i64.pow(res_scale - s_r);
                Ok(Field::Decimal(adjusted_a + adjusted_b, res_scale))
            }
            (Field::Int(a), Field::Decimal(b, s_r)) => {
                let adjusted_a = a * 10i64.pow(s_r);
                Ok(Field::Decimal(adjusted_a + b, s_r))
            }
            (Field::Decimal(a, s_l), Field::Int(b)) => {
                let adjusted_b = b * 10i64.pow(s_l);
                Ok(Field::Decimal(a + adjusted_b, s_l))
            }
            _ => panic!("Expected int or decimal"),
        }
    }
}

impl Sub for Field {
    type Output = Result<Self, CrustyError>;

    fn sub(self, other: Self) -> Self::Output {
        match (self, other) {
            (Field::Int(a), Field::Int(b)) => Ok(Field::Int(a - b)),
            (Field::Decimal(a, s_l), Field::Decimal(b, s_r)) => {
                // We adjust to the larger scale
                let res_scale = if s_l > s_r { s_l } else { s_r };
                let adjusted_a = a * 10i64.pow(res_scale - s_l);
                let adjusted_b = b * 10i64.pow(res_scale - s_r);
                Ok(Field::Decimal(adjusted_a - adjusted_b, res_scale))
            }
            (Field::Int(a), Field::Decimal(b, s_r)) => {
                let adjusted_a = a * 10i64.pow(s_r);
                Ok(Field::Decimal(adjusted_a - b, s_r))
            }
            (Field::Decimal(a, s_l), Field::Int(b)) => {
                let adjusted_b = b * 10i64.pow(s_l);
                Ok(Field::Decimal(a - adjusted_b, s_l))
            }
            _ => Err(c_err("Expected int or decimal")),
        }
    }
}

impl Mul for Field {
    type Output = Result<Self, CrustyError>;

    fn mul(self, other: Self) -> Self::Output {
        match (self, other) {
            (Field::Int(a), Field::Int(b)) => Ok(Field::Int(a * b)),
            (Field::Decimal(a, s_l), Field::Decimal(b, s_r)) => {
                // We adjust to the larger scale
                // e.g. 123.456 * 2.34
                // 123.456 is stored as 123456 with a scale of 3
                // 2.34 is stored as 234 with a scale of 2
                // We will compute 123456 * 234 = 28846104.
                // We will divide 28846104 by the SMALLER scale 10^2, which gives 288461 with rounding.
                // This result, 288461, represents 288.461 when considering the scale 3.
                let larger_scale = s_l.max(s_r);
                let smaller_scale = s_l.min(s_r);
                let res = (a * b) as f64;
                let num = (res / 10i64.pow(smaller_scale) as f64).round() as i64;
                Ok(Field::Decimal(num, larger_scale))
            }
            (Field::Int(a), Field::Decimal(b, s_r)) => {
                // We remain the scale unchanged. We round the result to the nearest integer.
                // e.g. 123 * 2.34
                // 2.34 is stored as 234 with a scale of 2
                // We will compute 123 * 234 = 28782 and store it as 28782 with a scale of 2
                let res = a * b;
                Ok(Field::Decimal(res, s_r))
            }
            (Field::Decimal(a, s_l), Field::Int(b)) => {
                let res = a * b;
                Ok(Field::Decimal(res, s_l))
            }
            _ => Err(c_err("Expected int or decimal")),
        }
    }
}

impl Div for Field {
    type Output = Result<Self, CrustyError>;

    fn div(self, other: Self) -> Self::Output {
        match (self, other) {
            (Field::Int(a), Field::Int(b)) => {
                if b == 0 {
                    return Err(c_err("Division by zero"));
                }
                Ok(Field::Int(a / b))
            }
            (Field::Decimal(a, s_l), Field::Decimal(b, s_r)) => {
                if b == 0 {
                    return Err(c_err("Division by zero"));
                }
                // We adjust to the larger scale
                // e.g. 123.456 / 2.34
                // 123.456 is stored as 123456 with a scale of 3
                // 2.34 is stored as 234 with a scale of 2
                // At the end, we want the division result to have scale of 3
                // Hence, we adjust 123456 to 12345600 by multiplying by the scale 10^2
                // Now, divide 12345600 by 234, which gives 52728 with rounding.
                // This result, 52728, represents 52.728 when considering the scale 3.

                // e.g. 123.45 / 2.345
                // 123.45 is stored as 12345 with a scale of 2
                // 2.345 is stored as 2345 with a scale of 3
                // At the end, we want the division result to have scale of 3
                // Hence, we adjust 12345 to 123450000 by multiplying by the scale 10^4
                // Now, divide 123450000 by 2345, which gives 52628 with rounding.
                // This result, 52628, represents 52.628 when considering the scale 3.

                // Hence, the following is true:
                // s_l + alpha - s_r = max(s_l, s_r)
                // where alpha is the number of digits you need to multiply the numerator by to get the adjusted numerator
                // alpha = max(s_l, s_r) - s_l + s_r
                // In the first example, alpha = 3 - 3 + 2 = 2
                // In the second example, alpha = 3 - 2 + 3 = 4

                let larger_scale = s_l.max(s_r);
                let alpha = larger_scale - s_l + s_r;
                let res = (a * 10i64.pow(alpha)) as f64;
                let num = (res / b as f64).round() as i64;
                Ok(Field::Decimal(num, larger_scale))
            }
            (Field::Int(a), Field::Decimal(b, s_r)) => {
                Field::Decimal(a, 0) / Field::Decimal(b, s_r)
            }
            (Field::Decimal(a, s_l), Field::Int(b)) => {
                Field::Decimal(a, s_l) / Field::Decimal(b, 0)
            }
            _ => Err(c_err("Expected int or decimal")),
        }
    }
}

impl Field {
    pub fn size(&self) -> usize {
        match self {
            Field::Int(_) => 8,
            Field::String(s) => s.len(),
            Field::Date(_) => 8,
            Field::Decimal(_, _) => 12,
            Field::Bool(_) => 1,
            Field::Null => 1,
        }
    }

    /// Function to convert a Tuple field into bytes for serialization
    ///
    /// This function always uses least endian byte ordering and stores strings in the format |string length|string contents|.
    pub fn to_bytes(&self) -> Vec<u8> {
        match self {
            Field::Int(x) => x.to_le_bytes().to_vec(),
            Field::String(s) => {
                let s_len: u32 = s.len() as u32;
                let mut result = s_len.to_le_bytes().to_vec();
                result.extend(s.clone().into_bytes());
                result
            }
            Field::Date(x) => x.to_le_bytes().to_vec(),
            Field::Decimal(whole, scale) => {
                let mut bytes = whole.to_le_bytes().to_vec();
                bytes.extend(scale.to_le_bytes().to_vec());
                bytes
            }
            Field::Bool(b) => {
                if *b {
                    vec![1_u8]
                } else {
                    vec![0_u8]
                }
            }
            Field::Null => b"\0".to_vec(),
        }
    }

    pub fn from_bytes(bytes: &[u8], dtype: &DataType) -> Result<Self, CrustyError> {
        match dtype {
            DataType::Int => {
                let value = i64::from_le_bytes(bytes.try_into().map_err(|_| {
                    CrustyError::CrustyError("Failed to convert bytes to i64.".to_string())
                })?);
                Ok(Field::Int(value))
            }
            DataType::String => {
                let s_len = u32::from_le_bytes(bytes[0..4].try_into().map_err(|_| {
                    CrustyError::CrustyError("Failed to get string length.".to_string())
                })?) as usize;
                let value = String::from_utf8(bytes[4..4 + s_len].to_vec()).map_err(|_| {
                    CrustyError::CrustyError("Failed to convert bytes to string.".to_string())
                })?;
                Ok(Field::String(value))
            }
            DataType::Date => {
                let value = i64::from_le_bytes(bytes.try_into().map_err(|_| {
                    CrustyError::CrustyError("Failed to convert bytes to u32.".to_string())
                })?);
                Ok(Field::Date(value))
            }
            DataType::Decimal(_, _) => {
                let whole = i64::from_le_bytes(bytes[0..8].try_into().map_err(|_| {
                    CrustyError::CrustyError("Failed to convert bytes to i64.".to_string())
                })?);
                let scale = u32::from_le_bytes(bytes[8..12].try_into().map_err(|_| {
                    CrustyError::CrustyError("Failed to convert bytes to u32.".to_string())
                })?);
                Ok(Field::Decimal(whole, scale))
            }
            DataType::Bool => {
                let value = bytes[0] == 1;
                Ok(Field::Bool(value))
            }
            DataType::Null => {
                if bytes[0] == 0 {
                    Ok(Field::Null)
                } else {
                    Err(CrustyError::CrustyError("Invalid null field".to_string()))
                }
            }
        }
    }

    pub fn unwrap_int_field(&self) -> i64 {
        match self {
            Field::Int(i) => *i,
            _ => panic!("Expected i32"),
        }
    }

    pub fn unwrap_string_field(&self) -> &str {
        match self {
            Field::String(s) => s,
            _ => panic!("Expected String"),
        }
    }

    pub fn unwrap_bool_field(&self) -> bool {
        match self {
            Field::Bool(b) => *b,
            _ => panic!("Expected bool"),
        }
    }

    pub fn from_str(field: &str, attr: &Attribute) -> Result<Self, CrustyError> {
        if field == null_string() {
            return Field::from_str_to_null(field);
        }
        match &attr.dtype() {
            DataType::Int => Field::from_str_to_int(field),
            DataType::String => Field::from_str_to_string(field),
            DataType::Decimal(p, s) => Field::from_str_to_decimal(field, *p, *s),
            DataType::Date => Field::from_str_to_date(field),
            DataType::Bool => Field::from_str_to_bool(field),
            DataType::Null => Field::from_str_to_null(field),
        }
    }

    pub fn from_str_to_int(field: &str) -> Result<Self, CrustyError> {
        let value = field.parse::<i64>();
        if let Ok(i) = value {
            Ok(Field::Int(i))
        } else {
            Err(CrustyError::ValidationError(format!(
                "Invalid int field {}",
                field
            )))
        }
    }

    pub fn from_str_to_decimal(field: &str, p: u32, s: u32) -> Result<Self, CrustyError> {
        // Divide the field into integer and fractional parts
        let parts = field.split('.').collect::<Vec<&str>>();
        if parts.len() > 2 {
            return Err(CrustyError::ValidationError(format!(
                "Invalid decimal field {}",
                field
            )));
        }
        let integer = parts[0].parse::<i64>().map_err(|_| {
            CrustyError::ValidationError(format!("Invalid decimal field {}", field))
        })?;

        // Check the integer part
        if integer.abs() >= 10i64.pow(p - s) {
            return Err(CrustyError::ValidationError(format!(
                "Invalid decimal field precision {}. Expected {} found {}",
                field, p, s
            )));
        }
        // if scale is 2, then adjusted_fractional is 0.05 -> 5, 0.5 -> 50
        let adjusted_fractional = if parts.len() == 2 {
            let fractional = parts[1].parse::<i64>().map_err(|_| {
                CrustyError::ValidationError(format!("Invalid decimal field {}", field))
            })?;
            if fractional < 0 || fractional >= 10i64.pow(s) {
                return Err(CrustyError::ValidationError(format!(
                    "Invalid decimal field scale {}. Expected {} found {}",
                    field, p, s
                )));
            }
            fractional * 10i64.pow(s - parts[1].len() as u32)
        } else {
            0
        };
        if integer < 0 {
            Ok(Field::Decimal(
                integer * 10i64.pow(s) - adjusted_fractional,
                s,
            ))
        } else {
            Ok(Field::Decimal(
                integer * 10i64.pow(s) + adjusted_fractional,
                s,
            ))
        }
    }

    pub fn from_str_to_string(field: &str) -> Result<Self, CrustyError> {
        let value: String = field.to_string().clone();
        Ok(Field::String(value))
    }

    pub fn from_str_to_date(field: &str) -> Result<Self, CrustyError> {
        let value = NaiveDate::parse_from_str(field, "%Y-%m-%d");
        if let Ok(date) = value {
            let days = date.signed_duration_since(base_date()).num_days();
            Ok(Field::Date(days))
        } else {
            Err(CrustyError::ValidationError(format!(
                "Invalid date field {}",
                field
            )))
        }
    }

    pub fn from_str_to_bool(field: &str) -> Result<Self, CrustyError> {
        let value = field.parse::<bool>();
        if let Ok(value) = value {
            Ok(Field::Bool(value))
        } else {
            Err(CrustyError::ValidationError(format!(
                "Invalid bool field {}",
                field
            )))
        }
    }

    pub fn from_str_to_null(field: &str) -> Result<Self, CrustyError> {
        if field == null_string() {
            Ok(Field::Null)
        } else {
            Err(CrustyError::ValidationError(format!(
                "Invalid null field {}",
                field
            )))
        }
    }
}

impl std::fmt::Display for Field {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            Field::Int(i) => i.to_string(),
            Field::String(s) => s.to_string(),
            Field::Date(i) => {
                let date = base_date() + Duration::days(*i);
                date.format("%Y-%m-%d").to_string()
            }
            Field::Decimal(whole, scale) => {
                let s = whole.to_string();

                // Calculate the number of padding zeros required
                let padding = if s.len() <= *scale as usize {
                    *scale as usize + 1 - s.len() // +1 for the digit before the decimal point
                } else {
                    0
                };

                // Insert the padding zeros at the beginning
                let padded_s = format!("{:0>width$}", s, width = s.len() + padding);

                // Calculate the position to insert the decimal point
                let decimal_pos = padded_s.len() - *scale as usize;

                // Insert the decimal point
                let mut result = padded_s;
                result.insert(decimal_pos, '.');

                result
            }
            Field::Bool(b) => b.to_string(),
            Field::Null => null_string(),
        };
        write!(f, "{}", s)
    }
}

pub fn compare_fields(op: BooleanOp, left: &Field, right: &Field) -> bool {
    match op {
        BooleanOp::Eq => left == right,
        BooleanOp::Neq => left != right,
        BooleanOp::Gt => left > right,
        BooleanOp::Gte => left >= right,
        BooleanOp::Lt => left < right,
        BooleanOp::Lte => left <= right,
        BooleanOp::And => left.and(right).unwrap_bool_field(),
        BooleanOp::Or => left.or(right).unwrap_bool_field(),
    }
}
