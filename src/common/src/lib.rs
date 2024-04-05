extern crate csv;
#[macro_use]
extern crate serde;
#[macro_use]
extern crate log;

use prelude::{TidType, ValueId};
use serde::de::{Deserialize, Deserializer};
use serde::ser::{Serialize, Serializer};

pub mod catalog;
pub mod commands;
pub mod crusty_graph;
pub mod data_reader;
pub mod datatypes;
pub mod error;
pub mod ids;
pub mod logical_plan;
pub mod physical_plan;
pub mod query_result;
pub mod storage_trait;
pub mod table;
pub mod testutil;
pub mod traits;

pub mod ast_expr;
pub mod bytecode_expr;
pub mod operation;

/// Page size in bytes
pub const PAGE_SIZE: usize = 4096;
// How many pages a buffer pool can hold
pub const PAGE_SLOTS: usize = 50;

pub mod prelude {
    pub use crate::datatypes::{DataType, Field};
    pub use crate::error::CrustyError;
    pub use crate::ids::Permissions;
    pub use crate::ids::{
        ContainerId, LogicalTimeStamp, Lsn, PageId, SlotId, StateType, TidType, TransactionId,
        ValueId,
    };
    pub use crate::table::TableInfo;
    pub use crate::{TableSchema, Tuple};
}
pub use crate::datatypes::{DataType, Field};
pub use crate::error::{ConversionError, CrustyError};
pub use crate::operation::{AggOp, BooleanOp, MathOp};
pub use query_result::QueryResult;

/// Handle schemas.
#[derive(Default, PartialEq, Eq, Clone, Debug)]
pub struct TableSchema {
    /// Attributes of the schema.
    pub attributes: Vec<Attribute>,
}

impl Serialize for TableSchema {
    /// Custom serialize to avoid serializing name_map.
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.attributes.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for TableSchema {
    /// Custom deserialize to avoid serializing name_map.
    fn deserialize<D>(deserializer: D) -> Result<TableSchema, D::Error>
    where
        D: Deserializer<'de>,
    {
        let attrs = Vec::deserialize(deserializer)?;
        Ok(TableSchema::new(attrs))
    }
}

impl TableSchema {
    /// Create a new schema.
    ///
    /// # Arguments
    ///
    /// * `attributes` - Attributes of the schema in the order that they are in the schema.
    pub fn new(attributes: Vec<Attribute>) -> Self {
        Self { attributes }
    }

    /// Create a new schema with the given names and dtypes.
    ///
    /// # Arguments
    ///
    /// * `names` - Names of the new schema.
    /// * `dtypes` - Dypes of the new schema.
    pub fn from_vecs(names: Vec<&str>, dtypes: Vec<DataType>) -> Self {
        let mut attrs = Vec::new();
        for (name, dtype) in names.iter().zip(dtypes.iter()) {
            attrs.push(Attribute::new(name.to_string(), dtype.clone()));
        }
        TableSchema::new(attrs)
    }

    /// Get the attribute from the given index.
    ///
    /// # Arguments
    ///
    /// * `i` - Index of the attribute to look for.
    pub fn get_attribute(&self, i: usize) -> Option<&Attribute> {
        self.attributes.get(i)
    }

    /// Get the index of the attribute.
    ///
    /// # Arguments
    ///
    /// * `name` - Name of the attribute to get the index for.
    pub fn get_field_index(&self, name: &str) -> Option<usize> {
        // parse the name
        // if it is a table_name.column_name, then use the column_name only
        // otherwise use the name as is
        for (i, attr) in self.attributes.iter().enumerate() {
            if attr.name == name {
                return Some(i);
            }
        }
        None
    }

    /// Returns attribute(s) that are primary keys
    ///
    ///
    pub fn get_pks(&self) -> Vec<Attribute> {
        let mut pk_attributes: Vec<Attribute> = Vec::new();
        for attribute in &self.attributes {
            if attribute.constraint == Constraint::PrimaryKey {
                pk_attributes.push(attribute.clone());
            }
        }
        pk_attributes
    }

    /// Check if the attribute name is in the schema.
    ///
    /// # Arguments
    ///
    /// * `name` - Name of the attribute to look for.
    pub fn contains(&self, name: &str) -> bool {
        for attr in &self.attributes {
            if attr.name == name {
                return true;
            }
        }
        false
    }

    /// Get an iterator of the attributes.
    pub fn attributes(&self) -> impl Iterator<Item = &Attribute> {
        self.attributes.iter()
    }

    /// Merge two schemas into one.
    ///
    /// The other schema is appended to the current schema.
    ///
    /// # Arguments
    ///
    /// * `other` - Other schema to add to current schema.
    pub fn merge(&self, other: &Self) -> Self {
        let mut attrs = self.attributes.clone();
        attrs.append(&mut other.attributes.clone());
        Self::new(attrs)
    }

    /// Returns the length of the schema.
    pub fn size(&self) -> usize {
        self.attributes.len()
    }
}

impl std::fmt::Display for TableSchema {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut res = String::new();
        for attr in &self.attributes {
            res.push_str(&attr.name);
            res.push('\t');
        }
        write!(f, "{}", res)
    }
}

#[derive(Serialize, Deserialize, PartialEq, Eq, Clone, Debug)]
pub enum Constraint {
    None,
    PrimaryKey,
    Unique,
    NotNull,
    UniqueNotNull,
    ForeignKey(prelude::ContainerId), // Points to other table. Infer PK
    NotNullFKey(prelude::ContainerId),
}

/// Handle attributes. Pairs the name with the dtype.
#[derive(Serialize, Deserialize, PartialEq, Eq, Clone, Debug)]
pub struct Attribute {
    /// Attribute name.
    pub name: String,
    /// Attribute dtype.
    pub dtype: DataType,
    /// Attribute constraint
    pub constraint: Constraint,
}

impl Attribute {
    /// Create a new attribute with the given name and dtype.
    ///
    /// # Arguments
    ///
    /// * `name` - Name of the attribute.
    /// * `dtype` - Dtype of the attribute.
    // pub fn new(name: String, dtype: DataType) -> Self { Self { name, dtype, is_pk: false } }

    pub fn new(name: String, dtype: DataType) -> Self {
        Self {
            name,
            dtype,
            constraint: Constraint::None,
        }
    }

    pub fn new_with_constraint(name: String, dtype: DataType, constraint: Constraint) -> Self {
        Self {
            name,
            dtype,
            constraint,
        }
    }

    pub fn new_pk(name: String, dtype: DataType) -> Self {
        Self {
            name,
            dtype,
            constraint: Constraint::PrimaryKey,
        }
    }

    /// Returns the name of the attribute.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the dtype of the attribute.
    pub fn dtype(&self) -> &DataType {
        &self.dtype
    }
}

/// Tuple type.
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
pub struct Tuple {
    // Header
    /// The transaction id for concurrency control
    pub tid: TidType,
    #[serde(skip_serializing)]
    #[cfg(feature = "inlinecc")]
    /// Optionally used for read lock or read-ts
    pub read: TidType,

    #[serde(skip_serializing)]
    #[cfg(feature = "mvcc")]
    /// Used for multi-version systems
    pub begin_ts: TidType,

    #[serde(skip_serializing)]
    #[cfg(feature = "mvcc")]
    /// Used for multi-version systems
    pub end_ts: TidType,

    /// Used for multi-version systems, points to next version (older or newer)
    #[cfg(feature = "mvcc")]
    pub tuple_pointer: Option<ValueId>,

    #[serde(skip_serializing)]
    /// Used for query processing to track the source
    pub value_id: Option<ValueId>,

    /// Tuple data.
    pub field_vals: Vec<Field>,
}

impl Tuple {
    /// Create a new tuple with the given data.
    ///
    /// # Arguments
    ///
    /// * `field_vals` - Field values of the tuple.
    pub fn new(field_vals: Vec<Field>) -> Self {
        Self {
            tid: 0,
            value_id: None,
            field_vals,
        }
    }

    /// Get the field at index.
    ///
    /// # Arguments
    ///
    /// * `i` - Index of the field.
    pub fn get_field(&self, i: usize) -> Option<&Field> {
        self.field_vals.get(i)
    }

    /// Update the index at field.
    ///
    /// # Arguments
    ///
    /// * `i` - Index of the value to insert.
    /// * `f` - Value to add.
    ///
    /// # Panics
    ///
    /// Panics if the index is out-of-bounds.
    pub fn set_field(&mut self, i: usize, f: Field) {
        self.field_vals[i] = f;
    }

    /// Returns an iterator over the field values.
    pub fn field_vals(&self) -> impl Iterator<Item = &Field> {
        self.field_vals.iter()
    }

    /// Return the length of the tuple.
    pub fn len(&self) -> usize {
        self.field_vals.len()
    }

    pub fn is_empty(&self) -> bool {
        self.field_vals.is_empty()
    }

    pub fn size(&self) -> usize {
        let mut size = 0;
        for field in &self.field_vals {
            size += field.size();
        }
        size += std::mem::size_of::<TidType>();
        size += std::mem::size_of::<Option<ValueId>>();
        size
    }

    /// Append another tuple with self.
    ///
    /// # Arguments
    ///
    /// * `other` - Other tuple to append.
    pub fn merge(&self, other: &Self) -> Self {
        let mut fields = self.field_vals.clone();
        fields.append(&mut other.field_vals.clone());
        Self::new(fields)
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        serde_cbor::to_vec(&self).unwrap()
    }

    pub fn from_bytes(bytes: &[u8]) -> Self {
        serde_cbor::from_slice(bytes).unwrap()
    }

    pub fn to_csv(&self) -> String {
        let mut res = Vec::new();
        for field in &self.field_vals {
            res.push(field.to_string());
        }
        res.join(",")
    }
}

impl std::fmt::Display for Tuple {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut res = String::new();
        for field in &self.field_vals {
            res.push_str(&field.to_string());
            res.push('\t');
        }
        write!(f, "{}", res)
    }
}

/// The result of converting tuples for ingestion
#[derive(Serialize, Deserialize, Debug, Default)]
pub struct ConvertedResult {
    /// The records that converted succesfully
    pub converted: Vec<Tuple>,
    /// The list of records that did no convert by offset and issues
    pub unconverted: Vec<(usize, Vec<ConversionError>)>,
}

impl ConvertedResult {
    pub fn new() -> Self {
        Self {
            converted: Vec::new(),
            unconverted: Vec::new(),
        }
    }

    pub fn reset(&mut self) {
        self.converted.clear();
        self.unconverted.clear();
    }
}

#[cfg(test)]
mod libtests {
    use super::*;
    use crate::testutil::*;

    #[test]
    fn test_tuple_bytes() {
        let tuple = int_vec_to_tuple(vec![0, 1, 0]);
        let tuple_bytes = tuple.to_bytes();
        let check_tuple: Tuple = Tuple::from_bytes(&tuple_bytes);
        assert_eq!(tuple, check_tuple);
    }

    #[test]
    fn test_decimal_field() {
        let d1 = "13.2";
        let dfield = Field::Decimal(132, 1);
        let dtype = DataType::Decimal(3, 1);
        let attr = Attribute::new("dec field".to_string(), dtype);
        let df = Field::from_str(d1, &attr).unwrap();
        assert_eq!(dfield, df);
        assert_eq!(df.to_string(), d1);
    }
}
