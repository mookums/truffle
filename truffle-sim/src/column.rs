use std::fmt::Display;

use serde::Serialize;
use sqlparser::ast::DataType;

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub enum ColumnType {
    /// 16 bit Signed Integer
    SmallInt,
    /// 32 bit Signed Integer
    Integer,
    /// 64 bit Signed Integer,
    BigInt,

    /// 32 bit Floating
    Float,
    /// 64 bit Floating
    Double,

    /// String
    Text,

    Boolean,
    Date,
    Timestamp,
    Uuid,
    Json,
    Unknown(String),
}

impl Display for ColumnType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let str = match self {
            ColumnType::SmallInt => "smallint",
            ColumnType::Integer => "integer",
            ColumnType::BigInt => "bigint",
            ColumnType::Float => "float",
            ColumnType::Double => "double",
            ColumnType::Text => "text",
            ColumnType::Boolean => "bool",
            ColumnType::Date => "date",
            ColumnType::Timestamp => "timestamp",
            ColumnType::Uuid => "uuid",
            ColumnType::Json => "json",
            ColumnType::Unknown(_) => "unknown",
        };

        write!(f, "{str}")
    }
}

impl From<DataType> for ColumnType {
    fn from(value: DataType) -> Self {
        match value {
            DataType::Int2(_) | DataType::SmallInt(_) => ColumnType::SmallInt,
            DataType::Int4(_) | DataType::Integer(_) | DataType::Int(_) => ColumnType::Integer,
            DataType::Int8(_) | DataType::BigInt(_) => ColumnType::BigInt,
            DataType::Real | DataType::Float(Some(4)) | DataType::Float4 | DataType::Float(_) => {
                ColumnType::Float
            }
            DataType::Double(_) | DataType::Float8 => ColumnType::Double,
            DataType::Text => ColumnType::Text,
            DataType::Bool | DataType::Boolean => ColumnType::Boolean,
            DataType::Date => ColumnType::Date,
            DataType::Timestamp(_, _) => ColumnType::Timestamp,
            DataType::Uuid => ColumnType::Uuid,
            DataType::JSON => ColumnType::Json,
            _ => ColumnType::Unknown(value.to_string()),
        }
    }
}

#[derive(Debug, Serialize)]
pub struct ColumnForeignKey {
    pub foreign_table: String,
    pub foreign_columns: Vec<String>,
}

#[derive(Debug)]
pub struct Column {
    pub ty: ColumnType,
    pub nullable: bool,
    pub default: bool,
}

impl Column {
    pub fn get_kind(&self) -> &ColumnType {
        &self.ty
    }
}
