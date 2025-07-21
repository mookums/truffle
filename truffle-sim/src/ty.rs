use std::fmt::Display;

use itertools::Itertools;
use serde::Serialize;
use sqlparser::ast::DataType;

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub enum SqlType {
    // NULL
    Null,
    // Tuple of Types
    Tuple(Vec<SqlType>),
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
    Time,
    Timestamp,
    TimestampTz,
    Uuid,
    Json,
    Unknown(String),
}

impl SqlType {
    pub fn is_integer(&self) -> bool {
        matches!(self, Self::SmallInt | Self::Integer | Self::BigInt)
    }

    pub fn is_floating(&self) -> bool {
        matches!(self, Self::Float | Self::Double)
    }

    pub fn is_numeric(&self) -> bool {
        self.is_integer() || self.is_floating()
    }
}

impl Display for SqlType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SqlType::Tuple(sql_types) => write!(
                f,
                "Tuple({})",
                sql_types.iter().map(|ty| ty.to_string()).join(", ")
            ),
            _ => write!(f, "{self:#?}"),
        }
    }
}

impl From<DataType> for SqlType {
    fn from(value: DataType) -> Self {
        match value {
            DataType::Int2(_) | DataType::SmallInt(_) => SqlType::SmallInt,
            DataType::Int4(_) | DataType::Integer(_) | DataType::Int(_) => SqlType::Integer,
            DataType::Int8(_) | DataType::BigInt(_) => SqlType::BigInt,
            DataType::Real | DataType::Float(Some(4)) | DataType::Float4 | DataType::Float(_) => {
                SqlType::Float
            }
            DataType::Double(_) | DataType::Float8 => SqlType::Double,
            DataType::Text | DataType::String(_) => SqlType::Text,
            DataType::Bool | DataType::Boolean => SqlType::Boolean,
            DataType::Date => SqlType::Date,
            DataType::Timestamp(_, _) | DataType::Datetime(_) => SqlType::TimestampTz,
            DataType::TimestampNtz => SqlType::Timestamp,
            DataType::Time(_, _) => SqlType::Time,
            DataType::Uuid => SqlType::Uuid,
            DataType::JSON => SqlType::Json,
            _ => SqlType::Unknown(value.to_string()),
        }
    }
}
