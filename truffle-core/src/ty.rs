use std::{fmt::Display, hash::Hash};

use itertools::Itertools;
use sqlparser::ast::DataType;

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

use crate::column::Column;

#[cfg_attr(feature = "serde", derive(Deserialize, Serialize))]
#[derive(Debug, Clone, Eq)]
pub enum SqlType {
    // Tuple of Types
    Tuple(Vec<Column>),
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

    #[cfg(feature = "time")]
    Date,
    #[cfg(feature = "time")]
    Time,
    #[cfg(feature = "time")]
    Timestamp,
    #[cfg(feature = "time")]
    TimestampTz,

    #[cfg(feature = "uuid")]
    Uuid,

    #[cfg(feature = "json")]
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

impl PartialEq for SqlType {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (SqlType::Tuple(first), SqlType::Tuple(second)) => {
                if first.len() != second.len() {
                    return false;
                }

                first.iter().zip(second.iter()).all(|(f, s)| f.ty.eq(&s.ty))
            }
            (SqlType::SmallInt, SqlType::SmallInt) => true,
            (SqlType::Integer, SqlType::Integer) => true,
            (SqlType::BigInt, SqlType::BigInt) => true,
            (SqlType::Float, SqlType::Float) => true,
            (SqlType::Double, SqlType::Double) => true,
            (SqlType::Text, SqlType::Text) => true,
            (SqlType::Boolean, SqlType::Boolean) => true,
            #[cfg(feature = "time")]
            (SqlType::Date, SqlType::Date) => true,
            #[cfg(feature = "time")]
            (SqlType::Time, SqlType::Time) => true,
            #[cfg(feature = "time")]
            (SqlType::Timestamp, SqlType::Timestamp) => true,
            #[cfg(feature = "time")]
            (SqlType::TimestampTz, SqlType::TimestampTz) => true,
            #[cfg(feature = "uuid")]
            (SqlType::Uuid, SqlType::Uuid) => true,
            #[cfg(feature = "json")]
            (SqlType::Json, SqlType::Json) => true,
            (SqlType::Unknown(a), SqlType::Unknown(b)) => a == b,
            _ => false,
        }
    }
}

impl Hash for SqlType {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        match self {
            SqlType::SmallInt => state.write_u8(1),
            SqlType::Integer => state.write_u8(2),
            SqlType::BigInt => state.write_u8(3),
            SqlType::Float => state.write_u8(4),
            SqlType::Double => state.write_u8(5),
            SqlType::Text => state.write_u8(6),
            SqlType::Boolean => state.write_u8(7),
            #[cfg(feature = "time")]
            SqlType::Date => state.write_u8(8),
            #[cfg(feature = "time")]
            SqlType::Time => state.write_u8(9),
            #[cfg(feature = "time")]
            SqlType::Timestamp => state.write_u8(10),
            #[cfg(feature = "time")]
            SqlType::TimestampTz => state.write_u8(11),
            #[cfg(feature = "uuid")]
            SqlType::Uuid => state.write_u8(12),
            #[cfg(feature = "json")]
            SqlType::Json => state.write_u8(13),
            SqlType::Tuple(columns) => {
                state.write_u8(14);
                state.write_usize(columns.len());
                columns.iter().for_each(|c| c.ty.hash(state))
            }
            SqlType::Unknown(text) => {
                state.write_u8(15);
                text.hash(state)
            }
        }
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
            DataType::Real | DataType::Float(None) | DataType::Float4 => SqlType::Float,
            DataType::Float(Some(n)) if (0..=4).contains(&n) => SqlType::Float,
            DataType::Double(_) | DataType::Float8 => SqlType::Double,
            DataType::Float(Some(n)) if (4..=8).contains(&n) => SqlType::Double,
            DataType::Text | DataType::String(_) => SqlType::Text,
            // TODO: Length validation.
            DataType::Character(_)
            | DataType::CharacterVarying(_)
            | DataType::Char(_)
            | DataType::Varchar(_)
            | DataType::Nvarchar(_) => SqlType::Text,
            DataType::Bool | DataType::Boolean => SqlType::Boolean,
            #[cfg(feature = "time")]
            DataType::Date => SqlType::Date,
            #[cfg(feature = "time")]
            DataType::Timestamp(_, _) | DataType::Datetime(_) => SqlType::TimestampTz,
            #[cfg(feature = "time")]
            DataType::TimestampNtz => SqlType::Timestamp,
            #[cfg(feature = "time")]
            DataType::Time(_, _) => SqlType::Time,
            #[cfg(feature = "uuid")]
            DataType::Uuid => SqlType::Uuid,
            #[cfg(feature = "json")]
            DataType::JSON => SqlType::Json,
            _ => SqlType::Unknown(value.to_string()),
        }
    }
}
