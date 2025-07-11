use sqlparser::ast::DataType;

#[derive(Debug, PartialEq, Eq)]
pub enum ColumnKind {
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

impl From<DataType> for ColumnKind {
    fn from(value: DataType) -> Self {
        match value {
            DataType::Int2(_) | DataType::SmallInt(_) => ColumnKind::SmallInt,
            DataType::Int4(_) | DataType::Integer(_) | DataType::Int(_) => ColumnKind::Integer,
            DataType::Int8(_) | DataType::BigInt(_) => ColumnKind::BigInt,
            DataType::Real | DataType::Float(Some(4)) | DataType::Float4 | DataType::Float(_) => {
                ColumnKind::Float
            }
            DataType::Double(_) | DataType::Float8 => ColumnKind::Double,
            DataType::Text => ColumnKind::Text,
            DataType::Bool | DataType::Boolean => ColumnKind::Boolean,
            DataType::Date => ColumnKind::Date,
            DataType::Timestamp(_, _) => ColumnKind::Timestamp,
            DataType::Uuid => ColumnKind::Uuid,
            DataType::JSON => ColumnKind::Json,
            _ => ColumnKind::Unknown(value.to_string()),
        }
    }
}

#[derive(Debug)]
pub struct Column {
    pub kind: ColumnKind,
}
