use crate::impl_transparent_compat;

use truffle::dialect::SqliteDialect;

#[cfg(feature = "time")]
use time::format_description::well_known::Rfc3339;

use super::{FromSql, IntoSql};

impl_transparent_compat!(SqliteDialect, i16, i32, i64, f32, f64, String);

impl IntoSql<i32, SqliteDialect> for bool {
    fn into_sql_type(self) -> i32 {
        if self { 1 } else { 0 }
    }
}

impl FromSql<i32, SqliteDialect> for bool {
    fn from_sql_type(value: i32) -> Self {
        value == 1
    }
}

impl IntoSql<String, SqliteDialect> for &str {
    fn into_sql_type(self) -> String {
        self.to_string()
    }
}

#[cfg(feature = "uuid")]
impl IntoSql<String, SqliteDialect> for uuid::Uuid {
    fn into_sql_type(self) -> String {
        self.to_string()
    }
}

#[cfg(feature = "uuid")]
impl FromSql<String, SqliteDialect> for uuid::Uuid {
    fn from_sql_type(value: String) -> Self {
        uuid::Uuid::parse_str(&value).unwrap()
    }
}

#[cfg(feature = "time")]
impl IntoSql<String, SqliteDialect> for time::PrimitiveDateTime {
    fn into_sql_type(self) -> String {
        self.format(&Rfc3339).unwrap()
    }
}

#[cfg(feature = "time")]
impl FromSql<String, SqliteDialect> for time::PrimitiveDateTime {
    fn from_sql_type(value: String) -> Self {
        Self::parse(&value, &Rfc3339).unwrap()
    }
}

#[cfg(feature = "time")]
impl IntoSql<String, SqliteDialect> for time::OffsetDateTime {
    fn into_sql_type(self) -> String {
        self.unix_timestamp().to_string()
    }
}

#[cfg(feature = "time")]
impl FromSql<String, SqliteDialect> for time::OffsetDateTime {
    fn from_sql_type(value: String) -> Self {
        let timestamp: i64 = value.parse().unwrap();
        time::OffsetDateTime::from_unix_timestamp(timestamp).unwrap()
    }
}

#[cfg(feature = "time")]
impl IntoSql<String, SqliteDialect> for time::Date {
    fn into_sql_type(self) -> String {
        self.format(&Rfc3339).unwrap()
    }
}

#[cfg(feature = "time")]
impl FromSql<String, SqliteDialect> for time::Date {
    fn from_sql_type(value: String) -> Self {
        Self::parse(&value, &Rfc3339).unwrap()
    }
}

#[cfg(feature = "time")]
impl IntoSql<String, SqliteDialect> for time::Time {
    fn into_sql_type(self) -> String {
        self.format(&Rfc3339).unwrap()
    }
}

#[cfg(feature = "time")]
impl FromSql<String, SqliteDialect> for time::Time {
    fn from_sql_type(value: String) -> Self {
        Self::parse(&value, &Rfc3339).unwrap()
    }
}

#[cfg(feature = "json")]
impl IntoSql<String, SqliteDialect> for serde_json::Value {
    fn into_sql_type(self) -> String {
        self.to_string()
    }
}

#[cfg(feature = "json")]
impl FromSql<String, SqliteDialect> for serde_json::Value {
    fn from_sql_type(value: String) -> Self {
        use std::str::FromStr;
        Self::from_str(&value).unwrap()
    }
}
