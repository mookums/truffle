use std::{fmt::Debug, sync::Arc};

use serde::Deserialize;

use crate::misc::immutable::Immutable;

#[derive(Debug, Copy, Clone, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum DialectKind {
    Generic,
    Ansi,
    Sqlite,
    Postgres,
}

pub trait Dialect: Debug + 'static {
    fn kind(&self) -> DialectKind;
    fn parser_dialect(&self) -> Immutable<Arc<dyn sqlparser::dialect::Dialect>>;
}

#[derive(Debug)]
pub struct GenericDialect {
    parser: Immutable<Arc<dyn sqlparser::dialect::Dialect>>,
}

impl Default for GenericDialect {
    fn default() -> Self {
        Self {
            parser: Immutable::new(Arc::new(sqlparser::dialect::SQLiteDialect {})),
        }
    }
}

impl Dialect for GenericDialect {
    fn kind(&self) -> DialectKind {
        DialectKind::Generic
    }

    fn parser_dialect(&self) -> Immutable<Arc<dyn sqlparser::dialect::Dialect>> {
        self.parser.clone()
    }
}

#[derive(Debug)]
pub struct SqliteDialect {
    parser: Immutable<Arc<dyn sqlparser::dialect::Dialect>>,
}

impl Default for SqliteDialect {
    fn default() -> Self {
        Self {
            parser: Immutable::new(Arc::new(sqlparser::dialect::SQLiteDialect {})),
        }
    }
}

impl Dialect for SqliteDialect {
    fn kind(&self) -> DialectKind {
        DialectKind::Sqlite
    }

    fn parser_dialect(&self) -> Immutable<Arc<dyn sqlparser::dialect::Dialect>> {
        self.parser.clone()
    }
}

#[derive(Debug)]
pub struct PostgreSqlDialect {
    parser: Immutable<Arc<dyn sqlparser::dialect::Dialect>>,
}

impl Default for PostgreSqlDialect {
    fn default() -> Self {
        Self {
            parser: Immutable::new(Arc::new(sqlparser::dialect::PostgreSqlDialect {})),
        }
    }
}

impl Dialect for PostgreSqlDialect {
    fn kind(&self) -> DialectKind {
        DialectKind::Postgres
    }

    fn parser_dialect(&self) -> Immutable<Arc<dyn sqlparser::dialect::Dialect>> {
        self.parser.clone()
    }
}
