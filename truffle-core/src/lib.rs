mod action;
mod column;
mod expr;
mod misc;
pub mod resolve;
mod table;
pub mod ty;

pub use misc::config::Config;
pub use misc::config::DialectKind;
use misc::immutable::Immutable;

use resolve::ResolvedQuery;
pub use sqlparser::dialect::*;
use sqlparser::{
    ast::{ObjectName, Statement},
    parser::Parser,
};
use ty::SqlType;

use std::{collections::HashMap, sync::Arc};
use table::Table;

#[derive(Debug, PartialEq, Eq, thiserror::Error)]
pub enum Error {
    #[error("Parsing: {0}")]
    Parsing(#[from] sqlparser::parser::ParserError),
    #[error("SQL: {0}")]
    Sql(String),
    #[error("Table '{0}' already exists")]
    TableAlreadyExists(String),
    #[error("Column '{0}' already exists")]
    ColumnAlreadyExists(String),
    #[error("Table '{0}' doesn't exist")]
    TableDoesntExist(String),
    #[error("Column '{0}' doesn't exist")]
    ColumnDoesntExist(String),
    #[error("Ambiguous Column: {0}")]
    AmbiguousColumn(String),
    #[error("Ambiguous Alias: {0}")]
    AmbiguousAlias(String),
    #[error("Alias '{0}' doesn't exist")]
    AliasDoesntExist(String),
    #[error("Qualifier '{0}' doesn't exist")]
    QualifierDoesntExist(String),
    #[error("Qualified Column '{qualifier}.{column}' doesn't exist")]
    QualifiedColumnDoesntExist { qualifier: String, column: String },
    #[error("Alias '{0}' is the name of an existing Table")]
    AliasIsTableName(String),
    #[error("Foreign Key Constraint Failure on Column '{0}'")]
    ForeignKeyConstraint(String),
    #[error("Type Mismatch: expected {expected} and got {got}")]
    TypeMismatch { expected: SqlType, got: SqlType },
    #[error("Type Not Numeric: got {0}")]
    TypeNotNumeric(SqlType),
    #[error("Cannot set not null column '{0}' to null")]
    NullOnNotNullColumn(String),
    #[error("Cannot set not default column '{0}' to default value")]
    DefaultOnNotDefaultColumn(String),
    #[error("{0} cannot be used as a default. Use a literal value.")]
    InvalidDefault(String),
    #[error("Column count mismatch: expected {expected} and got {got}")]
    ColumnCountMismatch { expected: usize, got: usize },
    #[error("Required column missing for '{0}'")]
    RequiredColumnMissing(String),
    #[error("No common column")]
    NoCommonColumn,
    #[error("'{0}' is currently unsupported")]
    Unsupported(String),
}

#[derive(Debug, Clone)]
pub struct Simulator {
    pub kind: DialectKind,
    dialect: Immutable<Arc<dyn Dialect>>,
    tables: HashMap<String, Table>,
}

fn object_name_to_strings(name: &ObjectName) -> Vec<String> {
    name.0
        .iter()
        .map(|p| p.as_ident().unwrap().value.clone())
        .collect()
}

impl Default for Simulator {
    fn default() -> Self {
        Self {
            kind: DialectKind::Generic,
            dialect: Immutable::new(Arc::new(GenericDialect {})),
            tables: HashMap::new(),
        }
    }
}

impl Simulator {
    /// Construct a new Simulator with the given SQL Dialect.
    fn create<D: Dialect>(dialect: D, kind: DialectKind) -> Self {
        Self {
            kind,
            dialect: Immutable::new(Arc::new(dialect)),
            tables: HashMap::new(),
        }
    }

    // Construct a new Simulator with the given Dialect.
    pub fn with_dialect(kind: DialectKind) -> Self {
        match kind {
            DialectKind::Generic => Simulator::create(GenericDialect {}, DialectKind::Generic),
            DialectKind::Ansi => Simulator::create(AnsiDialect {}, DialectKind::Ansi),
            DialectKind::Sqlite => Simulator::create(SQLiteDialect {}, DialectKind::Sqlite),
            DialectKind::Postgres => Simulator::create(PostgreSqlDialect {}, DialectKind::Postgres),
        }
    }

    /// Get a Table that exists within the Simulator.
    pub fn get_table(&self, name: &str) -> Option<&Table> {
        self.tables.get(name)
    }

    pub fn get_tables(&self) -> &HashMap<String, Table> {
        &self.tables
    }

    pub fn has_table(&self, name: &str) -> bool {
        self.tables.contains_key(name)
    }

    /// Executes the given SQL in the Simulator and updates the state.
    /// Returns the resolved query for the last statement ran.
    pub fn execute(&mut self, sql: impl AsRef<str>) -> Result<ResolvedQuery, Error> {
        let parser = Parser::new(&**self.dialect);
        let statements = parser.try_with_sql(sql.as_ref())?.parse_statements()?;

        let mut resolved = ResolvedQuery::default();

        for statement in statements {
            resolved = match statement {
                Statement::CreateTable(create_table) => self.create_table(create_table)?,
                // TODO: Support Alter Table
                Statement::Query(query) => self.query(query)?,
                Statement::Insert(insert) => self.insert(insert)?,
                Statement::Delete(delete) => self.delete(delete)?,
                Statement::Drop {
                    object_type, names, ..
                } => self.drop(&object_type, names)?,
                _ => return Err(Error::Unsupported(statement.to_string())),
            };
        }

        Ok(resolved)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn invalid_sql() {
        let mut sim = Simulator::default();
        assert!(matches!(
            sim.execute("create eveyrthing (id int);"),
            Err(Error::Parsing(_))
        ))
    }
}
