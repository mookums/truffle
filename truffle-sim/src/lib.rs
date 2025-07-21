mod action;
mod column;
mod expr;
mod table;
mod ty;

use action::{create_table::handle_create_table, drop::handle_drop, query::handle_query};
pub use sqlparser::dialect::*;
use sqlparser::{
    ast::{ObjectName, Statement},
    parser::Parser,
};
use ty::SqlType;

use std::collections::HashMap;
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
    #[error("Ambigious Column: {0}")]
    AmbigiousColumn(String),
    #[error("Alias '{0}' doesn't exist")]
    AliasDoesntExist(String),
    #[error("Table or Alias '{0}' doesn't exist")]
    TableOrAliasDoesntExist(String),
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
    #[error("'{0}' is currently unsupported")]
    Unsupported(String),
}

#[derive(Debug)]
pub struct Simulator {
    dialect: Box<dyn Dialect>,
    tables: HashMap<String, Table>,
}

fn object_name_to_strings(name: &ObjectName) -> Vec<String> {
    name.0
        .iter()
        .map(|p| p.as_ident().unwrap().value.clone())
        .collect()
}

impl Simulator {
    /// Construct a new Simulator with the given SQL Dialect.
    pub fn new(dialect: Box<dyn Dialect>) -> Self {
        Self {
            dialect,
            tables: HashMap::new(),
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
    pub fn execute(&mut self, sql: &str) -> Result<(), Error> {
        let parser = Parser::new(&*self.dialect);
        let statements = parser.try_with_sql(sql)?.parse_statements()?;

        for statement in statements {
            match statement {
                Statement::CreateTable(create_table) => handle_create_table(self, create_table)?,
                Statement::Query(query) => handle_query(self, query)?,
                // TODO: Support Insert
                Statement::Drop {
                    object_type, names, ..
                } => handle_drop(self, &object_type, names)?,
                _ => return Err(Error::Unsupported(statement.to_string())),
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn invalid_sql() {
        let mut sim = Simulator::new(Box::new(GenericDialect {}));
        assert!(matches!(
            sim.execute("create eveyrthing (id int);"),
            Err(Error::Parsing(_))
        ))
    }
}
