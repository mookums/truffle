use sqlparser::ast::{Expr, Query, SelectItem, SetExpr, TableFactor};
use tracing::warn;

use crate::{Error, Simulator, object_name_to_strings};

pub fn handle_query(sim: &mut Simulator, query: Box<Query>) -> Result<(), Error> {
    if let SetExpr::Select(select) = *query.body {
        let mut columns = vec![];

        // Collect all of the columns used in the SELECT.
        // TODO: Handle Aliasing
        for projection in select.projection {
            match projection {
                SelectItem::UnnamedExpr(expr) => {
                    if let Expr::Identifier(expr) = expr {
                        columns.push(expr.value.clone());
                    } else {
                        return Err(Error::Unsupported(format!(
                            "Unsupported Select Projection Expr: {expr}"
                        )));
                    }
                }
                SelectItem::ExprWithAlias { expr, alias } => {
                    return Err(Error::Unsupported(format!(
                        "Unsupported Select Expr with Alias: expr={expr}, alias={alias}"
                    )));
                }
                SelectItem::QualifiedWildcard(..) => {
                    return Err(Error::Unsupported(
                        "Unsupported Select Expr Qualified Wildcard".to_string(),
                    ));
                }
                SelectItem::Wildcard(_) => {}
            }
        }

        let from = select.from.first().unwrap();
        // TODO: Support Joins.

        match &from.relation {
            TableFactor::Table { name, .. } => {
                let n = object_name_to_strings(name).first().unwrap().clone();

                // Ensure the table exists.
                let table = sim.tables.get(&n).ok_or(Error::TableDoesntExist(n))?;

                // Ensure that SELECT columns exist in Table.
                for column in columns {
                    if !table.columns.contains_key(&column) {
                        return Err(Error::ColumnDoesntExist(column));
                    }
                }
            }
            _ => {
                warn!(relation = %from.relation, "Unsupported Select Relation");
            }
        }

        // TODO: Validate WHERE clause.
    } else {
        warn!(query_type = %query.body, "Unsupported Drop");
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::*;

    #[test]
    fn select_wildcard_success() {
        let mut sim = Simulator::new(Box::new(GenericDialect {}));
        sim.execute("create table person (id int, name text)")
            .unwrap();

        sim.execute("select * from person;").unwrap();
        sim.execute("select * from person;").unwrap();
    }

    #[test]
    fn select_fields_success() {
        let mut sim = Simulator::new(Box::new(GenericDialect {}));
        sim.execute("create table person (id int, name text)")
            .unwrap();

        sim.execute("select id, name from person;").unwrap();
    }

    #[test]
    fn select_column_doesnt_exist() {
        let mut sim = Simulator::new(Box::new(GenericDialect {}));
        sim.execute("create table person (id int, name text)")
            .unwrap();

        assert_eq!(
            sim.execute("select id, weight from person;"),
            Err(Error::ColumnDoesntExist("weight".to_string()))
        )
    }

    #[test]
    fn select_from_missing_table() {
        let mut sim = Simulator::new(Box::new(GenericDialect {}));
        assert_eq!(
            sim.execute("select * from person;"),
            Err(Error::TableDoesntExist("person".to_string()))
        );
    }
}
