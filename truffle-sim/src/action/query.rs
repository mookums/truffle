use std::collections::HashSet;

use sqlparser::ast::{Expr, Query, SelectItem, SetExpr, TableFactor};
use tracing::warn;

use crate::{Error, Simulator, object_name_to_strings};

pub enum SelectColumn {
    /// This is the true name of the column.)
    Unqualified(String),
    /// This is an absolute qualified (eg. table.col)
    Absolute { table: String, column: String },
    /// Aliased (eg.p.col) where p is mapped to a table later
    Aliased { alias: String, column: String },
}

pub struct SelectTable {
    name: String,
    alias: Option<String>,
}

pub fn handle_query(sim: &mut Simulator, query: Box<Query>) -> Result<(), Error> {
    if let SetExpr::Select(select) = *query.body {
        let mut columns = vec![];
        let mut tables = vec![];

        // Collect all of the columns used in the SELECT.

        // Ensure we have a FROM clause.
        if select.from.is_empty() {
            return Err(Error::Sql("Missing FROM clause on SELECT".to_string()));
        }

        for from in select.from {
            match &from.relation {
                TableFactor::Table { name, alias, .. } => {
                    let name = object_name_to_strings(name).first().unwrap().clone();
                    let alias = alias.as_ref().map(|a| a.name.value.clone());

                    // Ensure the table exists.
                    if !sim.tables.contains_key(&name) {
                        return Err(Error::TableDoesntExist(name));
                    }

                    // Ensure that the alias isn't a table name.
                    if let Some(alias) = &alias {
                        if sim.has_table(alias) {
                            return Err(Error::AliasIsTableName(alias.to_string()));
                        }
                    }

                    tables.push(SelectTable { name, alias });
                }
                _ => {
                    warn!(relation = %from.relation, "Unsupported Select Relation");
                }
            }
        }

        for projection in select.projection {
            match projection {
                SelectItem::UnnamedExpr(expr) => match expr {
                    Expr::Identifier(expr) => {
                        let value = expr.value.clone();
                        columns.push(SelectColumn::Unqualified(value));
                    }
                    Expr::CompoundIdentifier(idents) => {
                        let table_or_alias = &idents.first().unwrap().value;
                        let column_name = &idents.get(1).unwrap().value;

                        // If the identifier matches a table in the FROM clause,
                        // we treat this as an absolute column.
                        //
                        // If the identifier matches an alias in the FROM clause,
                        // we treat this as an aliased column.
                        //
                        // Otherwise, this TableOrAliasDoesntExist gets returned.
                        if tables.iter().any(|t| &t.name == table_or_alias) {
                            columns.push(SelectColumn::Absolute {
                                table: table_or_alias.to_string(),
                                column: column_name.to_string(),
                            });
                        } else if tables
                            .iter()
                            .any(|t| t.alias.as_ref().is_some_and(|a| a == table_or_alias))
                        {
                            columns.push(SelectColumn::Aliased {
                                alias: table_or_alias.to_string(),
                                column: column_name.to_string(),
                            });
                        } else {
                            return Err(Error::TableOrAliasDoesntExist(table_or_alias.to_string()));
                        }
                    }
                    _ => {
                        return Err(Error::Unsupported(format!(
                            "Unsupported Select Expr: {expr:?}"
                        )));
                    }
                },
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
                SelectItem::Wildcard(_) => {
                    // verify that no none of the tables have the same columns
                    let mut column_names = HashSet::new();
                    for table in &tables {
                        let table = sim.get_table(&table.name).expect("The table must exist");
                        for column in table.columns.iter().map(|c| c.0) {
                            if !column_names.insert(column) {
                                return Err(Error::AmbigiousColumn(column.to_string()));
                            }
                        }
                    }
                }
            }
        }

        for column in columns.into_iter() {
            match column {
                SelectColumn::Unqualified(column) => {
                    // ensure it shows up EXACTLY once.
                    // otherwise, it is ambiguous.
                    let mut column_found = false;

                    for table in &tables {
                        let found = sim
                            .get_table(&table.name)
                            .expect("The table must exist here.")
                            .has_column(&column);

                        // Ensure that the unqualified column is unique.
                        if column_found && found {
                            return Err(Error::AmbigiousColumn(column));
                        } else if found {
                            column_found = true;
                        }
                    }

                    // If we haven't found it, it doesn't exist in any of the tables.
                    if !column_found {
                        return Err(Error::ColumnDoesntExist(column));
                    }
                }
                SelectColumn::Absolute { table, column } => {
                    if !sim
                        .get_table(&table)
                        .expect("The table must exist")
                        .has_column(&column)
                    {
                        return Err(Error::ColumnDoesntExist(column));
                    }
                }
                SelectColumn::Aliased { alias, column } => {
                    let table_name = tables
                        .iter()
                        .find(|t| t.alias.as_ref().is_some_and(|a| a == &alias))
                        .map(|t| &t.name)
                        .ok_or(Error::AliasDoesntExist(alias))?;

                    let table = sim
                        .get_table(table_name)
                        .ok_or(Error::TableDoesntExist(table_name.to_string()))?;

                    if !table.has_column(&column) {
                        return Err(Error::ColumnDoesntExist(column));
                    }
                }
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

    #[test]
    fn select_ambiguous_column() {
        let mut sim = Simulator::new(Box::new(GenericDialect {}));
        sim.execute("create table person (id int, name text)")
            .unwrap();
        sim.execute("create table orders (id int, total int)")
            .unwrap();

        assert_eq!(
            sim.execute("select id from person, orders"),
            Err(Error::AmbigiousColumn("id".to_string()))
        );
    }

    #[test]
    fn select_wildcard_ambiguous_column() {
        let mut sim = Simulator::new(Box::new(GenericDialect {}));
        sim.execute("create table person (id int, name text)")
            .unwrap();
        sim.execute("create table orders (id int, total int)")
            .unwrap();

        assert!(matches!(
            sim.execute("select * from person, orders"),
            Err(Error::AmbigiousColumn(_))
        ));
    }

    #[test]
    fn select_multiple_tables_wildcard() {
        let mut sim = Simulator::new(Box::new(GenericDialect {}));
        sim.execute("create table person (id int, name text)")
            .unwrap();
        sim.execute("create table orders (order_id int, total int)")
            .unwrap();
        sim.execute("select * from person, orders").unwrap();
    }

    #[test]
    fn select_multiple_tables_unique_columns() {
        let mut sim = Simulator::new(Box::new(GenericDialect {}));
        sim.execute("create table person (id int, name text)")
            .unwrap();
        sim.execute("create table orders (order_id int, total int)")
            .unwrap();
        sim.execute("select name, total from person, orders")
            .unwrap();
    }

    #[test]
    fn select_multiple_tables_column_doesnt_exist() {
        let mut sim = Simulator::new(Box::new(GenericDialect {}));
        sim.execute("create table person (id int, name text)")
            .unwrap();
        sim.execute("create table orders (order_id int, total int)")
            .unwrap();

        assert_eq!(
            sim.execute("select name, weight from person, orders"),
            Err(Error::ColumnDoesntExist("weight".to_string()))
        )
    }

    #[test]
    fn select_multiple_tables_with_aliases() {
        let mut sim = Simulator::new(Box::new(GenericDialect {}));
        sim.execute("create table person (id int, name text)")
            .unwrap();
        sim.execute("create table orders (order_id int, total int)")
            .unwrap();
        sim.execute("select p.name, o.total from person p, orders o")
            .unwrap();
    }

    #[test]
    fn select_multiple_tables_with_as_aliases() {
        let mut sim = Simulator::new(Box::new(GenericDialect {}));
        sim.execute("create table person (id int, name text)")
            .unwrap();
        sim.execute("create table orders (order_id int, total int)")
            .unwrap();
        sim.execute("select p.name, o.total from person AS p, orders AS o")
            .unwrap();
    }

    #[test]
    fn select_qualified_column_with_unknown_table() {
        let mut sim = Simulator::new(Box::new(GenericDialect {}));
        sim.execute("create table person (id int, name text)")
            .unwrap();

        assert_eq!(
            sim.execute("select unknown_table.id from person"),
            Err(Error::TableOrAliasDoesntExist("unknown_table".to_string()))
        );
    }

    #[test]
    fn select_qualified_column_with_unincluded_table() {
        let mut sim = Simulator::new(Box::new(GenericDialect {}));
        sim.execute("create table person (id int, name text)")
            .unwrap();

        sim.execute("create table order (id int, name text)")
            .unwrap();

        assert_eq!(
            sim.execute("select order.id from person"),
            Err(Error::TableOrAliasDoesntExist("order".to_string()))
        );
    }

    #[test]
    fn select_alias_conflicts_with_table_name() {
        let mut sim = Simulator::new(Box::new(GenericDialect {}));
        sim.execute("create table person (id int, name text)")
            .unwrap();
        sim.execute("create table orders (id int, total int)")
            .unwrap();

        assert_eq!(
            sim.execute("select person.name from orders person"),
            Err(Error::AliasIsTableName("person".to_string()))
        );
    }

    #[test]
    fn select_qualified_wildcard() {
        let mut sim = Simulator::new(Box::new(GenericDialect {}));
        sim.execute("create table person (id int, name text)")
            .unwrap();

        sim.execute("select person.* from person").unwrap();
    }

    #[test]
    fn select_qualified_wildcard_with_alias() {
        let mut sim = Simulator::new(Box::new(GenericDialect {}));
        sim.execute("create table person (id int, name text)")
            .unwrap();
        sim.execute("create table orders (id int, total int)")
            .unwrap();

        sim.execute("select p.* from person p, orders").unwrap();
    }

    #[test]
    fn select_qualified_wildcard_with_unknown_table() {
        let mut sim = Simulator::new(Box::new(GenericDialect {}));
        sim.execute("create table person (id int, name text)")
            .unwrap();

        assert_eq!(
            sim.execute("select unknown.* from person"),
            Err(Error::TableOrAliasDoesntExist("unknown".to_string()))
        );
    }

    #[test]
    fn select_qualified_wildcard_table_not_in_from() {
        let mut sim = Simulator::new(Box::new(GenericDialect {}));
        sim.execute("create table person (id int, name text)")
            .unwrap();
        sim.execute("create table orders (id int, total int)")
            .unwrap();

        assert_eq!(
            sim.execute("select orders.* from person"),
            Err(Error::TableOrAliasDoesntExist("orders".to_string()))
        );
    }

    #[test]
    fn select_multiple_qualified_wildcards() {
        let mut sim = Simulator::new(Box::new(GenericDialect {}));
        sim.execute("create table person (id int, name text)")
            .unwrap();
        sim.execute("create table orders (order_id int, total int)")
            .unwrap();

        sim.execute("select p.*, o.* from person p, orders o")
            .unwrap();
    }

    #[test]
    fn select_qualified_wildcard_with_columns() {
        let mut sim = Simulator::new(Box::new(GenericDialect {}));
        sim.execute("create table person (id int, name text)")
            .unwrap();
        sim.execute("create table orders (order_id int, total int)")
            .unwrap();

        sim.execute("select p.*, orders.total from person p, orders")
            .unwrap();
    }

    // #[test]
    // fn select_where_column_doesnt_exist() {
    //     let mut sim = Simulator::new(Box::new(GenericDialect {}));
    //     sim.execute("create table person (id int, name text)")
    //         .unwrap();

    //     assert_eq!(
    //         sim.execute("select name from person where weight = 100"),
    //         Err(Error::ColumnDoesntExist("weight".to_string()))
    //     );
    // }
}
