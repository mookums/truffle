use std::collections::HashSet;

use sqlparser::ast::{Expr, Select, SelectItem, SelectItemQualifiedWildcardKind, TableFactor};
use tracing::warn;

use crate::{Error, Simulator, object_name_to_strings};

pub enum SelectColumns {
    /// Selects all columns across the FROM clause.
    Wildcard,
    /// List of SelectColumn Expressions.
    List(Vec<SelectColumn>),
}

impl SelectColumns {
    pub fn expect_list_mut(&mut self) -> &mut Vec<SelectColumn> {
        let SelectColumns::List(list) = self else {
            unreachable!("columns must be List");
        };

        list
    }
}

pub enum SelectColumn {
    /// This is the true name of the column.)
    Unqualified(String),
    /// This is an absolute qualified (eg. table.col)
    Absolute { table: String, column: String },
    /// Aliased (eg.p.col) where p is mapped to a table later
    Aliased { alias: String, column: String },
    /// This is a qualified wildcard that uses the table name.
    AbsoluteWildcard(String),
    /// This is a wildcard that uses a table alias.
    AliasedWildcard(String),
}

pub struct SelectTable {
    name: String,
    alias: Option<String>,
}

enum TableOrAlias {
    Table,
    Alias,
}

/// Checks if a value is table or an alias.
/// Returns Error:TableOrAliasDoesntExist if it is neither.
///
/// If the identifier matches a table in the FROM clause,
/// we treat this as an absolute column.
///
/// If the identifier matches an alias in the FROM clause,
/// we treat this as an aliased column.
///
/// Otherwise, this TableOrAliasDoesntExist gets returned.
fn check_table_or_alias(
    table_or_alias: &str,
    tables: &[SelectTable],
) -> Result<TableOrAlias, Error> {
    if tables.iter().any(|t| t.name == table_or_alias) {
        Ok(TableOrAlias::Table)
    } else if tables
        .iter()
        .any(|t| t.alias.as_ref().is_some_and(|a| a == table_or_alias))
    {
        Ok(TableOrAlias::Alias)
    } else {
        Err(Error::TableOrAliasDoesntExist(table_or_alias.to_string()))
    }
}

fn validate_where_expr(
    expr: &Expr,
    sim: &mut Simulator,
    tables: &Vec<SelectTable>,
) -> Result<(), Error> {
    // TODO: validate types on expressions.
    match expr {
        Expr::Value(_)
        | Expr::IsTrue(_)
        | Expr::IsNotTrue(_)
        | Expr::IsFalse(_)
        | Expr::IsNotFalse(_)
        | Expr::IsNull(_)
        | Expr::IsNotNull(_) => {}
        Expr::Identifier(ident) => {
            let name = &ident.value;

            if !has_unqualified_column(sim, tables, name)? {
                return Err(Error::ColumnDoesntExist(name.to_string()));
            }
        }
        Expr::CompoundIdentifier(idents) => {
            // validate that identifier is a column.
            let table_or_alias = &idents.first().unwrap().value;
            let column_name = &idents.get(1).unwrap().value;

            match check_table_or_alias(table_or_alias, tables)? {
                TableOrAlias::Table => {
                    if !sim
                        .get_table(table_or_alias)
                        .unwrap()
                        .has_column(column_name)
                    {
                        return Err(Error::ColumnDoesntExist(column_name.to_string()));
                    }
                }
                TableOrAlias::Alias => {
                    let table_name = &tables
                        .iter()
                        .find(|t| t.alias.as_ref().is_some_and(|a| a == table_or_alias))
                        .ok_or(Error::AliasDoesntExist(table_or_alias.to_string()))?
                        .name;

                    if !sim.get_table(table_name).unwrap().has_column(column_name) {
                        return Err(Error::ColumnDoesntExist(column_name.to_string()));
                    }
                }
            }
        }
        Expr::BinaryOp { left, right, .. } => {
            validate_where_expr(left, sim, tables)?;
            validate_where_expr(right, sim, tables)?;
        }
        Expr::UnaryOp { expr, .. } => {
            validate_where_expr(expr, sim, tables)?;
        }
        Expr::Nested(expr) => {
            validate_where_expr(expr, sim, tables)?;
        }
        Expr::InList { expr, list, .. } => {
            validate_where_expr(expr, sim, tables)?;
            for item in list {
                validate_where_expr(item, sim, tables)?;
            }
        }
        Expr::Case {
            operand,
            conditions,
            else_result,
            ..
        } => {
            if let Some(op) = operand {
                validate_where_expr(op, sim, tables)?;
            }

            for condition in conditions {
                validate_where_expr(&condition.condition, sim, tables)?;
                validate_where_expr(&condition.result, sim, tables)?;
            }

            if let Some(else_result) = else_result {
                validate_where_expr(else_result, sim, tables)?;
            }
        }
        Expr::Between {
            expr, low, high, ..
        } => {
            validate_where_expr(expr, sim, tables)?;
            validate_where_expr(low, sim, tables)?;
            validate_where_expr(high, sim, tables)?;
        }
        _ => {
            return Err(Error::Unsupported(format!(
                "Unsupported WHERE expr: {expr:#?}"
            )));
        }
    }

    Ok(())
}

/// This checks through all of the tables to:
/// 1. Ensure that a column with the name 'column' exists.
/// 2. That it is unique with no other columns with the same name.
///
/// If 1 fails, we return false.
/// If 2 fails, we return an Error::AmbigiousColumn.
/// Else, we return true.
fn has_unqualified_column(
    sim: &Simulator,
    tables: &[SelectTable],
    column: &str,
) -> Result<bool, Error> {
    let mut column_found = false;

    for table in tables {
        let found = sim
            .get_table(&table.name)
            .expect("The table must exist here.")
            .has_column(column);

        // Ensure that the unqualified column is unique.
        if column_found && found {
            return Err(Error::AmbigiousColumn(column.to_string()));
        } else if found {
            column_found = true;
        }
    }

    Ok(column_found)
}

pub fn handle_select_query(sim: &mut Simulator, select: &Select) -> Result<(), Error> {
    let mut columns = SelectColumns::List(vec![]);
    let mut tables = vec![];

    // Ensure we have a FROM clause.
    if select.from.is_empty() {
        return Err(Error::Sql("Missing FROM clause on SELECT".to_string()));
    }

    for from in &select.from {
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

    for projection in &select.projection {
        match projection {
            SelectItem::UnnamedExpr(expr) => match expr {
                Expr::Identifier(ident) => {
                    let value = ident.value.clone();

                    columns
                        .expect_list_mut()
                        .push(SelectColumn::Unqualified(value));
                }
                Expr::CompoundIdentifier(idents) => {
                    let table_or_alias = &idents.first().unwrap().value;
                    let column_name = &idents.get(1).unwrap().value;

                    match check_table_or_alias(table_or_alias, &tables)? {
                        TableOrAlias::Table => {
                            columns.expect_list_mut().push(SelectColumn::Absolute {
                                table: table_or_alias.to_string(),
                                column: column_name.to_string(),
                            });
                        }
                        TableOrAlias::Alias => {
                            columns.expect_list_mut().push(SelectColumn::Aliased {
                                alias: table_or_alias.to_string(),
                                column: column_name.to_string(),
                            });
                        }
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
            SelectItem::QualifiedWildcard(kind, _) => match kind {
                SelectItemQualifiedWildcardKind::ObjectName(name) => {
                    let table_or_alias = object_name_to_strings(name).first().unwrap().clone();

                    match check_table_or_alias(&table_or_alias, &tables)? {
                        TableOrAlias::Table => {
                            columns
                                .expect_list_mut()
                                .push(SelectColumn::AbsoluteWildcard(table_or_alias));
                        }
                        TableOrAlias::Alias => {
                            columns
                                .expect_list_mut()
                                .push(SelectColumn::AliasedWildcard(table_or_alias));
                        }
                    }
                }
                SelectItemQualifiedWildcardKind::Expr(_) => {
                    return Err(Error::Unsupported(
                        "Expression as qualifier for wildcard in SELECT".to_string(),
                    ));
                }
            },
            SelectItem::Wildcard(_) => {
                columns = SelectColumns::Wildcard;
                break;
            }
        }
    }

    match columns {
        SelectColumns::Wildcard => {
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
        SelectColumns::List(list) => {
            for column in list.into_iter() {
                match column {
                    SelectColumn::Unqualified(column) => {
                        if !has_unqualified_column(sim, &tables, &column)? {
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
                    SelectColumn::AliasedWildcard(alias) => {
                        let table_name = tables
                            .iter()
                            .find(|t| t.alias.as_ref().is_some_and(|a| a == &alias))
                            .map(|t| &t.name)
                            .ok_or(Error::AliasDoesntExist(alias))?;

                        if sim.get_table(table_name).is_none() {
                            return Err(Error::TableDoesntExist(table_name.clone()));
                        }
                    }
                    SelectColumn::AbsoluteWildcard(table) => {
                        if sim.get_table(&table).is_none() {
                            return Err(Error::TableDoesntExist(table));
                        }
                    }
                }
            }
        }
    }

    // Validate WHERE clause.
    if let Some(selection) = &select.selection {
        validate_where_expr(selection, sim, &tables)?;
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
        sim.execute("create table orders (id int, total int)")
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

    #[test]
    fn select_where() {
        let mut sim = Simulator::new(Box::new(GenericDialect {}));
        sim.execute("create table person (id int, name text)")
            .unwrap();

        sim.execute("select name from person where (name = 'abc')")
            .unwrap();
    }

    #[test]
    fn select_where_column_doesnt_exist() {
        let mut sim = Simulator::new(Box::new(GenericDialect {}));
        sim.execute("create table person (id int, name text)")
            .unwrap();

        assert_eq!(
            sim.execute("select name from person where weight = 100"),
            Err(Error::ColumnDoesntExist("weight".to_string()))
        );
    }

    #[test]
    fn select_where_simple_comparison() {
        let mut sim = Simulator::new(Box::new(GenericDialect {}));
        sim.execute("create table person (id int, name text, age int)")
            .unwrap();
        sim.execute("select name from person where age > 18")
            .unwrap();
        sim.execute("select name from person where id = 1").unwrap();
    }

    #[test]
    fn select_where_logical_operators() {
        let mut sim = Simulator::new(Box::new(GenericDialect {}));
        sim.execute("create table person (id int, name text, age int)")
            .unwrap();
        sim.execute("select name from person where age > 18 AND name = 'John'")
            .unwrap();
        sim.execute("select name from person where age < 18 OR age > 65")
            .unwrap();
        sim.execute("select name from person where NOT (age = 25)")
            .unwrap();
    }

    #[test]
    fn select_where_qualified_columns() {
        let mut sim = Simulator::new(Box::new(GenericDialect {}));
        sim.execute("create table person (id int, name text)")
            .unwrap();
        sim.execute("create table company (id int, name text)")
            .unwrap();
        sim.execute("select person.name from person, company where person.id = company.id")
            .unwrap();
    }

    #[test]
    fn select_where_ambiguous_column() {
        let mut sim = Simulator::new(Box::new(GenericDialect {}));
        sim.execute("create table person (id int, name text)")
            .unwrap();
        sim.execute("create table company (id int, name text)")
            .unwrap();
        assert_eq!(
            sim.execute("select person.name from person, company where id = 1"),
            Err(Error::AmbigiousColumn("id".to_string()))
        );
    }

    #[test]
    fn select_where_with_aliases() {
        let mut sim = Simulator::new(Box::new(GenericDialect {}));
        sim.execute("create table person (id int, name text)")
            .unwrap();
        sim.execute("create table company (id int, name text)")
            .unwrap();
        sim.execute("select p.name from person p, company c where p.id = c.id")
            .unwrap();
    }

    #[test]
    fn select_where_nested_expressions() {
        let mut sim = Simulator::new(Box::new(GenericDialect {}));
        sim.execute("create table person (id int, name text, age int)")
            .unwrap();
        sim.execute("select name from person where ((age > 18 AND age < 65) OR name = 'Admin')")
            .unwrap();
    }

    #[test]
    fn select_where_invalid_qualified_column() {
        let mut sim = Simulator::new(Box::new(GenericDialect {}));
        sim.execute("create table person (id int, name text)")
            .unwrap();
        assert_eq!(
            sim.execute("select name from person where person.weight = 100"),
            Err(Error::ColumnDoesntExist("weight".to_string()))
        );
    }

    #[test]
    fn select_where_invalid_table_reference() {
        let mut sim = Simulator::new(Box::new(GenericDialect {}));
        sim.execute("create table person (id int, name text)")
            .unwrap();
        assert_eq!(
            sim.execute("select name from person where company.id = 1"),
            Err(Error::TableOrAliasDoesntExist("company".to_string()))
        );
    }

    #[test]
    fn select_where_invalid_alias() {
        let mut sim = Simulator::new(Box::new(GenericDialect {}));
        sim.execute("create table person (id int, name text)")
            .unwrap();
        assert_eq!(
            sim.execute("select name from person p where x.id = 1"),
            Err(Error::TableOrAliasDoesntExist("x".to_string()))
        );
    }
}
