use std::collections::HashSet;

use sqlparser::ast::{
    Expr, JoinConstraint, JoinOperator, Select, SelectItem, SelectItemQualifiedWildcardKind,
    TableFactor,
};

use crate::{
    Error, Simulator, column::Column, expr::ColumnInferrer, object_name_to_strings, ty::SqlType,
};

impl Simulator {
    pub(crate) fn select(&self, sel: &Select) -> Result<(), Error> {
        let mut columns = SelectColumns::List(vec![]);
        let mut tables = vec![];

        // Ensure we have a FROM clause.
        if sel.from.is_empty() {
            return Err(Error::Sql("Missing FROM clause on SELECT".to_string()));
        }

        for from in &sel.from {
            let TableFactor::Table { name, alias, .. } = &from.relation else {
                return Err(Error::Unsupported(
                    "Unsupported SELECT relation".to_string(),
                ));
            };

            let name = object_name_to_strings(name).first().unwrap().clone();
            let alias = alias.as_ref().map(|a| a.name.value.clone());

            // Ensure the table exists.
            let table = self
                .get_table(&name)
                .ok_or_else(|| Error::TableDoesntExist(name.clone()))?;

            // Ensure that the alias isn't a table name.
            if let Some(alias) = &alias {
                if self.has_table(alias) {
                    return Err(Error::AliasIsTableName(alias.to_string()));
                }
            }

            tables.push(SelectTable { name, alias });

            // TODO: This needs to properly track evolving joins.
            // This means that "table" needs to basically be mutable and represent a
            // temporary table that is the previous join's result table.
            //
            // This currently only supports a single JOIN where the left is the original table.
            for join in &from.joins {
                match &join.relation {
                    TableFactor::Table { name, alias, .. } => {
                        let name = object_name_to_strings(name).first().unwrap().clone();
                        let alias = alias.as_ref().map(|a| a.name.value.clone());

                        let right_table = self
                            .get_table(&name)
                            .ok_or_else(|| Error::TableDoesntExist(name.clone()))?;

                        if let Some(alias) = &alias {
                            if self.has_table(alias) {
                                return Err(Error::AliasIsTableName(alias.to_string()));
                            }
                        }

                        tables.push(SelectTable {
                            name: name.clone(),
                            alias,
                        });

                        let inferrer = SelectInferrer { tables: &tables };

                        match &join.join_operator {
                            JoinOperator::Join(join_constraint)
                            | JoinOperator::Inner(join_constraint) => match join_constraint {
                                JoinConstraint::On(expr) => {
                                    let ty = self.infer_expr_type(
                                        expr,
                                        Some(SqlType::Boolean),
                                        &inferrer,
                                    )?;

                                    if ty != SqlType::Boolean {
                                        return Err(Error::TypeMismatch {
                                            expected: SqlType::Boolean,
                                            got: ty,
                                        });
                                    }
                                }
                                JoinConstraint::Using(object_names) => {
                                    for object_name in object_names {
                                        let column_name = object_name_to_strings(object_name)
                                            .first()
                                            .unwrap()
                                            .clone();

                                        if inferrer
                                            .infer_unqualified_type(self, &column_name)?
                                            .is_none()
                                        {
                                            return Err(Error::ColumnDoesntExist(column_name));
                                        }
                                    }
                                }
                                JoinConstraint::Natural => {
                                    let mut found_common_column = false;
                                    for (column_name, column) in &table.columns {
                                        if let Some(right_column) =
                                            right_table.get_column(column_name)
                                        {
                                            if column.ty == right_column.ty {
                                                found_common_column = true;
                                                break;
                                            } else {
                                                return Err(Error::TypeMismatch {
                                                    expected: column.ty.clone(),
                                                    got: right_column.ty.clone(),
                                                });
                                            }
                                        }
                                    }

                                    if !found_common_column {
                                        return Err(Error::NoCommonColumn);
                                    }
                                }
                                JoinConstraint::None => {}
                            },
                            _ => todo!(),
                        }
                    }
                    _ => return Err(Error::Unsupported("Unsupported JOIN relation".to_string())),
                }
            }
        }

        let inferrer = SelectInferrer { tables: &tables };

        for projection in &sel.projection {
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

                        match check_table_or_alias(&tables, table_or_alias)? {
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

                        match check_table_or_alias(&tables, &table_or_alias)? {
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
                    let table = self.get_table(&table.name).expect("The table must exist");
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
                            if inferrer.infer_unqualified_type(self, &column)?.is_none() {
                                return Err(Error::ColumnDoesntExist(column));
                            }
                        }
                        SelectColumn::Absolute { table, column } => {
                            if !self
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

                            let table = self
                                .get_table(table_name)
                                .ok_or_else(|| Error::TableDoesntExist(table_name.to_string()))?;

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

                            if self.get_table(table_name).is_none() {
                                return Err(Error::TableDoesntExist(table_name.clone()));
                            }
                        }
                        SelectColumn::AbsoluteWildcard(table) => {
                            if self.get_table(&table).is_none() {
                                return Err(Error::TableDoesntExist(table));
                            }
                        }
                    }
                }
            }
        }

        // Validate WHERE clause.
        if let Some(selection) = &sel.selection {
            let ty = self.infer_expr_type(selection, Some(SqlType::Boolean), &inferrer)?;
            if ty != SqlType::Boolean {
                return Err(Error::TypeMismatch {
                    expected: SqlType::Boolean,
                    got: ty,
                });
            }
        }

        Ok(())
    }
}

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

struct SelectInferrer<'a> {
    tables: &'a [SelectTable],
}

impl<'a> ColumnInferrer for SelectInferrer<'a> {
    fn infer_unqualified_type(
        &self,
        sim: &Simulator,
        column: &str,
    ) -> Result<Option<SqlType>, Error> {
        let mut found_column: Option<Column> = None;

        for table in self.tables {
            if let Some(col) = sim
                .get_table(&table.name)
                .expect("The table must exist here")
                .get_column(column)
            {
                match found_column {
                    // Ensure that the unqualified column is unique.
                    Some(_) => return Err(Error::AmbigiousColumn(column.to_string())),
                    None => found_column = Some(col.clone()),
                }
            };
        }

        Ok(found_column.map(|c| c.ty))
    }

    fn infer_qualified_type(
        &self,
        sim: &Simulator,
        qualifier: &str,
        column: &str,
    ) -> Result<SqlType, Error> {
        match check_table_or_alias(self.tables, qualifier)? {
            TableOrAlias::Table => {}
            TableOrAlias::Alias => {}
        }

        match check_table_or_alias(self.tables, qualifier)? {
            TableOrAlias::Table => {
                let column = sim
                    .get_table(qualifier)
                    .unwrap()
                    .get_column(column)
                    .ok_or_else(|| Error::ColumnDoesntExist(column.to_string()))?;

                Ok(column.ty.clone())
            }
            TableOrAlias::Alias => {
                let table_name = &self
                    .tables
                    .iter()
                    .find(|t| t.alias.as_ref().is_some_and(|a| a == qualifier))
                    .ok_or_else(|| Error::AliasDoesntExist(qualifier.to_string()))?
                    .name;

                let column = sim
                    .get_table(table_name)
                    .unwrap()
                    .get_column(column)
                    .ok_or_else(|| Error::ColumnDoesntExist(column.to_string()))?;

                Ok(column.ty.clone())
            }
        }
    }
}

enum TableOrAlias {
    Table,
    Alias,
}

fn check_table_or_alias(tables: &[SelectTable], name: &str) -> Result<TableOrAlias, Error> {
    if tables.iter().any(|t| t.name == name) {
        Ok(TableOrAlias::Table)
    } else if tables
        .iter()
        .any(|t| t.alias.as_ref().is_some_and(|a| a == name))
    {
        Ok(TableOrAlias::Alias)
    } else {
        Err(Error::TableOrAliasDoesntExist(name.to_string()))
    }
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
        sim.execute(
            r#"
                select name from person where
                    ((age > 18 AND age < 65)
                    OR name = 'Admin'
                    or (name is null or age is not null))
            "#,
        )
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

    #[test]
    fn select_where_invalid_type() {
        let mut sim = Simulator::new(Box::new(GenericDialect {}));
        sim.execute("create table person (id int, name text)")
            .unwrap();
        assert_eq!(
            sim.execute("select name from person p where p.id = false"),
            Err(Error::TypeMismatch {
                expected: SqlType::Integer,
                got: SqlType::Boolean
            })
        );
    }

    #[test]
    fn select_where_invalid_type_string_with_int() {
        let mut sim = Simulator::new(Box::new(GenericDialect {}));
        sim.execute("create table person (id int, name text)")
            .unwrap();
        assert_eq!(
            sim.execute("select name from person where id = 'hello'"),
            Err(Error::TypeMismatch {
                expected: SqlType::Integer,
                got: SqlType::Text
            })
        );
    }

    #[test]
    fn select_where_invalid_type_bool_with_text() {
        let mut sim = Simulator::new(Box::new(GenericDialect {}));
        sim.execute("create table person (id int, name text)")
            .unwrap();
        assert_eq!(
            sim.execute("select name from person where name = true"),
            Err(Error::TypeMismatch {
                expected: SqlType::Text,
                got: SqlType::Boolean
            })
        );
    }

    #[test]
    fn select_where_invalid_type_int_with_bool() {
        let mut sim = Simulator::new(Box::new(GenericDialect {}));
        sim.execute("create table person (active bool)").unwrap();
        assert_eq!(
            sim.execute("select * from person where active = 123"),
            Err(Error::TypeMismatch {
                expected: SqlType::Boolean,
                got: SqlType::SmallInt
            })
        );
    }

    #[test]
    fn select_where_invalid_type_arithmetic() {
        let mut sim = Simulator::new(Box::new(GenericDialect {}));
        sim.execute("create table person (id int)").unwrap();
        assert_eq!(
            sim.execute("select * from person where id + 'hello' > 10"),
            Err(Error::TypeMismatch {
                expected: SqlType::Integer,
                got: SqlType::Text
            })
        );
    }

    #[test]
    fn select_where_invalid_type_comparison() {
        let mut sim = Simulator::new(Box::new(GenericDialect {}));
        sim.execute("create table person (id int)").unwrap();
        assert_eq!(
            sim.execute("select * from person where id > 'five'"),
            Err(Error::TypeMismatch {
                expected: SqlType::Integer,
                got: SqlType::Text
            })
        );
    }

    #[test]
    fn select_where_invalid_type_logical_and() {
        let mut sim = Simulator::new(Box::new(GenericDialect {}));
        sim.execute("create table person (id int, active bool)")
            .unwrap();
        assert_eq!(
            sim.execute("select * from person where active AND id"),
            Err(Error::TypeMismatch {
                expected: SqlType::Boolean,
                got: SqlType::Integer
            })
        );
    }

    #[test]
    fn select_where_invalid_type_not_operator() {
        let mut sim = Simulator::new(Box::new(GenericDialect {}));
        sim.execute("create table person (id int)").unwrap();
        assert_eq!(
            sim.execute("select * from person where NOT id"),
            Err(Error::TypeMismatch {
                expected: SqlType::Boolean,
                got: SqlType::Integer
            })
        );
    }

    #[test]
    fn select_where_invalid_type_unary_minus() {
        let mut sim = Simulator::new(Box::new(GenericDialect {}));
        sim.execute("create table person (active bool)").unwrap();
        assert_eq!(
            sim.execute("select * from person where -active"),
            Err(Error::TypeNotNumeric(SqlType::Boolean))
        );
    }

    #[test]
    fn select_where_invalid_type_in_list() {
        let mut sim = Simulator::new(Box::new(GenericDialect {}));
        sim.execute("create table person (id int)").unwrap();
        assert_eq!(
            sim.execute("select * from person where id IN (1, 'two', 3)"),
            Err(Error::TypeMismatch {
                expected: SqlType::Integer,
                got: SqlType::Text
            })
        );
    }

    #[test]
    fn select_where_invalid_type_is_true() {
        let mut sim = Simulator::new(Box::new(GenericDialect {}));
        sim.execute("create table person (id int)").unwrap();
        assert_eq!(
            sim.execute("select * from person where id IS TRUE"),
            Err(Error::TypeMismatch {
                expected: SqlType::Boolean,
                got: SqlType::Integer
            })
        );
    }

    #[test]
    fn select_where_type_mismatch_expr() {
        let mut sim = Simulator::new(Box::new(GenericDialect {}));
        sim.execute("create table person (id int)").unwrap();
        assert_eq!(
            sim.execute("select * from person where 10 + 20"),
            Err(Error::TypeMismatch {
                expected: SqlType::Boolean,
                got: SqlType::SmallInt
            })
        );
    }

    #[test]
    fn select_join_basic() {
        let mut sim = Simulator::new(Box::new(GenericDialect {}));
        sim.execute("create table person (id int primary key, name text)")
            .unwrap();
        sim.execute(
            "create table order (id int primary key, person_id int references person(id), total float)",
        )
        .unwrap();

        sim.execute("select person.* from person join order on person.id = order.person_id")
            .unwrap()
    }

    #[test]
    fn select_join_on_type_mismatch() {
        let mut sim = Simulator::new(Box::new(GenericDialect {}));
        sim.execute("create table person (id int primary key, name text)")
            .unwrap();
        sim.execute(
            "create table order (id int primary key, person_id int references person(id), total float)",
        )
        .unwrap();

        assert_eq!(
            sim.execute("select person.* from person join order on person.id = order.total"),
            Err(Error::TypeMismatch {
                expected: SqlType::Integer,
                got: SqlType::Float
            })
        );
    }

    #[test]
    fn select_join_on_type_mismatch_on() {
        let mut sim = Simulator::new(Box::new(GenericDialect {}));
        sim.execute("create table person (id int primary key, name text)")
            .unwrap();
        sim.execute(
            "create table order (id int primary key, person_id int references person(id), total float)",
        )
        .unwrap();

        assert_eq!(
            sim.execute("select person.* from person join order on person.id + 3"),
            Err(Error::TypeMismatch {
                expected: SqlType::Boolean,
                got: SqlType::Integer
            })
        );
    }

    #[test]
    fn select_join_ambigious_column() {
        let mut sim = Simulator::new(Box::new(GenericDialect {}));
        sim.execute("create table person (id int primary key, name text)")
            .unwrap();
        sim.execute(
            "create table order (order_id int primary key, name text, person_id int references person(id), total float)",
        )
        .unwrap();

        assert_eq!(
            sim.execute("select name from person natural join order"),
            Err(Error::AmbigiousColumn("name".to_string()))
        );
    }

    #[test]
    fn select_join_natural() {
        let mut sim = Simulator::new(Box::new(GenericDialect {}));
        sim.execute("create table person (id int primary key, cart_id int, name text)")
            .unwrap();
        sim.execute(
            "create table order (id int primary key, cart_id int, person_id int references person(id), total float)",
        )
        .unwrap();

        sim.execute("select order.* from person natural join order")
            .unwrap();
    }

    #[test]
    fn select_join_natural_no_common_column() {
        let mut sim = Simulator::new(Box::new(GenericDialect {}));
        sim.execute("create table person (id int primary key, name text)")
            .unwrap();
        sim.execute(
            "create table order (order_id int primary key, person_id int references person(id), total float)",
        )
        .unwrap();

        assert_eq!(
            sim.execute("select order.* from person natural join order"),
            Err(Error::NoCommonColumn)
        );
    }
}
