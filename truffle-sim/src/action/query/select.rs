use std::collections::HashSet;

use itertools::Itertools;
use sqlparser::ast::{Expr, Select, SelectItem, SelectItemQualifiedWildcardKind, TableFactor};

use crate::{
    Error, Simulator,
    action::join::{JoinContext, JoinInferrer},
    expr::ColumnInferrer,
    object_name_to_strings,
    ty::SqlType,
};

impl Simulator {
    pub(crate) fn select(&self, sel: &Select) -> Result<(), Error> {
        let mut columns = SelectColumns::List(vec![]);
        let mut contexts = vec![];

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

            let from_table_name = object_name_to_strings(name).first().unwrap().clone();
            let from_table_alias = alias.as_ref().map(|a| a.name.value.clone());

            // Ensure the table exists.
            let from_table = self
                .get_table(&from_table_name)
                .ok_or_else(|| Error::TableDoesntExist(from_table_name.clone()))?;

            // Ensure that the alias isn't a table name.
            if let Some(alias) = &from_table_alias {
                if self.has_table(alias) {
                    return Err(Error::AliasIsTableName(alias.to_string()));
                }
            }

            let join_table = self.infer_joins(
                from_table,
                &from_table_name,
                from_table_alias.as_ref(),
                &from.joins,
            )?;

            contexts.push(join_table);
        }

        let inferrer = JoinInferrer {
            join_contexts: &contexts,
        };

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

                        match check_table_or_alias(&contexts, table_or_alias)? {
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

                        match check_table_or_alias(&contexts, &table_or_alias)? {
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
                let mut all_columns = HashSet::new();

                for context in &contexts {
                    for (col_ref, _) in context.refs.iter().unique_by(|r| *r.1) {
                        let column = &col_ref.name;

                        if all_columns.contains(column) {
                            return Err(Error::AmbiguousColumn(column.to_string()));
                        } else {
                            all_columns.insert(column.to_string());
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
                            if !contexts
                                .iter()
                                .any(|c| c.has_column_in_table(&table, &column))
                            {
                                return Err(Error::ColumnDoesntExist(column));
                            }
                        }
                        SelectColumn::Aliased { alias, column } => {
                            let table_name = contexts
                                .iter()
                                .find_map(|c| c.aliases.get(&alias))
                                .ok_or(Error::AliasDoesntExist(alias))?;

                            if !contexts.iter().any(|c| c.has_table(table_name)) {
                                return Err(Error::TableDoesntExist(table_name.clone()));
                            }

                            if !contexts.iter().any(|c| c.has_column(&column)) {
                                return Err(Error::ColumnDoesntExist(column));
                            }
                        }
                        SelectColumn::AliasedWildcard(alias) => {
                            let table_name = contexts
                                .iter()
                                .find_map(|c| c.aliases.get(&alias))
                                .ok_or(Error::AliasDoesntExist(alias))?;

                            if !contexts.iter().any(|c| c.has_table(table_name)) {
                                return Err(Error::TableDoesntExist(table_name.clone()));
                            }
                        }
                        SelectColumn::AbsoluteWildcard(table) => {
                            if !contexts
                                .iter()
                                .any(|c| c.refs.iter().any(|(r, _)| r.qualifier == table))
                            {
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

enum TableOrAlias {
    Table,
    Alias,
}

fn check_table_or_alias(ctx: &[JoinContext], name: &str) -> Result<TableOrAlias, Error> {
    if ctx
        .iter()
        .any(|c| c.refs.iter().any(|(r, _)| r.qualifier == name))
    {
        Ok(TableOrAlias::Table)
    } else if ctx.iter().any(|c| c.aliases.contains_key(name)) {
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
            Err(Error::AmbiguousColumn("id".to_string()))
        );
    }

    #[test]
    fn select_wildcard_ambiguous_column() {
        let mut sim = Simulator::new(Box::new(GenericDialect {}));
        sim.execute("create table person (id int, name text)")
            .unwrap();
        sim.execute("create table orders (id int, total int)")
            .unwrap();

        assert_eq!(
            sim.execute("select * from person, orders"),
            Err(Error::AmbiguousColumn("id".to_string()))
        );
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
            Err(Error::AmbiguousColumn("id".to_string()))
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
    fn select_join_chain_basic() {
        let mut sim = Simulator::new(Box::new(GenericDialect {}));
        sim.execute("create table users (id int primary key, name text)")
            .unwrap();
        sim.execute("create table orders (id int primary key, user_id int, product_id int)")
            .unwrap();
        sim.execute("create table products (id int primary key, name text, price float)")
            .unwrap();

        sim.execute(
            "select users.name, o.id, products.name 
             from users 
             join orders o on users.id = orders.user_id 
             join products on orders.product_id = products.id",
        )
        .unwrap();
    }

    #[test]
    fn select_join_chain_ambiguous() {
        let mut sim = Simulator::new(Box::new(GenericDialect {}));
        sim.execute("create table users (id int primary key, name text)")
            .unwrap();
        sim.execute("create table orders (id int primary key, user_id int)")
            .unwrap();
        sim.execute("create table products (id int primary key, name text)")
            .unwrap();

        assert_eq!(
            sim.execute(
                "select id from users 
                 join orders on users.id = orders.user_id 
                 join products on orders.id = products.id"
            ),
            Err(Error::AmbiguousColumn("id".to_string()))
        );
    }

    #[test]
    fn select_join_chain_wildcard() {
        let mut sim = Simulator::new(Box::new(GenericDialect {}));
        sim.execute("create table users (id int primary key, name text)")
            .unwrap();
        sim.execute("create table orders (id int primary key, user_id int, product_id int)")
            .unwrap();
        sim.execute("create table products (id int primary key, name text)")
            .unwrap();

        sim.execute(
            "select o1.*, o2.product_id from
                users join orders o1 on users.id = orders.user_id,
                products join orders o2 on products.id = orders.product_id ",
        )
        .unwrap();
    }

    #[test]
    fn select_join_chain_table_out_of_scope() {
        let mut sim = Simulator::new(Box::new(GenericDialect {}));
        sim.execute("create table users (id int primary key, name text)")
            .unwrap();
        sim.execute("create table orders (id int primary key, user_id int, product_id int)")
            .unwrap();
        sim.execute("create table products (id int primary key, name text)")
            .unwrap();

        assert_eq!(
            sim.execute(
                "select orders.* from
                users join orders o on users.id = orders.user_id,
                products join orders on users.id = orders.product_id ",
            ),
            Err(Error::TableOrAliasDoesntExist("users".to_string()))
        );
    }

    #[test]
    fn select_join_chain_table_doesnt_exist() {
        let mut sim = Simulator::new(Box::new(GenericDialect {}));
        sim.execute("create table users (id int primary key, name text)")
            .unwrap();
        sim.execute("create table orders (id int primary key, user_id int)")
            .unwrap();

        assert_eq!(
            sim.execute(
                "select id from users 
                 join orders on users.id = products.id"
            ),
            Err(Error::TableOrAliasDoesntExist("products".to_string()))
        );
    }

    #[test]
    fn select_join_ambiguous_column() {
        let mut sim = Simulator::new(Box::new(GenericDialect {}));
        sim.execute("create table person (id int primary key, name text)")
            .unwrap();
        sim.execute(
            "create table order (order_id int primary key, name text, person_id int references person(id), total float)",
        )
        .unwrap();

        assert_eq!(
            sim.execute("select name from person join order on person.id = person_id"),
            Err(Error::AmbiguousColumn("name".to_string()))
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

    #[test]
    fn select_join_natural_single_common_column() {
        let mut sim = Simulator::new(Box::new(GenericDialect {}));
        sim.execute("create table person (id int primary key, name text)")
            .unwrap();
        sim.execute("create table order (id int primary key, total float)")
            .unwrap();

        sim.execute("select * from person natural join order")
            .unwrap();
    }

    #[test]
    fn select_join_natural_multiple_common_columns() {
        let mut sim = Simulator::new(Box::new(GenericDialect {}));
        sim.execute("create table person (id int primary key, dept_id int, name text)")
            .unwrap();
        sim.execute("create table employee (id int, dept_id int, salary float)")
            .unwrap();

        sim.execute("select * from person natural join employee")
            .unwrap();
    }

    #[test]
    fn select_join_natural_common_column_not_ambiguous() {
        let mut sim = Simulator::new(Box::new(GenericDialect {}));
        sim.execute("create table person (id int primary key, name text)")
            .unwrap();
        sim.execute("create table order (id int primary key, total float)")
            .unwrap();

        sim.execute("select id from person natural join order")
            .unwrap();
    }

    #[test]
    fn select_join_natural_qualified_common_column() {
        let mut sim = Simulator::new(Box::new(GenericDialect {}));
        sim.execute("create table person (id int primary key, name text)")
            .unwrap();
        sim.execute("create table order (id int primary key, total float)")
            .unwrap();

        sim.execute("select person.id from person natural join order")
            .unwrap();

        // TODO: you should be able to reference it this way too.
        sim.execute("select order.id from person natural join order")
            .unwrap();
    }

    #[test]
    fn select_join_natural_type_mismatch() {
        let mut sim = Simulator::new(Box::new(GenericDialect {}));
        sim.execute("create table person (id int primary key, name text)")
            .unwrap();
        sim.execute("create table order (id text primary key, total float)")
            .unwrap();

        assert_eq!(
            sim.execute("select * from person natural join order"),
            Err(Error::TypeMismatch {
                expected: SqlType::Integer,
                got: SqlType::Text
            })
        );
    }

    #[test]
    fn select_join_natural_mixed_common_and_unique() {
        let mut sim = Simulator::new(Box::new(GenericDialect {}));
        sim.execute("create table users (user_id int, dept_id int, name text)")
            .unwrap();
        sim.execute("create table departments (dept_id int, manager_id int, dept_name text)")
            .unwrap();

        sim.execute("select * from users natural join departments")
            .unwrap();

        sim.execute("select user_id, dept_name from users natural join departments")
            .unwrap();

        sim.execute("select dept_id from users natural join departments")
            .unwrap();
    }

    #[test]
    fn select_join_natural_chain() {
        let mut sim = Simulator::new(Box::new(GenericDialect {}));
        sim.execute("create table a (id int, x int)").unwrap();
        sim.execute("create table b (id int, y int)").unwrap();
        sim.execute("create table c (id int, z int)").unwrap();

        sim.execute("select * from a natural join b natural join c")
            .unwrap();

        sim.execute("select id, x, y, z from a natural join b natural join c")
            .unwrap();
    }

    #[test]
    fn select_join_natural_chain_non_existing_column() {
        let mut sim = Simulator::new(Box::new(GenericDialect {}));
        sim.execute("create table a (id int, x int)").unwrap();
        sim.execute("create table b (id int, y int)").unwrap();
        sim.execute("create table c (id int, z int)").unwrap();

        assert_eq!(
            sim.execute("select id, x, y, z, v from a natural join b natural join c"),
            Err(Error::ColumnDoesntExist("v".to_string()))
        )
    }

    #[test]
    fn select_join_natural_chain_non_existing_table() {
        let mut sim = Simulator::new(Box::new(GenericDialect {}));
        sim.execute("create table a (id int, x int)").unwrap();
        sim.execute("create table b (id int, y int)").unwrap();
        sim.execute("create table c (id int, z int)").unwrap();

        assert_eq!(
            sim.execute("select id, x, y, z, v.id from a natural join b natural join c"),
            Err(Error::TableOrAliasDoesntExist("v".to_string()))
        )
    }
}
