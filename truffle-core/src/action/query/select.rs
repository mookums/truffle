use std::collections::HashSet;

use itertools::Itertools;
use sqlparser::ast::{Expr, Select, SelectItem, SelectItemQualifiedWildcardKind, TableFactor};

use crate::{
    Error, Simulator,
    action::join::{JoinContext, JoinInferrer},
    expr::{ExprFlow, InferType},
    object_name_to_strings,
    resolve::{ResolveOutputKey, ResolvedQuery},
    ty::SqlType,
};

impl Simulator {
    pub(crate) fn select(&self, sel: &Select) -> Result<ResolvedQuery, Error> {
        let mut columns = SelectColumns::List(vec![]);
        let mut contexts = vec![];
        let mut resolved = ResolvedQuery::default();

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
                &mut resolved,
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
                        let column_name = &col_ref.name;
                        if all_columns.contains(column_name) {
                            return Err(Error::AmbiguousColumn(column_name.to_string()));
                        } else {
                            // The existence of this column should've already been confirmed earlier.
                            let column = context
                                .get_qualified_column(&col_ref.qualifier, &col_ref.name)?
                                .unwrap();

                            resolved.insert_output(
                                ResolveOutputKey::new(
                                    Some(col_ref.qualifier.clone()),
                                    col_ref.name.clone(),
                                ),
                                column.clone(),
                            );

                            all_columns.insert(column_name.to_string());
                        }
                    }
                }
            }
            SelectColumns::List(list) => {
                for column in list.into_iter() {
                    match column {
                        SelectColumn::Unqualified(column) => {
                            let true_column = contexts
                                .iter()
                                .filter_map(|c| c.get_column(&column).transpose())
                                .at_most_one()
                                .map_err(|_| Error::AmbiguousColumn(column.clone()))?
                                .ok_or_else(|| Error::ColumnDoesntExist(column.clone()))??;

                            resolved.insert_output(
                                ResolveOutputKey::new(None, column),
                                true_column.clone(),
                            );
                        }
                        SelectColumn::Absolute { table, column } => {
                            let true_column = contexts
                                .iter()
                                .filter_map(|c| c.get_qualified_column(&table, &column).transpose())
                                .exactly_one()
                                .map_err(|_| Error::ColumnDoesntExist(column.to_string()))??;

                            resolved.insert_output(
                                ResolveOutputKey::new(Some(table), column),
                                true_column.clone(),
                            );
                        }
                        SelectColumn::Aliased { alias, column } => {
                            let table_name = contexts
                                .iter()
                                .find_map(|c| c.aliases.get(&alias))
                                .ok_or(Error::AliasDoesntExist(alias))?;

                            if !contexts.iter().any(|c| c.has_table(table_name)) {
                                return Err(Error::TableDoesntExist(table_name.clone()));
                            }

                            // let inferred_type =
                            //     inferrer.infer_qualified_type(self, table_name, &column)?;

                            // resolved.insert_output(
                            //     ResolveOutputKey::new(Some(table_name.clone()), column),
                            //     inferred_type,
                            // );
                        }
                        SelectColumn::AliasedWildcard(alias) => {
                            let table_name = contexts
                                .iter()
                                .find_map(|c| c.aliases.get(&alias))
                                .ok_or(Error::AliasDoesntExist(alias))?;

                            if !contexts.iter().any(|c| c.has_table(table_name)) {
                                return Err(Error::TableDoesntExist(table_name.clone()));
                            }

                            for (col_ref, _) in contexts
                                .iter()
                                .filter(|c| c.has_table(table_name))
                                .flat_map(|c| &c.refs)
                                .unique_by(|r| r.1)
                            {
                                // let inferred_type = inferrer.infer_qualified_type(
                                //     self,
                                //     &col_ref.qualifier,
                                //     &col_ref.name,
                                // )?;

                                // resolved.insert_output(
                                //     ResolveOutputKey::new(
                                //         Some(col_ref.qualifier.clone()),
                                //         col_ref.name.clone(),
                                //     ),
                                //     inferred_type.clone(),
                                // );
                            }
                        }
                        SelectColumn::AbsoluteWildcard(table) => {
                            if !contexts
                                .iter()
                                .any(|c| c.refs.iter().any(|(r, _)| r.qualifier == table))
                            {
                                return Err(Error::TableDoesntExist(table));
                            }

                            for (col_ref, _) in contexts
                                .iter()
                                .filter(|c| c.has_table(&table))
                                .flat_map(|c| &c.refs)
                                .unique_by(|r| r.1)
                            {
                                // let inferred_type = inferrer.infer_qualified_type(
                                //     self,
                                //     &col_ref.qualifier,
                                //     &col_ref.name,
                                // )?;

                                // resolved.insert_output(
                                //     ResolveOutputKey::new(
                                //         Some(col_ref.qualifier.clone()),
                                //         col_ref.name.clone(),
                                //     ),
                                //     inferred_type.clone(),
                                // );
                            }
                        }
                    }
                }
            }
        }

        // Validate WHERE clause.
        if let Some(selection) = &sel.selection {
            self.infer_expr_type(
                selection,
                InferType::Required(SqlType::Boolean),
                &inferrer,
                &mut resolved,
                ExprFlow::Input,
            )?;
        }

        Ok(resolved)
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
    /// This is the true name of the column.
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
    use crate::{resolve::ResolveOutputKey, *};

    #[test]
    fn select_wildcard_success() {
        let mut sim = Simulator::new(GenericDialect {});
        sim.execute("create table person (id int, name text)")
            .unwrap();

        sim.execute("select * from person;").unwrap();
        sim.execute("select * from person;").unwrap();
    }

    #[test]
    fn select_fields_success() {
        let mut sim = Simulator::new(GenericDialect {});
        sim.execute("create table person (id int, name text)")
            .unwrap();

        sim.execute("select id, name from person;").unwrap();
    }

    #[test]
    fn select_column_doesnt_exist() {
        let mut sim = Simulator::new(GenericDialect {});
        sim.execute("create table person (id int, name text)")
            .unwrap();

        assert_eq!(
            sim.execute("select id, weight from person;"),
            Err(Error::ColumnDoesntExist("weight".to_string()))
        )
    }

    #[test]
    fn select_from_missing_table() {
        let mut sim = Simulator::new(GenericDialect {});
        assert_eq!(
            sim.execute("select * from person;"),
            Err(Error::TableDoesntExist("person".to_string()))
        );
    }

    #[test]
    fn select_ambiguous_column() {
        let mut sim = Simulator::new(GenericDialect {});
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
        let mut sim = Simulator::new(GenericDialect {});
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
        let mut sim = Simulator::new(GenericDialect {});
        sim.execute("create table person (id int, name text)")
            .unwrap();
        sim.execute("create table orders (order_id int, total int)")
            .unwrap();
        sim.execute("select * from person, orders").unwrap();
    }

    #[test]
    fn select_multiple_tables_unique_columns() {
        let mut sim = Simulator::new(GenericDialect {});
        sim.execute("create table person (id int, name text)")
            .unwrap();
        sim.execute("create table orders (order_id int, total int)")
            .unwrap();
        sim.execute("select name, total from person, orders")
            .unwrap();
    }

    #[test]
    fn select_multiple_tables_column_doesnt_exist() {
        let mut sim = Simulator::new(GenericDialect {});
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
        let mut sim = Simulator::new(GenericDialect {});
        sim.execute("create table person (id int, name text)")
            .unwrap();
        sim.execute("create table orders (order_id int, total int)")
            .unwrap();
        sim.execute("select p.name, o.total from person p, orders o")
            .unwrap();
    }

    #[test]
    fn select_multiple_tables_with_as_aliases() {
        let mut sim = Simulator::new(GenericDialect {});
        sim.execute("create table person (id int, name text)")
            .unwrap();
        sim.execute("create table orders (order_id int, total int)")
            .unwrap();
        sim.execute("select p.name, o.total from person AS p, orders AS o")
            .unwrap();
    }

    #[test]
    fn select_qualified_column_with_unknown_table() {
        let mut sim = Simulator::new(GenericDialect {});
        sim.execute("create table person (id int, name text)")
            .unwrap();

        assert_eq!(
            sim.execute("select unknown_table.id from person"),
            Err(Error::TableOrAliasDoesntExist("unknown_table".to_string()))
        );
    }

    #[test]
    fn select_qualified_column_with_unincluded_table() {
        let mut sim = Simulator::new(GenericDialect {});
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
        let mut sim = Simulator::new(GenericDialect {});
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
        let mut sim = Simulator::new(GenericDialect {});
        sim.execute("create table person (id int, name text)")
            .unwrap();

        sim.execute("select person.* from person").unwrap();
    }

    #[test]
    fn select_qualified_wildcard_with_alias() {
        let mut sim = Simulator::new(GenericDialect {});
        sim.execute("create table person (id int, name text)")
            .unwrap();
        sim.execute("create table orders (id int, total int)")
            .unwrap();

        sim.execute("select p.* from person p, orders").unwrap();
    }

    #[test]
    fn select_qualified_wildcard_with_unknown_table() {
        let mut sim = Simulator::new(GenericDialect {});
        sim.execute("create table person (id int, name text)")
            .unwrap();

        assert_eq!(
            sim.execute("select unknown.* from person"),
            Err(Error::TableOrAliasDoesntExist("unknown".to_string()))
        );
    }

    #[test]
    fn select_qualified_wildcard_table_not_in_from() {
        let mut sim = Simulator::new(GenericDialect {});
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
        let mut sim = Simulator::new(GenericDialect {});
        sim.execute("create table person (id int, name text)")
            .unwrap();
        sim.execute("create table orders (id int, total int)")
            .unwrap();

        sim.execute("select p.*, o.* from person p, orders o")
            .unwrap();
    }

    #[test]
    fn select_qualified_wildcard_with_columns() {
        let mut sim = Simulator::new(GenericDialect {});
        sim.execute("create table person (id int, name text)")
            .unwrap();
        sim.execute("create table orders (order_id int, total int)")
            .unwrap();

        sim.execute("select p.*, orders.total from person p, orders")
            .unwrap();
    }

    #[test]
    fn select_where() {
        let mut sim = Simulator::new(GenericDialect {});
        sim.execute("create table person (id int, name text)")
            .unwrap();

        sim.execute("select name from person where (name = 'abc')")
            .unwrap();
    }

    #[test]
    fn select_where_column_doesnt_exist() {
        let mut sim = Simulator::new(GenericDialect {});
        sim.execute("create table person (id int, name text)")
            .unwrap();

        assert_eq!(
            sim.execute("select name from person where weight = 100"),
            Err(Error::ColumnDoesntExist("weight".to_string()))
        );
    }

    #[test]
    fn select_where_simple_comparison() {
        let mut sim = Simulator::new(GenericDialect {});
        sim.execute("create table person (id int, name text, age int)")
            .unwrap();
        sim.execute("select name from person where age > 18")
            .unwrap();
        sim.execute("select name from person where id = 1").unwrap();
    }

    #[test]
    fn select_where_tuple_comparison() {
        let mut sim = Simulator::new(GenericDialect {});
        sim.execute("create table person (id int, name text, age int)")
            .unwrap();
        sim.execute("select name from person where (id, name) = (1, 'abc')")
            .unwrap();
    }

    #[test]
    fn select_where_logical_operators() {
        let mut sim = Simulator::new(GenericDialect {});
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
        let mut sim = Simulator::new(GenericDialect {});
        sim.execute("create table person (id int, name text)")
            .unwrap();
        sim.execute("create table company (id int, name text)")
            .unwrap();
        sim.execute("select person.name from person, company where person.id = company.id")
            .unwrap();
    }

    #[test]
    fn select_where_ambiguous_column() {
        let mut sim = Simulator::new(GenericDialect {});
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
        let mut sim = Simulator::new(GenericDialect {});
        sim.execute("create table person (id int, name text)")
            .unwrap();
        sim.execute("create table company (id int, name text)")
            .unwrap();
        sim.execute("select p.name from person p, company c where p.id = c.id")
            .unwrap();
    }

    #[test]
    fn select_where_nested_expressions() {
        let mut sim = Simulator::new(GenericDialect {});
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
        let mut sim = Simulator::new(GenericDialect {});
        sim.execute("create table person (id int, name text)")
            .unwrap();
        assert_eq!(
            sim.execute("select name from person where person.weight = 100"),
            Err(Error::QualifiedColumnDoesntExist {
                qualifier: "person".to_string(),
                column: "weight".to_string()
            })
        );
    }

    #[test]
    fn select_where_invalid_table_reference() {
        let mut sim = Simulator::new(GenericDialect {});
        sim.execute("create table person (id int, name text)")
            .unwrap();
        assert_eq!(
            sim.execute("select name from person where company.id = 1"),
            Err(Error::QualifiedColumnDoesntExist {
                qualifier: "company".to_string(),
                column: "id".to_string()
            })
        );
    }

    #[test]
    fn select_where_invalid_alias() {
        let mut sim = Simulator::new(GenericDialect {});
        sim.execute("create table person (id int, name text)")
            .unwrap();
        assert_eq!(
            sim.execute("select name from person p where x.id = 1"),
            Err(Error::QualifiedColumnDoesntExist {
                qualifier: "x".to_string(),
                column: "id".to_string()
            })
        );
    }

    #[test]
    fn select_where_invalid_type() {
        let mut sim = Simulator::new(GenericDialect {});
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
    fn select_where_invalid_tuple() {
        let mut sim = Simulator::new(GenericDialect {});
        sim.execute("create table person (id int, name text)")
            .unwrap();
        assert_eq!(
            sim.execute("select name from person p where (p.id, p.name) = (false, 200)"),
            Err(Error::TypeMismatch {
                expected: SqlType::Integer,
                got: SqlType::Boolean,
            })
        );
    }

    #[test]
    fn select_where_invalid_type_string_with_int() {
        let mut sim = Simulator::new(GenericDialect {});
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
        let mut sim = Simulator::new(GenericDialect {});
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
        let mut sim = Simulator::new(GenericDialect {});
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
        let mut sim = Simulator::new(GenericDialect {});
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
        let mut sim = Simulator::new(GenericDialect {});
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
        let mut sim = Simulator::new(GenericDialect {});
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
        let mut sim = Simulator::new(GenericDialect {});
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
        let mut sim = Simulator::new(GenericDialect {});
        sim.execute("create table person (active bool)").unwrap();
        assert_eq!(
            sim.execute("select * from person where -active"),
            Err(Error::TypeNotNumeric(SqlType::Boolean))
        );
    }

    #[test]
    fn select_where_invalid_type_in_list() {
        let mut sim = Simulator::new(GenericDialect {});
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
        let mut sim = Simulator::new(GenericDialect {});
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
        let mut sim = Simulator::new(GenericDialect {});
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
        let mut sim = Simulator::new(GenericDialect {});
        sim.execute("create table person (id int primary key, name text)")
            .unwrap();
        sim.execute(
            "create table order (id int primary key, person_id int references person(id), total float)",
        )
        .unwrap();

        sim.execute("select person.* from person join order on person.id = order.person_id")
            .unwrap();
    }

    #[test]
    fn select_join_on_type_mismatch() {
        let mut sim = Simulator::new(GenericDialect {});
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
        let mut sim = Simulator::new(GenericDialect {});
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
        let mut sim = Simulator::new(GenericDialect {});
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
        let mut sim = Simulator::new(GenericDialect {});
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
        let mut sim = Simulator::new(GenericDialect {});
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
        let mut sim = Simulator::new(GenericDialect {});
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
            Err(Error::QualifiedColumnDoesntExist {
                qualifier: "users".to_string(),
                column: "id".to_string()
            })
        );
    }

    #[test]
    fn select_join_chain_table_doesnt_exist() {
        let mut sim = Simulator::new(GenericDialect {});
        sim.execute("create table users (id int primary key, name text)")
            .unwrap();
        sim.execute("create table orders (id int primary key, user_id int)")
            .unwrap();

        assert_eq!(
            sim.execute(
                "select id from users 
                 join orders on users.id = products.id"
            ),
            Err(Error::QualifiedColumnDoesntExist {
                qualifier: "products".to_string(),
                column: "id".to_string()
            })
        );
    }

    #[test]
    fn select_join_ambiguous_column() {
        let mut sim = Simulator::new(GenericDialect {});
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
        let mut sim = Simulator::new(GenericDialect {});
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
        let mut sim = Simulator::new(GenericDialect {});
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
        let mut sim = Simulator::new(GenericDialect {});
        sim.execute("create table person (id int primary key, name text)")
            .unwrap();
        sim.execute("create table order (id int primary key, total float)")
            .unwrap();

        sim.execute("select * from person natural join order")
            .unwrap();
    }

    #[test]
    fn select_join_natural_multiple_common_columns() {
        let mut sim = Simulator::new(GenericDialect {});
        sim.execute("create table person (id int primary key, dept_id int, name text)")
            .unwrap();
        sim.execute("create table employee (id int, dept_id int, salary float)")
            .unwrap();

        sim.execute("select * from person natural join employee")
            .unwrap();
    }

    #[test]
    fn select_join_natural_common_column_not_ambiguous() {
        let mut sim = Simulator::new(GenericDialect {});
        sim.execute("create table person (id int primary key, name text)")
            .unwrap();
        sim.execute("create table order (id int primary key, total float)")
            .unwrap();

        sim.execute("select id from person natural join order")
            .unwrap();
    }

    #[test]
    fn select_join_natural_qualified_common_column() {
        let mut sim = Simulator::new(GenericDialect {});
        sim.execute("create table person (id int primary key, name text)")
            .unwrap();
        sim.execute("create table order (id int primary key, total float)")
            .unwrap();

        // These all references the same logical column and so
        // it is valid to select any of them!
        sim.execute("select id from person natural join order")
            .unwrap();

        sim.execute("select person.id from person natural join order")
            .unwrap();

        sim.execute("select order.id from person natural join order")
            .unwrap();
    }

    #[test]
    fn select_join_natural_type_mismatch() {
        let mut sim = Simulator::new(GenericDialect {});
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
        let mut sim = Simulator::new(GenericDialect {});
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
        let mut sim = Simulator::new(GenericDialect {});
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
        let mut sim = Simulator::new(GenericDialect {});
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
        let mut sim = Simulator::new(GenericDialect {});
        sim.execute("create table a (id int, x int)").unwrap();
        sim.execute("create table b (id int, y int)").unwrap();
        sim.execute("create table c (id int, z int)").unwrap();

        assert_eq!(
            sim.execute("select id, x, y, z, v.id from a natural join b natural join c"),
            Err(Error::TableOrAliasDoesntExist("v".to_string()))
        )
    }

    #[test]
    fn select_join_none_basic() {
        let mut sim = Simulator::new(GenericDialect {});
        sim.execute("create table colors (color_id int, name text)")
            .unwrap();
        sim.execute("create table sizes (size_id int, size_name text)")
            .unwrap();

        sim.execute("select * from colors join sizes").unwrap();
    }

    #[test]
    fn select_join_none_with_ambiguous_wildcard() {
        let mut sim = Simulator::new(GenericDialect {});
        sim.execute("create table table1 (id int, name text)")
            .unwrap();
        sim.execute("create table table2 (id int, value text)")
            .unwrap();

        assert!(matches!(
            sim.execute("select * from table1 join table2"),
            Err(Error::AmbiguousColumn(_))
        ));
    }

    #[test]
    fn select_join_none_with_comma_syntax() {
        let mut sim = Simulator::new(GenericDialect {});
        sim.execute("create table a (x int)").unwrap();
        sim.execute("create table b (y int)").unwrap();

        sim.execute("select * from a, b").unwrap();
    }

    #[test]
    fn select_join_none_specific_columns() {
        let mut sim = Simulator::new(GenericDialect {});
        sim.execute("create table products (product_id int, name text)")
            .unwrap();
        sim.execute("create table categories (category_id int, category text)")
            .unwrap();

        sim.execute("select products.name, categories.category from products join categories")
            .unwrap();
    }

    #[test]
    fn select_join_none_qualified_columns() {
        let mut sim = Simulator::new(GenericDialect {});
        sim.execute("create table person (id int, name text)")
            .unwrap();
        sim.execute("create table company (id int, company_name text)")
            .unwrap();

        sim.execute("select person.id, company.id from person join company")
            .unwrap();
    }

    #[test]
    fn select_join_none_ambiguous_column() {
        let mut sim = Simulator::new(GenericDialect {});
        sim.execute("create table left_table (id int, value text)")
            .unwrap();
        sim.execute("create table right_table (id int, score int)")
            .unwrap();

        assert_eq!(
            sim.execute("select id from left_table join right_table"),
            Err(Error::AmbiguousColumn("id".to_string()))
        );
    }

    #[test]
    fn select_join_none_with_aliases() {
        let mut sim = Simulator::new(GenericDialect {});
        sim.execute("create table users (user_id int, name text)")
            .unwrap();
        sim.execute("create table roles (role_id int, role_name text)")
            .unwrap();

        sim.execute("select u.name, r.role_name from users u join roles r")
            .unwrap();
    }

    #[test]
    fn select_join_none_with_where_clause() {
        let mut sim = Simulator::new(GenericDialect {});
        sim.execute("create table numbers (n int)").unwrap();
        sim.execute("create table multipliers (m int)").unwrap();

        sim.execute("select * from numbers join multipliers where numbers.n < multipliers.m")
            .unwrap();
    }

    // // TODO: This requires supporting TableFactor::NestedJoin.
    // // As it considers this to be a nested join.
    // #[test]
    // fn select_join_none_multiple_tables() {
    //     let mut sim = Simulator::new(GenericDialect {});
    //     sim.execute("create table a (x int)").unwrap();
    //     sim.execute("create table b (y int)").unwrap();
    //     sim.execute("create table c (z int)").unwrap();

    //     sim.execute("select * from a join b join c").unwrap();
    // }

    #[test]
    fn select_join_none_empty_tables() {
        let mut sim = Simulator::new(GenericDialect {});
        sim.execute("create table empty1 (id int)").unwrap();
        sim.execute("create table empty2 (value text)").unwrap();

        // None join of empty tables should work (return no rows)
        sim.execute("select * from empty1 join empty2").unwrap();
    }

    #[test]
    fn select_join_none_wildcard_expansion() {
        let mut sim = Simulator::new(GenericDialect {});
        sim.execute("create table team (team_id int, team_name text)")
            .unwrap();
        sim.execute("create table player (player_id int, player_name text)")
            .unwrap();

        // SELECT * should include all columns from both tables
        sim.execute("select * from team join player").unwrap();

        // Qualified wildcards should work too
        sim.execute("select team.*, player.player_name from team join player")
            .unwrap();
    }

    #[test]
    fn select_inner_join_vs_join_none() {
        let mut sim = Simulator::new(GenericDialect {});
        sim.execute("create table table1 (a int)").unwrap();
        sim.execute("create table table2 (b int)").unwrap();

        sim.execute("select * from table1 join table2").unwrap();
        sim.execute("select * from table1 inner join table2")
            .unwrap();
    }

    #[test]
    fn select_cross_join() {
        let mut sim = Simulator::new(GenericDialect {});
        sim.execute("create table empty1 (id int)").unwrap();
        sim.execute("create table empty2 (value text)").unwrap();

        // None join of empty tables should work (return no rows)
        sim.execute("select * from empty1 cross join empty2")
            .unwrap();
    }

    #[test]
    fn select_join_using() {
        let mut sim = Simulator::new(GenericDialect {});
        sim.execute("create table table1 (id int primary key, fruit text not null)")
            .unwrap();
        sim.execute("create table table2 (id int primary key, juice text not null)")
            .unwrap();

        sim.execute("select id, fruit, juice from table1 join table2 using (id)")
            .unwrap();

        sim.execute("select id, table1.fruit, table2.juice from table1 join table2 using (id)")
            .unwrap();
    }

    #[test]
    fn select_join_using_column_doesnt_exist() {
        let mut sim = Simulator::new(GenericDialect {});
        sim.execute("create table table1 (id int primary key, fruit text not null)")
            .unwrap();
        sim.execute("create table table2 (id2 int primary key, juice text not null)")
            .unwrap();

        assert_eq!(
            sim.execute("select id, fruit, juice from table1 join table2 using (fruit)"),
            Err(Error::ColumnDoesntExist("fruit".to_string()))
        );
    }

    #[test]
    fn select_join_using_ambiguous_column() {
        let mut sim = Simulator::new(GenericDialect {});
        sim.execute("create table table1 (id int primary key, fruit text not null, price int)")
            .unwrap();
        sim.execute(
            "create table table2 (id int primary key, fruit text not null, juice text not null)",
        )
        .unwrap();

        assert_eq!(
            sim.execute("select id, price, juice from table1 join table2 using (fruit)"),
            Err(Error::AmbiguousColumn("id".to_string()))
        );
    }

    #[test]
    fn select_join_using_type_mismatch() {
        let mut sim = Simulator::new(GenericDialect {});
        sim.execute("create table table1 (id int primary key, fruit int not null, price int)")
            .unwrap();
        sim.execute(
            "create table table2 (id2 int primary key, fruit text not null, juice text not null)",
        )
        .unwrap();

        assert_eq!(
            sim.execute("select id, price, juice from table1 join table2 using (fruit)"),
            Err(Error::TypeMismatch {
                expected: SqlType::Integer,
                got: SqlType::Text
            })
        );
    }

    #[test]
    fn select_join_using_multi_column() {
        let mut sim = Simulator::new(GenericDialect {});
        sim.execute("create table table1 (id int primary key, fruit text not null, price int)")
            .unwrap();
        sim.execute(
            "create table table2 (id int primary key, fruit text not null, juice text not null)",
        )
        .unwrap();

        sim.execute("select id, fruit, price, juice from table1 join table2 using (id, fruit)")
            .unwrap();
    }

    #[test]
    fn select_left_join_basic() {
        let mut sim = Simulator::new(GenericDialect {});
        sim.execute("create table person (id int primary key, name text)")
            .unwrap();
        sim.execute(
            "create table order (id int primary key, person_id int references person(id), total float)",
        )
        .unwrap();

        sim.execute("select person.*, order.total from person left join order on person.id = order.person_id")
            .unwrap();
    }

    #[test]
    fn select_left_outer_join_basic() {
        let mut sim = Simulator::new(GenericDialect {});
        sim.execute("create table users (id int primary key, name text)")
            .unwrap();
        sim.execute("create table orders (id int primary key, user_id int, total float)")
            .unwrap();

        sim.execute("select users.name, orders.total from users left outer join orders on users.id = orders.user_id")
            .unwrap();
    }

    #[test]
    fn select_left_join_using() {
        let mut sim = Simulator::new(GenericDialect {});
        sim.execute("create table table1 (id int primary key, name text)")
            .unwrap();
        sim.execute("create table table2 (id int primary key, value int)")
            .unwrap();

        sim.execute("select id, name, value from table1 left join table2 using (id)")
            .unwrap();
    }

    #[test]
    fn select_right_join_basic() {
        let mut sim = Simulator::new(GenericDialect {});
        sim.execute("create table person (id int primary key, name text)")
            .unwrap();
        sim.execute(
            "create table order (id int primary key, person_id int references person(id), total float)",
        )
        .unwrap();

        sim.execute("select person.name, order.* from person right join order on person.id = order.person_id")
            .unwrap();
    }

    #[test]
    fn select_right_outer_join_basic() {
        let mut sim = Simulator::new(GenericDialect {});
        sim.execute("create table employees (id int primary key, name text)")
            .unwrap();
        sim.execute("create table departments (id int primary key, emp_id int, dept_name text)")
            .unwrap();

        let resolved = sim.execute("select employees.name, departments.dept_name from employees right outer join departments on employees.id = departments.emp_id")
            .unwrap();

        assert_eq!(resolved.inputs.len(), 0);
        assert_eq!(resolved.outputs.len(), 2);

        assert_eq!(
            resolved.get_output_with_name("name").map(|c| &c.ty),
            Some(&SqlType::Text)
        );
        assert_eq!(
            resolved.get_output_with_name("dept_name").map(|c| &c.ty),
            Some(&SqlType::Text)
        );
        assert_eq!(
            resolved
                .get_output(&ResolveOutputKey::new(
                    Some("employees".to_string()),
                    "name"
                ))
                .map(|c| &c.ty),
            Some(&SqlType::Text)
        );
        assert_eq!(
            resolved
                .get_output(&ResolveOutputKey::new(
                    Some("departments".to_string()),
                    "dept_name"
                ))
                .map(|c| &c.ty),
            Some(&SqlType::Text)
        );
        assert_eq!(resolved.get_output_with_name("emp_id"), None);
    }

    #[test]
    fn select_right_join_natural() {
        let mut sim = Simulator::new(GenericDialect {});
        sim.execute("create table products (id int primary key, name text)")
            .unwrap();
        sim.execute("create table inventory (id int primary key, quantity int)")
            .unwrap();

        sim.execute("select * from products natural right join inventory")
            .unwrap();
    }

    #[test]
    fn select_full_outer_join_basic() {
        let mut sim = Simulator::new(GenericDialect {});
        sim.execute("create table customers (id int primary key, name text)")
            .unwrap();
        sim.execute("create table orders (id int primary key, customer_id int, amount float)")
            .unwrap();

        sim.execute("select customers.name, orders.amount from customers full outer join orders on customers.id = orders.customer_id")
            .unwrap();
    }

    #[test]
    fn select_full_outer_join_using() {
        let mut sim = Simulator::new(GenericDialect {});
        sim.execute("create table left_table (shared_id int, left_data text)")
            .unwrap();
        sim.execute("create table right_table (shared_id int, right_data text)")
            .unwrap();

        sim.execute("select shared_id, left_data, right_data from left_table full outer join right_table using (shared_id)")
            .unwrap();
    }

    #[test]
    fn select_outer_join_type_mismatch() {
        let mut sim = Simulator::new(GenericDialect {});
        sim.execute("create table table1 (id int primary key, name text)")
            .unwrap();
        sim.execute("create table table2 (id int primary key, value text)")
            .unwrap();

        assert_eq!(
            sim.execute("select * from table1 left outer join table2 on table1.id = table2.value"),
            Err(Error::TypeMismatch {
                expected: SqlType::Integer,
                got: SqlType::Text
            })
        );
    }

    #[test]
    fn select_with_resolved_input_output() {
        let mut sim = Simulator::new(GenericDialect {});
        sim.execute("create table person (id integer not null, name text not null, weight float default 10.01)").unwrap();

        let resolve = sim.execute("select * from person where id = $1").unwrap();

        assert_eq!(resolve.inputs.len(), 1);
        assert_eq!(resolve.get_input(0).unwrap(), &SqlType::Integer);

        assert_eq!(resolve.outputs.len(), 3);
        resolve
            .output_iter()
            .map(|(k, t)| (k, &t.ty))
            .for_each(|(key, ty)| match key.name.as_ref() {
                "id" => assert!(ty == &SqlType::Integer),
                "name" => assert!(ty == &SqlType::Text),
                "weight" => assert!(ty == &SqlType::Float),
                _ => unreachable!(),
            });
    }

    #[test]
    fn select_with_resolved_input_output_joins() {
        let mut sim = Simulator::new(GenericDialect {});
        sim.execute("create table person (id integer not null, name text not null, weight float default 10.01)").unwrap();

        let resolve = sim.execute("select * from person where id = $1").unwrap();

        assert_eq!(resolve.inputs.len(), 1);
        assert_eq!(resolve.get_input(0).unwrap(), &SqlType::Integer);

        assert_eq!(resolve.outputs.len(), 3);
        resolve
            .output_iter()
            .map(|(k, t)| (k, &t.ty))
            .for_each(|(key, ty)| match key.name.as_ref() {
                "id" => assert!(ty == &SqlType::Integer),
                "name" => assert!(ty == &SqlType::Text),
                "weight" => assert!(ty == &SqlType::Float),
                _ => unreachable!(),
            });
    }
}
