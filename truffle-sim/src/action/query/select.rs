use std::collections::HashSet;

use sqlparser::ast::{
    BinaryOperator, CastKind, Expr, Select, SelectItem, SelectItemQualifiedWildcardKind,
    TableFactor, UnaryOperator, Value,
};
use tracing::warn;

use crate::{Error, Simulator, column::Column, object_name_to_strings, ty::SqlType};

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

fn infer_expr_type(
    expr: &Expr,
    sim: &mut Simulator,
    tables: &Vec<SelectTable>,
    expected: Option<SqlType>,
) -> Result<SqlType, Error> {
    match expr {
        Expr::Value(val) => match val.value.clone() {
            Value::Number(str, _) => {
                // Initially, try to use the expected type.
                if let Some(expected_ty) = expected {
                    match expected_ty {
                        SqlType::SmallInt => {
                            if str.parse::<i16>().is_ok() {
                                return Ok(SqlType::SmallInt);
                            }
                        }
                        SqlType::Integer => {
                            if str.parse::<i32>().is_ok() {
                                return Ok(SqlType::Integer);
                            }
                        }
                        SqlType::BigInt => {
                            if str.parse::<i64>().is_ok() {
                                return Ok(SqlType::BigInt);
                            }
                        }
                        SqlType::Float => {
                            if str.parse::<f32>().is_ok() {
                                return Ok(SqlType::Float);
                            }
                        }
                        SqlType::Double => {
                            if str.parse::<f64>().is_ok() {
                                return Ok(SqlType::Float);
                            }
                        }
                        _ => {}
                    }
                }

                // Fallback to smallest type to biggest.
                if str.parse::<i16>().is_ok() {
                    Ok(SqlType::SmallInt)
                } else if str.parse::<i32>().is_ok() {
                    Ok(SqlType::Integer)
                } else if str.parse::<i64>().is_ok() {
                    Ok(SqlType::BigInt)
                } else if str.contains('.') || str.to_lowercase().contains('e') {
                    if str.parse::<f32>().is_ok() {
                        Ok(SqlType::Float)
                    } else if str.parse::<f64>().is_ok() {
                        Ok(SqlType::Double)
                    } else {
                        Err(Error::Sql("Invalid floating point number".to_string()))
                    }
                } else {
                    // Integer that's too large for i64
                    Err(Error::Sql("Number is too big".to_string()))
                }
            }
            Value::SingleQuotedString(_)
            | Value::DollarQuotedString(_)
            | Value::SingleQuotedByteStringLiteral(_)
            | Value::DoubleQuotedByteStringLiteral(_)
            | Value::NationalStringLiteral(_)
            | Value::HexStringLiteral(_)
            | Value::DoubleQuotedString(_) => Ok(SqlType::Text),
            Value::Boolean(_) => Ok(SqlType::Boolean),
            Value::Null => Ok(SqlType::Null),
            // Placeholder just takes the type of the expected.
            Value::Placeholder(_) => expected.ok_or(Error::Unsupported(
                "Cannot infer type of the placeholder".to_string(),
            )),
            _ => todo!(),
        },
        Expr::IsTrue(expr)
        | Expr::IsNotTrue(expr)
        | Expr::IsFalse(expr)
        | Expr::IsNotFalse(expr)
        | Expr::IsUnknown(expr)
        | Expr::IsNotUnknown(expr) => {
            let ty = infer_expr_type(expr, sim, tables, Some(SqlType::Boolean))?;
            if ty != SqlType::Boolean {
                return Err(Error::TypeMismatch {
                    expected: SqlType::Boolean,
                    got: ty,
                });
            }

            Ok(SqlType::Boolean)
        }

        Expr::IsNull(expr) | Expr::IsNotNull(expr) => {
            _ = infer_expr_type(expr, sim, tables, expected)?;
            Ok(SqlType::Boolean)
        }
        Expr::Identifier(ident) => {
            let name = &ident.value;

            let column = get_unqualified_column(sim, tables, name)?
                .ok_or(Error::ColumnDoesntExist(name.to_string()))?;

            Ok(column.ty)
        }
        Expr::CompoundIdentifier(idents) => {
            // validate that identifier is a column.
            let table_or_alias = &idents.first().unwrap().value;
            let column_name = &idents.get(1).unwrap().value;

            match check_table_or_alias(table_or_alias, tables)? {
                TableOrAlias::Table => {
                    let column = sim
                        .get_table(table_or_alias)
                        .unwrap()
                        .get_column(column_name)
                        .ok_or_else(|| Error::ColumnDoesntExist(column_name.to_string()))?;

                    Ok(column.ty.clone())
                }
                TableOrAlias::Alias => {
                    let table_name = &tables
                        .iter()
                        .find(|t| t.alias.as_ref().is_some_and(|a| a == table_or_alias))
                        .ok_or(Error::AliasDoesntExist(table_or_alias.to_string()))?
                        .name;

                    let column = sim
                        .get_table(table_name)
                        .unwrap()
                        .get_column(column_name)
                        .ok_or_else(|| Error::ColumnDoesntExist(column_name.to_string()))?;

                    Ok(column.ty.clone())
                }
            }
        }
        Expr::BinaryOp { left, right, op } => {
            let left_ty = infer_expr_type(left, sim, tables, expected)?;
            let right_ty = infer_expr_type(right, sim, tables, Some(left_ty.clone()))?;

            match op {
                BinaryOperator::Plus
                | BinaryOperator::Minus
                | BinaryOperator::Multiply
                | BinaryOperator::Divide
                | BinaryOperator::Modulo => {
                    if left_ty != right_ty {
                        return Err(Error::TypeMismatch {
                            expected: left_ty,
                            got: right_ty,
                        });
                    }

                    Ok(left_ty)
                }
                BinaryOperator::Gt
                | BinaryOperator::Lt
                | BinaryOperator::GtEq
                | BinaryOperator::LtEq => {
                    if left_ty != right_ty {
                        return Err(Error::TypeMismatch {
                            expected: left_ty,
                            got: right_ty,
                        });
                    }

                    Ok(SqlType::Boolean)
                }

                BinaryOperator::StringConcat => todo!(),
                BinaryOperator::Spaceship => todo!(),
                BinaryOperator::Eq | BinaryOperator::NotEq => {
                    if left_ty != right_ty {
                        return Err(Error::TypeMismatch {
                            expected: left_ty,
                            got: right_ty,
                        });
                    }

                    Ok(SqlType::Boolean)
                }
                BinaryOperator::And | BinaryOperator::Or | BinaryOperator::Xor => {
                    if left_ty != SqlType::Boolean {
                        return Err(Error::TypeMismatch {
                            expected: SqlType::Boolean,
                            got: left_ty,
                        });
                    }
                    if right_ty != SqlType::Boolean {
                        return Err(Error::TypeMismatch {
                            expected: SqlType::Boolean,
                            got: right_ty,
                        });
                    }

                    Ok(SqlType::Boolean)
                }
                BinaryOperator::BitwiseOr
                | BinaryOperator::BitwiseAnd
                | BinaryOperator::BitwiseXor => {
                    if !matches!(
                        left_ty,
                        SqlType::SmallInt | SqlType::Integer | SqlType::BigInt
                    ) {
                        return Err(Error::TypeMismatch {
                            expected: SqlType::Integer,
                            got: left_ty,
                        });
                    }

                    if left_ty != right_ty {
                        return Err(Error::TypeMismatch {
                            expected: left_ty,
                            got: right_ty,
                        });
                    }

                    Ok(left_ty)
                }
                _ => {
                    todo!()
                }
            }
        }
        Expr::UnaryOp { expr, op } => {
            let ty = infer_expr_type(expr, sim, tables, expected)?;

            match op {
                UnaryOperator::Plus | UnaryOperator::Minus => {
                    if !ty.is_numeric() {
                        Err(Error::TypeNotNumeric(ty))
                    } else {
                        Ok(ty)
                    }
                }
                UnaryOperator::Not => {
                    if ty != SqlType::Boolean {
                        Err(Error::TypeMismatch {
                            expected: SqlType::Boolean,
                            got: ty,
                        })
                    } else {
                        Ok(SqlType::Boolean)
                    }
                }
                _ => todo!(),
            }
        }
        Expr::Nested(expr) => infer_expr_type(expr, sim, tables, expected),
        Expr::InList { expr, list, .. } => {
            let ty = infer_expr_type(expr, sim, tables, expected)?;
            for item in list {
                let item_ty = infer_expr_type(item, sim, tables, Some(ty.clone()))?;
                if ty != item_ty {
                    return Err(Error::TypeMismatch {
                        expected: ty,
                        got: item_ty,
                    });
                }
            }

            Ok(SqlType::Boolean)
        }
        Expr::Cast {
            kind,
            expr,
            data_type,
            ..
        } => {
            let ty: SqlType = data_type.clone().into();
            match kind {
                CastKind::Cast | CastKind::DoubleColon => {
                    let _inner_ty = infer_expr_type(expr, sim, tables, Some(ty.clone()))?;
                    // TODO: Ensure the two types are castable.

                    Ok(ty)
                }
                _ => todo!(),
            }
        }
        _ => Err(Error::Unsupported(format!(
            "Unsupported WHERE expr: {expr:#?}"
        ))),
    }
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

fn get_unqualified_column(
    sim: &Simulator,
    tables: &[SelectTable],
    column: &str,
) -> Result<Option<Column>, Error> {
    let mut found_column: Option<Column> = None;

    for table in tables {
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

    Ok(found_column)
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
        infer_expr_type(selection, sim, &tables, Some(SqlType::Boolean))?;
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
}
