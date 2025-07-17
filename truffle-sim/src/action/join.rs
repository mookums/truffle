use std::collections::HashMap;

use itertools::Itertools;
use sqlparser::ast::{Join, JoinConstraint, JoinOperator, TableFactor};

use crate::{
    Error, Simulator, column::Column, expr::ColumnInferrer, object_name_to_strings, table::Table,
    ty::SqlType,
};

impl Simulator {
    pub(crate) fn infer_joins(
        &self,
        table: &Table,
        name: &str,
        alias: Option<&String>,
        joins: &[Join],
    ) -> Result<JoinContext, Error> {
        let mut join_ctx = JoinContext::from_table(table, name, alias)?;

        for join in joins {
            match &join.relation {
                TableFactor::Table { name, alias, .. } => {
                    let right_table_name = object_name_to_strings(name).first().unwrap().clone();
                    let right_table_alias = alias.as_ref().map(|a| a.name.value.clone());

                    let right_table = self
                        .get_table(&right_table_name)
                        .ok_or_else(|| Error::TableDoesntExist(right_table_name.clone()))?;

                    if let Some(alias) = &right_table_alias
                        && self.has_table(alias)
                    {
                        return Err(Error::AliasIsTableName(alias.to_string()));
                    }

                    match &join.join_operator {
                        JoinOperator::Join(join_constraint)
                        | JoinOperator::Inner(join_constraint) => match join_constraint {
                            JoinConstraint::On(expr) => {
                                let inferrer = JoinContextInferrer {
                                    join_ctx: &join_ctx,
                                    right_table: (&right_table_name, right_table),
                                };

                                let ty =
                                    self.infer_expr_type(expr, Some(SqlType::Boolean), &inferrer)?;

                                if ty != SqlType::Boolean {
                                    return Err(Error::TypeMismatch {
                                        expected: SqlType::Boolean,
                                        got: ty,
                                    });
                                }

                                join_ctx.join_table(
                                    right_table,
                                    &right_table_name,
                                    right_table_alias,
                                    JoinKind::On,
                                )?;
                            }
                            JoinConstraint::Using(names) => {
                                let column_names: Vec<String> = names
                                    .iter()
                                    .map(|n| object_name_to_strings(n).first().unwrap().clone())
                                    .collect();

                                for column_name in column_names.iter() {
                                    let left_ty = if let Some((col_ref, _)) = join_ctx
                                        .refs
                                        .iter()
                                        .unique_by(|(_, idx)| *idx)
                                        .filter(|(r, _)| &r.name == column_name)
                                        .at_most_one()
                                        .map_err(|_| {
                                            Error::AmbiguousColumn(column_name.to_string())
                                        })? {
                                        let table_name = &col_ref.qualifier;
                                        let column = self
                                            .get_table(table_name)
                                            .unwrap()
                                            .get_column(column_name)
                                            .unwrap();

                                        Some(column.ty.clone())
                                    } else {
                                        None
                                    };

                                    let right_ty =
                                        right_table.get_column(column_name).map(|rc| rc.ty.clone());

                                    match (left_ty, right_ty) {
                                        (Some(lty), Some(rty)) => {
                                            if lty == rty {
                                                continue;
                                            } else {
                                                return Err(Error::TypeMismatch {
                                                    expected: lty,
                                                    got: rty,
                                                });
                                            }
                                        }
                                        _ => {
                                            return Err(Error::ColumnDoesntExist(
                                                column_name.to_string(),
                                            ));
                                        }
                                    }
                                }

                                join_ctx.join_table(
                                    right_table,
                                    &right_table_name,
                                    right_table_alias,
                                    JoinKind::Using(column_names),
                                )?;
                            }
                            JoinConstraint::Natural => {
                                let mut found_common_column = false;

                                // Check all columns from left tables against right table
                                for (col_ref, _) in join_ctx.refs.iter().unique_by(|r| *r.1) {
                                    let table_name = &col_ref.qualifier;
                                    let column_name = &col_ref.name;

                                    if let Some(right_column) = right_table.get_column(column_name)
                                    {
                                        let column = self
                                            .get_table(table_name)
                                            .unwrap()
                                            .get_column(column_name)
                                            .unwrap();

                                        // Check if types match
                                        if column.ty == right_column.ty {
                                            found_common_column = true;
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

                                join_ctx.join_table(
                                    right_table,
                                    &right_table_name,
                                    right_table_alias,
                                    JoinKind::Natural,
                                )?;
                            }
                            JoinConstraint::None => {
                                join_ctx.join_table(
                                    right_table,
                                    &right_table_name,
                                    right_table_alias,
                                    JoinKind::On,
                                )?;
                            }
                        },
                        _ => todo!(),
                    }
                }
                _ => todo!("Unsupported Join TableFactor: {}", join.relation),
            }
        }

        Ok(join_ctx)
    }
}

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub struct ColumnRef {
    pub qualifier: String,
    pub name: String,
}

impl ColumnRef {
    pub fn new(qualifier: impl ToString, name: impl ToString) -> ColumnRef {
        ColumnRef {
            qualifier: qualifier.to_string(),
            name: name.to_string(),
        }
    }
}

pub struct JoinContext {
    // Maps the alias to the table name
    pub aliases: HashMap<String, String>,
    pub refs: HashMap<ColumnRef, usize>,
    pub columns: Vec<Column>,
}

enum JoinKind {
    On,
    Natural,
    Using(Vec<String>),
}

impl JoinContext {
    fn from_table(
        table: &Table,
        name: impl ToString,
        alias: Option<impl ToString>,
    ) -> Result<JoinContext, Error> {
        let mut aliases = HashMap::new();

        let table_columns = table.columns.clone();
        let mut refs = HashMap::new();
        let mut columns = Vec::new();

        let table_name = name.to_string();

        for (i, (column_name, column)) in table_columns.iter().enumerate() {
            assert!(
                refs.insert(ColumnRef::new(&table_name, column_name), i)
                    .is_none()
            );

            columns.push(column.clone());
        }

        if let Some(alias) = alias {
            aliases.insert(alias.to_string(), name.to_string());
        }

        Ok(JoinContext {
            refs,
            aliases,
            columns,
        })
    }

    fn join_table(
        &mut self,
        table: &Table,
        name: impl ToString,
        alias: Option<impl ToString>,
        kind: JoinKind,
    ) -> Result<(), Error> {
        let columns = table.columns.clone();
        let table_name = name.to_string();

        match kind {
            JoinKind::On => {
                // add all columns from the right to the left
                for (column_name, column) in columns.iter() {
                    let index = self.columns.len();
                    self.columns.push(column.clone());

                    assert!(
                        self.refs
                            .insert(ColumnRef::new(&table_name, column_name), index)
                            .is_none()
                    );
                }
            }
            JoinKind::Natural => {
                let all_existing_columns: Vec<String> =
                    self.refs.keys().map(|r| r.name.clone()).collect();

                for (column_name, column) in columns.iter() {
                    if all_existing_columns.contains(column_name) {
                        let existing_index = self
                            .refs
                            .iter()
                            .find_map(|(r, idx)| {
                                if r.name == *column_name {
                                    Some(*idx)
                                } else {
                                    None
                                }
                            })
                            .unwrap();

                        assert!(
                            self.refs
                                .insert(ColumnRef::new(&table_name, column_name), existing_index)
                                .is_none()
                        );
                    } else {
                        let index = self.columns.len();
                        self.columns.push(column.clone());

                        assert!(
                            self.refs
                                .insert(ColumnRef::new(&table_name, column_name), index)
                                .is_none()
                        );
                    }
                }
            }
            JoinKind::Using(commons) => {
                for (column_name, column) in columns.iter() {
                    if commons.contains(column_name) {
                        let existing_index = self
                            .refs
                            .iter()
                            .find_map(|(r, idx)| {
                                if r.name == *column_name {
                                    Some(*idx)
                                } else {
                                    None
                                }
                            })
                            .unwrap();

                        assert!(
                            self.refs
                                .insert(ColumnRef::new(&table_name, column_name), existing_index)
                                .is_none()
                        );
                    } else {
                        let index = self.columns.len();
                        self.columns.push(column.clone());

                        assert!(
                            self.refs
                                .insert(ColumnRef::new(&table_name, column_name), index)
                                .is_none()
                        );
                    }
                }
            }
        }

        if let Some(alias) = alias {
            self.aliases.insert(alias.to_string(), name.to_string());
        }

        Ok(())
    }

    pub fn has_table(&self, table: &str) -> bool {
        self.refs.keys().any(|k| k.qualifier == table)
    }

    pub fn has_column_in_table(&self, table: &str, column: &str) -> bool {
        self.refs
            .iter()
            .any(|(r, _)| r.qualifier == table && r.name == column)
    }

    pub fn has_column(&self, column: &str) -> bool {
        self.refs.keys().map(|k| &k.name).any(|n| n == column)
    }

    fn infer_unqualified_type(
        &self,
        sim: &Simulator,
        column: &str,
    ) -> Result<Option<SqlType>, Error> {
        let matches: Vec<(ColumnRef, usize)> = self
            .refs
            .clone()
            .into_iter()
            .filter(|(r, _)| r.name == column)
            .collect();

        match matches.len() {
            0 => Ok(None),
            1 => {
                // TODO: dedupe this.
                let m = matches.first().unwrap();
                let col_ref = &m.0;
                let ty = sim
                    .get_table(&col_ref.qualifier)
                    .unwrap()
                    .get_column(&col_ref.name)
                    .unwrap()
                    .ty
                    .clone();

                Ok(Some(ty))
            }
            _ => {
                let same_logical = matches.iter().map(|m| m.1).all_equal();

                // It is only ambiguous if they map to different logical columns.
                if !same_logical {
                    Err(Error::AmbiguousColumn(column.to_string()))
                } else {
                    let m = matches.first().unwrap();
                    let col_ref = &m.0;
                    let ty = sim
                        .get_table(&col_ref.qualifier)
                        .unwrap()
                        .get_column(&col_ref.name)
                        .unwrap()
                        .ty
                        .clone();

                    Ok(Some(ty))
                }
            }
        }
    }

    fn infer_qualified_type(
        &self,
        sim: &Simulator,
        qualifier: &str,
        column: &str,
        matched: &mut bool,
    ) -> Option<SqlType> {
        // Check raw qualifier first.
        let columns: Vec<String> = self
            .refs
            .iter()
            .filter(|(r, _)| r.qualifier == qualifier)
            .map(|(r, _)| r.name.clone())
            .collect();

        if !columns.is_empty() {
            *matched = true;
            if columns.contains(&column.to_string()) {
                return Some(
                    sim.get_table(qualifier)
                        .unwrap()
                        .get_column(column)
                        .unwrap()
                        .ty
                        .clone(),
                );
            }
        }

        // Check if it is an alias.
        if let Some(table_name) = self.aliases.get(qualifier) {
            *matched = true;
            if self.refs.contains_key(&ColumnRef::new(table_name, column)) {
                return Some(
                    sim.get_table(table_name)
                        .unwrap()
                        .get_column(column)
                        .unwrap()
                        .ty
                        .clone(),
                );
            }
        }

        None
    }
}

pub struct JoinInferrer<'a> {
    pub join_contexts: &'a [JoinContext],
}

impl<'a> ColumnInferrer for JoinInferrer<'a> {
    fn infer_unqualified_type(
        &self,
        sim: &Simulator,
        column: &str,
    ) -> Result<Option<SqlType>, Error> {
        let mut found_ty: Option<SqlType> = None;

        for join_ctx in self.join_contexts {
            let new_found_ty = join_ctx.infer_unqualified_type(sim, column)?;
            if let Some(ty) = new_found_ty {
                match found_ty {
                    Some(_) => return Err(Error::AmbiguousColumn(column.to_string())),
                    None => found_ty = Some(ty),
                }
            }
        }

        Ok(found_ty)
    }

    fn infer_qualified_type(
        &self,
        sim: &Simulator,
        qualifier: &str,
        column: &str,
    ) -> Result<SqlType, Error> {
        let mut matched = false;
        // Search for Absolutes first.
        for join_ctx in self.join_contexts {
            if let Some(ty) = join_ctx.infer_qualified_type(sim, qualifier, column, &mut matched) {
                return Ok(ty);
            }
        }

        if matched {
            Err(Error::ColumnDoesntExist(column.to_string()))
        } else {
            Err(Error::TableOrAliasDoesntExist(qualifier.to_string()))
        }
    }
}

struct JoinContextInferrer<'a> {
    join_ctx: &'a JoinContext,
    right_table: (&'a str, &'a Table),
}

impl<'a> ColumnInferrer for JoinContextInferrer<'a> {
    fn infer_unqualified_type(
        &self,
        sim: &Simulator,
        column: &str,
    ) -> Result<Option<SqlType>, Error> {
        // Search Join Table.
        let mut found_ty = self.join_ctx.infer_unqualified_type(sim, column)?;

        // Search Right Table.
        if let Some(col) = self.right_table.1.get_column(column) {
            match found_ty {
                // Ensure that the unqualified column is unique.
                Some(_) => return Err(Error::AmbiguousColumn(column.to_string())),
                None => found_ty = Some(col.ty.clone()),
            }
        };

        Ok(found_ty)
    }

    fn infer_qualified_type(
        &self,
        sim: &Simulator,
        qualifier: &str,
        column: &str,
    ) -> Result<SqlType, Error> {
        let mut matched = false;

        if let Some(ty) = self
            .join_ctx
            .infer_qualified_type(sim, qualifier, column, &mut matched)
        {
            Ok(ty)
        } else {
            // Otherwise, try to find it in the right table...
            if self.right_table.0 == qualifier {
                matched = true;
                if let Some(col) = self.right_table.1.get_column(column) {
                    return Ok(col.ty.clone());
                }
            }

            if matched {
                Err(Error::ColumnDoesntExist(column.to_string()))
            } else {
                Err(Error::TableOrAliasDoesntExist(qualifier.to_string()))
            }
        }
    }
}
