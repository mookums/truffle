use std::{
    collections::{HashMap, hash_map},
    rc::Rc,
};

use itertools::Itertools;
use sqlparser::ast::{Join, JoinConstraint, JoinOperator, TableFactor};

use crate::{
    Error, Simulator,
    column::Column,
    expr::{ColumnInferrer, InferType},
    object_name_to_strings,
    resolve::ResolvedQuery,
    table::Table,
    ty::SqlType,
};

impl Simulator {
    pub(crate) fn infer_joins(
        &self,
        table: &Table,
        name: &str,
        alias: Option<&String>,
        joins: &[Join],
        resolved: &mut ResolvedQuery,
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
                        | JoinOperator::Inner(join_constraint) => self.handle_join_constraint(
                            join_constraint,
                            &mut join_ctx,
                            right_table,
                            &right_table_name,
                            right_table_alias.as_ref(),
                            resolved,
                        )?,
                        JoinOperator::Left(join_constraint)
                        | JoinOperator::LeftOuter(join_constraint) => self.handle_join_constraint(
                            join_constraint,
                            &mut join_ctx,
                            right_table,
                            &right_table_name,
                            right_table_alias.as_ref(),
                            resolved,
                        )?,
                        JoinOperator::Right(join_constraint)
                        | JoinOperator::RightOuter(join_constraint) => self
                            .handle_join_constraint(
                                join_constraint,
                                &mut join_ctx,
                                right_table,
                                &right_table_name,
                                right_table_alias.as_ref(),
                                resolved,
                            )?,
                        JoinOperator::FullOuter(join_constraint) => self.handle_join_constraint(
                            join_constraint,
                            &mut join_ctx,
                            right_table,
                            &right_table_name,
                            right_table_alias.as_ref(),
                            resolved,
                        )?,
                        JoinOperator::CrossJoin => join_ctx.join_table(
                            right_table,
                            right_table_name,
                            right_table_alias,
                            JoinKind::Cross,
                        )?,
                        _ => {
                            return Err(Error::Unsupported(format!(
                                "Unsupported Join Operator: {:?}",
                                join.join_operator
                            )));
                        }
                    }
                }
                _ => {
                    return Err(Error::Unsupported(format!(
                        "Unsupported Join TableFactor: {}",
                        join.relation
                    )));
                }
            }
        }

        Ok(join_ctx)
    }

    fn handle_join_constraint(
        &self,
        join_constraint: &JoinConstraint,
        join_ctx: &mut JoinContext,
        right_table: &Table,
        right_table_name: &str,
        right_table_alias: Option<&String>,
        resolved: &mut ResolvedQuery,
    ) -> Result<(), Error> {
        match join_constraint {
            JoinConstraint::On(expr) => {
                let inferrer = JoinContextInferrer {
                    join_ctx,
                    right_table: (
                        right_table_name,
                        right_table_alias.map(|x| x.as_str()),
                        right_table,
                    ),
                };

                let ty = self.infer_expr_type(
                    expr,
                    InferType::Required(SqlType::Boolean),
                    &inferrer,
                    resolved,
                )?;

                if ty != SqlType::Boolean {
                    return Err(Error::TypeMismatch {
                        expected: SqlType::Boolean,
                        got: ty,
                    });
                }

                join_ctx.join_table(
                    right_table,
                    right_table_name,
                    right_table_alias,
                    JoinKind::Cross,
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
                        // .unique_by(|(_, idx)| *idx)
                        .filter(|(r, _)| &r.name == column_name)
                        .at_most_one()
                        .map_err(|_| Error::AmbiguousColumn(column_name.to_string()))?
                    {
                        let table_name = &col_ref.qualifier;
                        let column = join_ctx
                            .get_qualified_column(table_name, column_name)?
                            .unwrap();

                        Some(column.ty.clone())
                    } else {
                        return Err(Error::ColumnDoesntExist(column_name.to_string()));
                    };

                    let right_ty = right_table.get_column(column_name).map(|rc| rc.ty.clone());

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
                            return Err(Error::ColumnDoesntExist(column_name.to_string()));
                        }
                    }
                }

                join_ctx.join_table(
                    right_table,
                    right_table_name,
                    right_table_alias,
                    JoinKind::Using(column_names),
                )?;
            }
            JoinConstraint::Natural => {
                let mut found_common_column = false;

                // Check all columns from left tables against right table
                for (col_ref, column) in join_ctx.refs.iter().unique_by(|r| Rc::as_ptr(r.1)) {
                    let column_name = &col_ref.name;

                    if let Some(right_column) = right_table.get_column(column_name) {
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
                    right_table_name,
                    right_table_alias,
                    JoinKind::Natural,
                )?;
            }
            JoinConstraint::None => {
                join_ctx.join_table(
                    right_table,
                    right_table_name,
                    right_table_alias,
                    JoinKind::Cross,
                )?;
            }
        };

        Ok(())
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

#[derive(Debug)]
pub struct JoinContext {
    pub refs: HashMap<ColumnRef, Rc<Column>>,
}

enum JoinKind {
    Cross,
    Natural,
    Using(Vec<String>),
}

impl JoinContext {
    fn from_table(
        table: &Table,
        name: impl ToString,
        alias: Option<impl ToString>,
    ) -> Result<JoinContext, Error> {
        let table_columns = table.columns.clone();
        let mut refs = HashMap::new();

        let table_name = name.to_string();

        for (column_name, column) in table_columns.iter() {
            let col_rc = Rc::new(column.clone());
            assert!(
                refs.insert(ColumnRef::new(&table_name, column_name), col_rc.clone())
                    .is_none()
            );

            if let Some(alias) = &alias {
                assert!(
                    refs.insert(ColumnRef::new(alias.to_string(), column_name), col_rc)
                        .is_none()
                )
            }
        }

        Ok(JoinContext { refs })
    }

    fn join_table(
        &mut self,
        table: &Table,
        table_name: impl ToString,
        alias: Option<impl ToString>,
        kind: JoinKind,
    ) -> Result<(), Error> {
        let columns = table.columns.clone();
        let table_name = table_name.to_string();

        match kind {
            JoinKind::Cross => {
                eprintln!("JOIN table columns: {columns:?}");
                // add all columns from the right to the left
                for (column_name, column) in columns.iter() {
                    let existing_column_rc = self
                        .refs
                        .iter()
                        .filter(|r| r.0.qualifier == table_name)
                        .find_map(|(col_ref, col_rc)| {
                            if col_ref.name == *column_name {
                                Some(col_rc.clone())
                            } else {
                                None
                            }
                        });

                    let col_rc = existing_column_rc.unwrap_or_else(|| Rc::new(column.clone()));

                    match self.refs.entry(ColumnRef::new(&table_name, column_name)) {
                        hash_map::Entry::Occupied(occupied_entry) => {
                            assert!(
                                Rc::ptr_eq(occupied_entry.get(), &col_rc),
                                "Table name collision with different logical columns"
                            )
                        }
                        hash_map::Entry::Vacant(vacant_entry) => {
                            vacant_entry.insert(col_rc.clone());
                        }
                    }

                    if let Some(alias) = &alias {
                        self.refs
                            .insert(ColumnRef::new(alias.to_string(), column_name), col_rc)
                            .map_or(Ok(()), |_| Err(Error::AmbiguousAlias(alias.to_string())))?;
                    }
                }
            }
            JoinKind::Natural => {
                let all_existing_columns: Vec<String> =
                    self.refs.keys().map(|r| r.name.clone()).collect();

                for (column_name, column) in columns.iter() {
                    if all_existing_columns.contains(column_name) {
                        let existing_col_rc = self
                            .refs
                            .iter()
                            .find_map(|(col_ref, col_rc)| {
                                if col_ref.name == *column_name {
                                    Some(col_rc.clone())
                                } else {
                                    None
                                }
                            })
                            .unwrap();

                        match self.refs.entry(ColumnRef::new(&table_name, column_name)) {
                            hash_map::Entry::Occupied(occupied_entry) => {
                                assert!(
                                    Rc::ptr_eq(occupied_entry.get(), &existing_col_rc),
                                    "Table name collision with different logical columns"
                                )
                            }
                            hash_map::Entry::Vacant(vacant_entry) => {
                                vacant_entry.insert(existing_col_rc.clone());
                            }
                        }

                        if let Some(alias) = &alias {
                            self.refs
                                .insert(
                                    ColumnRef::new(alias.to_string(), column_name),
                                    existing_col_rc,
                                )
                                .map_or(Ok(()), |_| {
                                    Err(Error::AmbiguousAlias(alias.to_string()))
                                })?;
                        }
                    } else {
                        let col_rc = Rc::new(column.clone());

                        match self.refs.entry(ColumnRef::new(&table_name, column_name)) {
                            hash_map::Entry::Occupied(occupied_entry) => {
                                assert!(
                                    Rc::ptr_eq(occupied_entry.get(), &col_rc),
                                    "Table name collision with different logical columns"
                                )
                            }
                            hash_map::Entry::Vacant(vacant_entry) => {
                                vacant_entry.insert(col_rc.clone());
                            }
                        }

                        if let Some(alias) = &alias {
                            self.refs
                                .insert(ColumnRef::new(alias.to_string(), column_name), col_rc)
                                .map_or(Ok(()), |_| {
                                    Err(Error::AmbiguousAlias(alias.to_string()))
                                })?;
                        }
                    }
                }
            }
            JoinKind::Using(commons) => {
                for (column_name, column) in columns.iter() {
                    if commons.contains(column_name) {
                        let existing_col_rc = self
                            .refs
                            .iter()
                            .filter_map(|(col_ref, col_rc)| {
                                if col_ref.name == *column_name {
                                    Some(col_rc.clone())
                                } else {
                                    None
                                }
                            })
                            .exactly_one()
                            .unwrap();

                        match self.refs.entry(ColumnRef::new(&table_name, column_name)) {
                            hash_map::Entry::Occupied(occupied_entry) => {
                                assert!(
                                    Rc::ptr_eq(occupied_entry.get(), &existing_col_rc),
                                    "Table name collision with different logical columns"
                                )
                            }
                            hash_map::Entry::Vacant(vacant_entry) => {
                                vacant_entry.insert(existing_col_rc.clone());
                            }
                        }

                        if let Some(alias) = &alias {
                            self.refs
                                .insert(
                                    ColumnRef::new(alias.to_string(), column_name),
                                    existing_col_rc,
                                )
                                .map_or(Ok(()), |_| {
                                    Err(Error::AmbiguousAlias(alias.to_string()))
                                })?;
                        }
                    } else {
                        let col_rc = Rc::new(column.clone());

                        match self.refs.entry(ColumnRef::new(&table_name, column_name)) {
                            hash_map::Entry::Occupied(occupied_entry) => {
                                assert!(
                                    Rc::ptr_eq(occupied_entry.get(), &col_rc),
                                    "Table name collision with different logical columns"
                                )
                            }
                            hash_map::Entry::Vacant(vacant_entry) => {
                                vacant_entry.insert(col_rc.clone());
                            }
                        }

                        if let Some(alias) = &alias {
                            self.refs
                                .insert(ColumnRef::new(alias.to_string(), column_name), col_rc)
                                .map_or(Ok(()), |_| {
                                    Err(Error::AmbiguousAlias(alias.to_string()))
                                })?;
                        }
                    }
                }
            }
        }

        Ok(())
    }

    pub fn has_qualifier(&self, table: &str) -> bool {
        self.refs.keys().any(|k| k.qualifier == table)
    }

    pub fn get_column(&self, column: &str) -> Result<Option<Column>, Error> {
        fn match_into_column(
            join_ctx: &JoinContext,
            matches: &[(ColumnRef, Rc<Column>)],
        ) -> Column {
            matches
                .first()
                .as_ref()
                .and_then(|m| {
                    join_ctx.refs.iter().find_map(
                        |(col_ref, col_rc)| {
                            if col_ref == &m.0 { Some(col_rc) } else { None }
                        },
                    )
                })
                .map(|c| Column::clone(c))
                .unwrap()
        }

        let matches: Vec<(ColumnRef, Rc<Column>)> = self
            .refs
            .clone()
            .into_iter()
            .filter(|(r, _)| r.name == column)
            .collect();

        match matches.len() {
            0 => Ok(None),
            1 => Ok(Some(match_into_column(self, &matches))),
            _ => {
                // We care if the Rcs are the same, not the underlying value.
                let same_logical = matches.iter().map(|m| Rc::as_ptr(&m.1)).all_equal();
                if same_logical {
                    Ok(Some(match_into_column(self, &matches)))
                } else {
                    // It is only ambiguous if they map to different logical columns.
                    Err(Error::AmbiguousColumn(column.to_string()))
                }
            }
        }
    }

    pub fn get_qualified_column(
        &self,
        qualifier: &str,
        column: &str,
    ) -> Result<Option<Column>, Error> {
        let matches: Vec<_> = self
            .refs
            .iter()
            .filter(|(col_ref, _)| col_ref.qualifier == qualifier && col_ref.name == column)
            .collect();

        match matches.len() {
            0 => Ok(None),
            1 => Ok(matches.first().map(|m| Column::clone(m.1))),
            _ => {
                // Should be impossible for us to have multiple logical columns for a qualified match.
                if matches.iter().map(|(_, idx)| idx).all_equal() {
                    Ok(matches.first().map(|m| Column::clone(m.1)))
                } else {
                    unreachable!()
                }
            }
        }
    }

    fn infer_unqualified_type(&self, column: &str) -> Result<Option<SqlType>, Error> {
        Ok(self.get_column(column)?.map(|col| col.ty.clone()))
    }

    fn infer_qualified_type(&self, qualifier: &str, column: &str) -> Result<SqlType, Error> {
        self.get_qualified_column(qualifier, column)?
            .map(|col| col.ty.clone())
            .ok_or_else(|| Error::ColumnDoesntExist(column.to_string()))
    }
}

pub struct JoinInferrer<'a> {
    pub join_contexts: &'a [JoinContext],
}

impl<'a> ColumnInferrer for JoinInferrer<'a> {
    fn infer_unqualified_type(
        &self,
        _sim: &Simulator,
        column: &str,
    ) -> Result<Option<SqlType>, Error> {
        let mut found_ty: Option<SqlType> = None;

        for join_ctx in self.join_contexts {
            if let Some(ty) = join_ctx.infer_unqualified_type(column)? {
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
        _sim: &Simulator,
        qualifier: &str,
        column: &str,
    ) -> Result<SqlType, Error> {
        for join_ctx in self.join_contexts {
            if let Ok(ty) = join_ctx.infer_qualified_type(qualifier, column) {
                return Ok(ty);
            }
        }

        Err(Error::QualifiedColumnDoesntExist {
            qualifier: qualifier.to_string(),
            column: column.to_string(),
        })
    }
}

struct JoinContextInferrer<'a> {
    join_ctx: &'a JoinContext,
    right_table: (&'a str, Option<&'a str>, &'a Table),
}

impl<'a> ColumnInferrer for JoinContextInferrer<'a> {
    fn infer_unqualified_type(
        &self,
        _sim: &Simulator,
        column: &str,
    ) -> Result<Option<SqlType>, Error> {
        // Search Join Table.
        let mut found_ty = self.join_ctx.infer_unqualified_type(column)?;

        // Search Right Table.
        if let Some(col) = self.right_table.2.get_column(column) {
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
        _sim: &Simulator,
        qualifier: &str,
        column: &str,
    ) -> Result<SqlType, Error> {
        if let Ok(ty) = self.join_ctx.infer_qualified_type(qualifier, column) {
            Ok(ty)
        } else {
            if let Some(right_alias) = self.right_table.1
                && qualifier == right_alias
            {
                if let Some(col) = self.right_table.2.get_column(column) {
                    return Ok(col.ty.clone());
                }
            } else if qualifier == self.right_table.0 {
                if let Some(col) = self.right_table.2.get_column(column) {
                    return Ok(col.ty.clone());
                }
            }

            Err(Error::QualifiedColumnDoesntExist {
                qualifier: qualifier.to_string(),
                column: column.to_string(),
            })
        }
    }
}
