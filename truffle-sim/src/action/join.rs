use std::collections::HashMap;

use indexmap::{IndexMap, map::Entry};
use sqlparser::ast::{Join, JoinConstraint, JoinOperator, TableFactor};

use crate::{
    Error, Simulator, expr::ColumnInferrer, object_name_to_strings, table::Table, ty::SqlType,
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

                                join_ctx.append_table(
                                    right_table,
                                    &right_table_name,
                                    right_table_alias,
                                )?;
                            }
                            // JoinConstraint::Using(object_names) => {
                            //     for object_name in object_names {
                            //         let column_name = object_name_to_strings(object_name)
                            //             .first()
                            //             .unwrap()
                            //             .clone();

                            //         if inferrer
                            //             .infer_unqualified_type(self, &column_name)?
                            //             .is_none()
                            //         {
                            //             return Err(Error::ColumnDoesntExist(column_name));
                            //         }
                            //     }

                            //     // add using columns from left and all from right.
                            // }
                            // JoinConstraint::Natural => {
                            //     let found_common_column = false;
                            //     // for (column_name, column) in &join_table.columns {
                            //     //     if let Some(right_column) = right_table.get_column(column_name)
                            //     //     {
                            //     //         if column.ty == right_column.ty {
                            //     //             found_common_column = true;
                            //     //             break;
                            //     //         } else {
                            //     //             return Err(Error::TypeMismatch {
                            //     //                 expected: column.ty.clone(),
                            //     //                 got: right_column.ty.clone(),
                            //     //             });
                            //     //         }
                            //     //     }
                            //     // }

                            //     if !found_common_column {
                            //         return Err(Error::NoCommonColumn);
                            //     }

                            //     // add common columns once and all non common from right
                            // }
                            // JoinConstraint::None => {}
                            _ => todo!(),
                        },
                        _ => todo!(),
                    }
                }
                _ => return Err(Error::Unsupported("Unsupported JOIN relation".to_string())),
            }
        }

        Ok(join_ctx)
    }
}

pub struct JoinContext {
    // Maps the alias to the table name
    pub aliases: HashMap<String, String>,
    // Maps the table name to the list of columns in this join table.
    pub tables: IndexMap<String, Vec<String>>,
}

impl JoinContext {
    pub fn from_table(
        table: &Table,
        name: impl ToString,
        alias: Option<impl ToString>,
    ) -> Result<JoinContext, Error> {
        let mut tables = IndexMap::new();
        let mut aliases = HashMap::new();

        let columns: Vec<String> = table.columns.iter().map(|c| c.0.clone()).collect();
        tables.insert(name.to_string(), columns);

        if let Some(alias) = alias {
            aliases.insert(alias.to_string(), name.to_string());
        }

        Ok(JoinContext { tables, aliases })
    }

    pub fn append_table(
        &mut self,
        table: &Table,
        name: impl ToString,
        alias: Option<impl ToString>,
    ) -> Result<(), Error> {
        let mut columns: Vec<String> = table.columns.iter().map(|c| c.0.clone()).collect();
        let table_name = name.to_string();
        match self.tables.entry(table_name) {
            Entry::Occupied(mut entry) => entry.get_mut().append(&mut columns),
            Entry::Vacant(entry) => _ = entry.insert(columns),
        };

        if let Some(alias) = alias {
            self.aliases.insert(alias.to_string(), name.to_string());
        }

        Ok(())
    }

    fn infer_unqualified_type(
        &self,
        sim: &Simulator,
        column: &str,
    ) -> Result<Option<SqlType>, Error> {
        let mut found_ty = None;

        for (table_name, columns) in &self.tables {
            if columns.contains(&column.to_string()) {
                match found_ty {
                    Some(_) => return Err(Error::AmbiguousColumn(column.to_string())),
                    None => {
                        found_ty = Some(
                            sim.get_table(table_name)
                                .unwrap()
                                .get_column(column)
                                .unwrap()
                                .ty
                                .clone(),
                        )
                    }
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
        matched: &mut bool,
    ) -> Option<SqlType> {
        if let Some(table) = self.tables.get(qualifier) {
            *matched = true;
            if table.contains(&column.to_string()) {
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

        if let Some(table_name) = self.aliases.get(qualifier) {
            let table = self.tables.get(table_name).unwrap();
            *matched = true;
            if table.contains(&column.to_string()) {
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
