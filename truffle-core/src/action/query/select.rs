use std::{collections::HashSet, rc::Rc};

use itertools::Itertools;
use sqlparser::ast::{
    Expr, Function, Select, SelectItem, SelectItemQualifiedWildcardKind, TableFactor, Value,
};

use crate::{
    Error, Simulator,
    action::join::{JoinContext, JoinInferrer},
    expr::{ColumnInferrer, InferContext},
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

            let from_table_name = &object_name_to_strings(name)[0];
            let from_table_alias = alias.as_ref().map(|a| &a.name.value);

            // Ensure the table exists.
            let from_table = self
                .get_table(from_table_name)
                .ok_or_else(|| Error::TableDoesntExist(from_table_name.clone()))?;

            // Ensure that the alias isn't a table name.
            if let Some(alias) = &from_table_alias {
                if self.has_table(alias) {
                    return Err(Error::AliasIsTableName(alias.to_string()));
                }
            }

            let join_table = self.infer_joins(
                from_table,
                from_table_name,
                from_table_alias,
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
                        let qualifier = &idents.first().unwrap().value;
                        let column_name = &idents.get(1).unwrap().value;

                        check_qualifier(&contexts, qualifier)?;
                        columns.expect_list_mut().push(SelectColumn::Qualified {
                            qualifier: qualifier.to_string(),
                            column: column_name.to_string(),
                        });
                    }
                    Expr::Function(function) => {
                        columns
                            .expect_list_mut()
                            .push(SelectColumn::Function(Box::new(function.clone())));
                    }
                    Expr::Value(val) => {
                        columns
                            .expect_list_mut()
                            .push(SelectColumn::Value(val.value.clone()));
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
                        let qualifier = object_name_to_strings(name)[0].clone();
                        columns
                            .expect_list_mut()
                            .push(SelectColumn::Wildcard(qualifier));
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
                    // We are about if the Rcs are the same, not the underlying value.
                    for (col_ref, _) in context.refs.iter().unique_by(|r| Rc::as_ptr(r.1)) {
                        let column_name = &col_ref.name;
                        if all_columns.contains(column_name) {
                            return Err(Error::AmbiguousColumn(column_name.to_string()));
                        } else {
                            // The existence of this column should've already been confirmed earlier.
                            let column =
                                context.get_qualified_column(&col_ref.qualifier, &col_ref.name)?;

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
                            let true_column = inferrer
                                .infer_unqualified_column(self, &column)?
                                .ok_or_else(|| Error::ColumnDoesntExist(column.clone()))?;

                            resolved.insert_output(
                                ResolveOutputKey::new(None, column),
                                true_column.clone(),
                            );
                        }
                        SelectColumn::Qualified { qualifier, column } => {
                            let true_column =
                                inferrer.infer_qualified_column(self, &qualifier, &column)?;

                            resolved.insert_output(
                                ResolveOutputKey::new(Some(qualifier), column),
                                true_column.clone(),
                            );
                        }
                        SelectColumn::Wildcard(qualifier) => {
                            let mut found = false;

                            for context in contexts.iter().filter(|c| c.has_qualifier(&qualifier)) {
                                // We are about if the Rcs are the same, not the underlying value.
                                for (col_ref, _) in context
                                    .refs
                                    .iter()
                                    .filter(|r| r.0.qualifier == qualifier)
                                    .unique_by(|r| Rc::as_ptr(r.1))
                                {
                                    let true_column = context
                                        .get_qualified_column(&col_ref.qualifier, &col_ref.name)?;

                                    resolved.insert_output(
                                        ResolveOutputKey::new(
                                            Some(col_ref.qualifier.clone()),
                                            col_ref.name.clone(),
                                        ),
                                        true_column.clone(),
                                    );

                                    found = true;
                                }
                            }

                            if !found {
                                return Err(Error::QualifierDoesntExist(qualifier.to_string()));
                            }
                        }
                        SelectColumn::Function(function) => {
                            let col = self.infer_function_column(
                                &function,
                                InferContext::default(),
                                &inferrer,
                                &mut resolved,
                            )?;

                            resolved.insert_output(
                                ResolveOutputKey {
                                    qualifier: None,
                                    name: function.name.to_string().to_lowercase(),
                                },
                                col,
                            );
                        }
                        SelectColumn::Value(val) => {
                            let col = Self::infer_value_column(
                                &val,
                                InferContext::default(),
                                &mut resolved,
                            )?;

                            // TODO: Determine what to use as Column name for raw value SELECTs
                            resolved.insert_output(
                                ResolveOutputKey {
                                    qualifier: None,
                                    name: "value".to_string(),
                                },
                                col,
                            );
                        }
                    }
                }
            }
        }

        // Validate WHERE clause.
        if let Some(selection) = &sel.selection {
            self.infer_expr_column(
                selection,
                InferContext::default().with_type(SqlType::Boolean),
                &inferrer,
                &mut resolved,
            )?;
        }

        Ok(resolved)
    }
}

#[derive(Debug)]
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

#[derive(Debug)]
pub enum SelectColumn {
    /// This is the true name of the column.
    Unqualified(String),
    Qualified {
        qualifier: String,
        column: String,
    },
    Wildcard(String),
    Function(Box<Function>),
    Value(Value),
}

fn check_qualifier(ctx: &[JoinContext], name: &str) -> Result<(), Error> {
    if ctx
        .iter()
        .any(|c| c.refs.iter().any(|(r, _)| r.qualifier == name))
    {
        Ok(())
    } else {
        Err(Error::QualifierDoesntExist(name.to_string()))
    }
}
