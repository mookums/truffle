use std::{collections::HashSet, rc::Rc};

use itertools::Itertools;
use sqlparser::ast::{
    GroupByExpr, Query, SelectItem, SelectItemQualifiedWildcardKind, TableFactor,
};

use crate::{
    Error, Simulator,
    action::join::JoinInferrer,
    expr::{InferContext, Scope},
    object_name_to_strings,
    resolve::{ColumnRef, ResolvedQuery},
    ty::SqlType,
};

impl Simulator {
    pub(crate) fn select(&self, query: &Query) -> Result<ResolvedQuery, Error> {
        let mut contexts = vec![];
        let mut resolved = ResolvedQuery::default();

        let sel = query
            .body
            .as_select()
            .expect("Query must be a SELECT by now.");

        for from in &sel.from {
            let TableFactor::Table { name, alias, .. } = &from.relation else {
                return Err(Error::Unsupported(format!(
                    "Unsupported Select Relation: {:?}",
                    from.relation
                )));
            };

            let from_table_name = &object_name_to_strings(name)[0];
            let from_table_alias = alias.as_ref().map(|a| &a.name.value);

            // Ensure the table exists.
            let from_table = self
                .get_table(from_table_name)
                .ok_or_else(|| Error::TableDoesntExist(from_table_name.clone()))?;

            // Ensure that the alias isn't a table name.
            if let Some(alias) = &from_table_alias
                && self.has_table(alias)
            {
                return Err(Error::AliasIsTableName(alias.to_string()));
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

        // Validate WHERE clause.
        if let Some(selection) = &sel.selection {
            self.infer_expr_column(
                selection,
                InferContext::default()
                    .with_type(SqlType::Boolean)
                    .with_scope(Scope::Row),
                &inferrer,
                &mut resolved,
            )?;
        }

        let mut grouped_exprs = Vec::new();

        // Validate Group By.
        match &sel.group_by {
            GroupByExpr::Expressions(exprs, ..) => {
                for expr in exprs {
                    let infer = self.infer_expr_column(
                        expr,
                        InferContext::default().with_scope(Scope::Row),
                        &inferrer,
                        &mut resolved,
                    )?;

                    grouped_exprs.push(expr.clone());

                    // We need to figure out a way to basically pass this information down the chain.
                    // Ensuring that we only do compatible operations on Grouped or NonGrouped columns.

                    // TODO: ensure type is comparable

                    _ = infer;
                }
            }
            _ => todo!("Unsupported GroupByExpr"),
        }

        let mut scope = if grouped_exprs.is_empty() {
            Scope::Literal
        } else {
            Scope::Group
        };

        // Validate HAVING clause.
        if let Some(having) = &sel.having {
            self.infer_expr_column(
                having,
                InferContext::default()
                    .with_type(SqlType::Boolean)
                    .with_scope(Scope::Group)
                    .with_grouped(&grouped_exprs),
                &inferrer,
                &mut resolved,
            )?;
        }

        for projection in &sel.projection {
            match projection {
                SelectItem::UnnamedExpr(expr) => {
                    // If we are grouped and this expression isn't, return an Error.
                    let infer = self.infer_expr_column(
                        expr,
                        InferContext::default()
                            .with_scope(scope)
                            .with_grouped(&grouped_exprs),
                        &inferrer,
                        &mut resolved,
                    )?;

                    scope = scope.combine(&infer.scope)?;

                    let key = Self::infer_expr_name(expr)?.unwrap_or_else(|| {
                        ColumnRef::new(None, resolved.outputs.len().to_string())
                    });

                    resolved.insert_output(key, infer.column);
                }
                SelectItem::ExprWithAlias { expr, alias } => {
                    let infer = self.infer_expr_column(
                        expr,
                        InferContext::default()
                            .with_scope(scope)
                            .with_grouped(&grouped_exprs),
                        &inferrer,
                        &mut resolved,
                    )?;

                    scope = scope.combine(&infer.scope)?;

                    let name = alias.value.to_string();

                    if resolved.get_output_with_name(&name).is_some() {
                        return Err(Error::AmbiguousAlias(name));
                    }

                    let key = ColumnRef::new(None, name);

                    resolved.insert_output(key, infer.column);
                }
                SelectItem::QualifiedWildcard(kind, _) => match kind {
                    SelectItemQualifiedWildcardKind::ObjectName(name) => {
                        let qualifier = &object_name_to_strings(name)[0];
                        let mut found = false;

                        for context in contexts.iter().filter(|c| c.has_qualifier(qualifier)) {
                            // We are about if the Rcs are the same, not the underlying value.
                            for (col_ref, _) in context
                                .refs
                                .iter()
                                .filter(|r| &r.0.qualifier == qualifier)
                                .unique_by(|r| Rc::as_ptr(r.1))
                            {
                                let true_column = context
                                    .get_qualified_column(&col_ref.qualifier, &col_ref.name)?;

                                resolved.insert_output(
                                    ColumnRef::new(
                                        Some(col_ref.qualifier.clone()),
                                        col_ref.name.clone(),
                                    ),
                                    true_column.clone(),
                                );

                                found = true;
                            }
                        }

                        scope = scope.combine(&Scope::Row)?;

                        if !found {
                            return Err(Error::QualifierDoesntExist(qualifier.to_string()));
                        }
                    }
                    SelectItemQualifiedWildcardKind::Expr(_) => {
                        return Err(Error::Unsupported(
                            "Expression as qualifier for wildcard in SELECT".to_string(),
                        ));
                    }
                },
                SelectItem::Wildcard(_) => {
                    let mut all_columns = HashSet::new();

                    for context in &contexts {
                        // We are about if the Rcs are the same, not the underlying value.
                        for (col_ref, _) in context.refs.iter().unique_by(|r| Rc::as_ptr(r.1)) {
                            let column_name = &col_ref.name;
                            if all_columns.contains(column_name) {
                                return Err(Error::AmbiguousColumn(column_name.to_string()));
                            } else {
                                // The existence of this column should've already been confirmed earlier.
                                let column = context
                                    .get_qualified_column(&col_ref.qualifier, &col_ref.name)?;

                                let key = ColumnRef::new(
                                    Some(col_ref.qualifier.clone()),
                                    col_ref.name.clone(),
                                );

                                resolved.insert_output(key, column.clone());
                                all_columns.insert(column_name.to_string());
                            }
                        }
                    }

                    scope = scope.combine(&Scope::Row)?;
                }
            }
        }

        // Validate Order By
        // if let Some(order_by) = &query.order_by {
        //     match &order_by.kind {
        //         OrderByKind::Expressions(order_by_exprs) => {
        //             for order_by_expr in order_by_exprs {
        //                 let col = self.infer_expr_column(
        //                     &order_by_expr.expr,
        //                     InferContext::default().with_scope(Scope::Row),
        //                     &inferrer,
        //                     &mut resolved,
        //                 )?;

        //                 // TODO: Ensure type is "comparable".
        //                 _ = col;
        //             }
        //         }
        //         _ => todo!("Unsupported OrderByKind"),
        //     }
        // }

        Ok(resolved)
    }
}
