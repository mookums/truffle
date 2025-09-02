use sqlparser::ast::{
    Expr, Insert, SelectItem, SelectItemQualifiedWildcardKind, SetExpr, TableObject,
};

use crate::{
    Error, Simulator,
    column::Column,
    expr::{ColumnInferrer, InferConstraints, InferContext},
    object_name_to_strings,
    resolve::{ColumnRef, ResolvedQuery},
    table::Table,
};

impl Simulator {
    pub(crate) fn insert(&self, ins: Insert) -> Result<ResolvedQuery, Error> {
        let TableObject::TableName(table_object_name) = ins.table else {
            todo!();
        };

        // Only POSTGRES uses this.
        let alias = ins.table_alias.map(|i| i.value);
        let table_name = &object_name_to_strings(&table_object_name)[0];

        let table = self
            .get_table(table_name)
            .ok_or_else(|| Error::TableDoesntExist(table_name.clone()))?;

        let mut provided_columns = vec![];
        for column in ins.columns {
            let column_name = column.value;
            if !table.has_column(&column_name) {
                return Err(Error::ColumnDoesntExist(column_name));
            }

            provided_columns.push(column_name);
        }

        // This stores the return information for this query.
        let mut resolved = ResolvedQuery::default();
        let inferrer = InsertInferrer {
            table,
            table_name,
            alias: alias.as_deref(),
        };

        let source = ins.source.unwrap();
        match *source.body {
            SetExpr::Values(values) => {
                for row in values.rows {
                    // Ensure we have the correct number of columns.
                    if provided_columns.is_empty() {
                        if table.columns.len() != row.len() {
                            return Err(Error::ColumnCountMismatch {
                                expected: table.columns.len(),
                                got: row.len(),
                            });
                        }
                    } else if provided_columns.len() != row.len() {
                        return Err(Error::ColumnCountMismatch {
                            expected: provided_columns.len(),
                            got: row.len(),
                        });
                    }

                    for (i, (column_name, column)) in table.columns.iter().enumerate() {
                        if provided_columns.is_empty() {
                            // Implicit (Table Index) Columns.
                            let expr = &row[i];

                            _ = self.infer_expr_column(
                                expr,
                                InferContext {
                                    constraints: InferConstraints {
                                        ty: Some(column.ty.clone()),
                                        nullable: Some(column.nullable),
                                        ..Default::default()
                                    },
                                    ..Default::default()
                                },
                                &inferrer,
                                &mut resolved,
                            )?;
                        } else if let Some(index) =
                            provided_columns.iter().position(|pc| pc == column_name)
                        {
                            // If the column was named explicitly...
                            let expr = &row[index];

                            _ = self.infer_expr_column(
                                expr,
                                InferContext {
                                    constraints: InferConstraints {
                                        ty: Some(column.ty.clone()),
                                        nullable: Some(column.nullable),
                                        ..Default::default()
                                    },
                                    ..Default::default()
                                },
                                &inferrer,
                                &mut resolved,
                            )?;
                        } else if !(column.nullable || column.default) {
                            // If the column was not named explicitly, we check it.
                            return Err(Error::RequiredColumnMissing(column_name.to_string()));
                        }
                    }
                }
            }
            _ => todo!("Unexpected body for INSERT"),
        }

        if let Some(returning) = ins.returning {
            for item in returning {
                match item {
                    SelectItem::UnnamedExpr(expr) => match expr {
                        Expr::Identifier(ident) => {
                            let column = ident.value.clone();

                            let true_column = inferrer
                                .infer_unqualified_column(self, &column)?
                                .ok_or_else(|| Error::ColumnDoesntExist(column.clone()))?;

                            let key = ColumnRef::new(None, column.to_string());

                            resolved.insert_output(key, true_column.clone());
                        }
                        Expr::CompoundIdentifier(idents) => {
                            let qualifier = &idents.first().unwrap().value;
                            let column_name = &idents.get(1).unwrap().value;

                            let true_column =
                                inferrer.infer_qualified_column(self, qualifier, column_name)?;

                            let key = ColumnRef::new(
                                Some(qualifier.to_string()),
                                column_name.to_string(),
                            );

                            resolved.insert_output(key, true_column.clone());
                        }
                        _ => {
                            return Err(Error::Unsupported(format!(
                                "Unsupported Select Expr: {expr:?}"
                            )));
                        }
                    },
                    SelectItem::ExprWithAlias { expr, alias } => {
                        let infer = self.infer_expr_column(
                            &expr,
                            InferContext::default(),
                            &inferrer,
                            &mut resolved,
                        )?;

                        let name = alias.value.to_string();

                        if resolved.get_output_with_name(&name).is_some() {
                            return Err(Error::AmbiguousAlias(name));
                        }

                        let key = ColumnRef {
                            qualifier: None,
                            name,
                        };

                        resolved.insert_output(key, infer.column);
                    }
                    SelectItem::QualifiedWildcard(kind, _) => match kind {
                        SelectItemQualifiedWildcardKind::ObjectName(name) => {
                            let qualifier = &object_name_to_strings(&name)[0];

                            // TODO: Have a way to validate an alias through the inferrer.
                            if qualifier == table_name
                                || alias.as_ref().is_some_and(|a| a == qualifier)
                            {
                                for column in table.columns.iter() {
                                    resolved.insert_output(
                                        ColumnRef {
                                            qualifier: Some(qualifier.clone()),
                                            name: column.0.to_string(),
                                        },
                                        column.1.clone(),
                                    );
                                }
                            } else {
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
                        for column in table.columns.iter() {
                            resolved.insert_output(
                                ColumnRef {
                                    qualifier: Some(table_name.clone()),
                                    name: column.0.to_string(),
                                },
                                column.1.clone(),
                            );
                        }
                    }
                }
            }
        }

        Ok(resolved)
    }
}

struct InsertInferrer<'a> {
    table: &'a Table,
    table_name: &'a str,
    alias: Option<&'a str>,
}

impl<'a> ColumnInferrer for InsertInferrer<'a> {
    fn infer_unqualified_column(
        &self,
        _: &Simulator,
        column: &str,
    ) -> Result<Option<Column>, Error> {
        Ok(self.table.get_column(column).cloned())
    }

    fn infer_qualified_column(
        &self,
        _: &Simulator,
        qualifier: &str,
        column: &str,
    ) -> Result<Column, Error> {
        if qualifier == self.table_name || self.alias.is_some_and(|a| a == qualifier) {
            Ok(self
                .table
                .get_column(column)
                .cloned()
                .ok_or_else(|| Error::ColumnDoesntExist(column.to_string()))?)
        } else {
            Err(Error::QualifierDoesntExist(qualifier.to_string()))
        }
    }
}
