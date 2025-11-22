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
            self.process_returning(
                returning,
                &inferrer,
                table_name,
                alias.as_deref(),
                table,
                &mut resolved,
            )?;
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
