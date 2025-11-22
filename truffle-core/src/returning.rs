use crate::Error;
use crate::Table;
use crate::expr::InferContext;
use crate::object_name_to_strings;
use crate::resolve::ColumnRef;
use sqlparser::ast::Expr;
use sqlparser::ast::SelectItem;
use sqlparser::ast::SelectItemQualifiedWildcardKind;

use crate::{Simulator, expr::ColumnInferrer, resolve::ResolvedQuery};

impl Simulator {
    pub(crate) fn process_returning(
        &self,
        returning_items: Vec<SelectItem>,
        inferrer: &impl ColumnInferrer,
        table_name: &str,
        alias: Option<&str>,
        table: &Table,
        resolved: &mut ResolvedQuery,
    ) -> Result<(), Error> {
        for item in returning_items {
            match item {
                SelectItem::UnnamedExpr(expr) => match expr {
                    Expr::Identifier(ident) => {
                        let column = ident.value.clone();

                        let true_column = inferrer
                            .infer_unqualified_column(self, &column)?
                            .ok_or_else(|| Error::ColumnDoesntExist(column.clone()))?;

                        let key = ColumnRef::new(None, column.to_string());

                        resolved.insert_output(key, true_column);
                    }
                    Expr::CompoundIdentifier(idents) => {
                        let qualifier = &idents.first().unwrap().value;
                        let column_name = &idents.get(1).unwrap().value;

                        let true_column =
                            inferrer.infer_qualified_column(self, qualifier, column_name)?;

                        let key =
                            ColumnRef::new(Some(qualifier.to_string()), column_name.to_string());

                        resolved.insert_output(key, true_column);
                    }
                    _ => {
                        return Err(Error::Unsupported(format!(
                            "Unsupported Select Expr: {expr:?}"
                        )));
                    }
                },
                SelectItem::ExprWithAlias { expr, alias } => {
                    let infer =
                        self.infer_expr_column(&expr, InferContext::default(), inferrer, resolved)?;

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

                        // Validate that the qualifier matches either the table name or its alias
                        if qualifier == table_name || alias.is_some_and(|a| a == qualifier) {
                            for (column_name, column) in table.columns.iter() {
                                resolved.insert_output(
                                    ColumnRef {
                                        qualifier: Some(qualifier.clone()),
                                        name: column_name.to_string(),
                                    },
                                    column.clone(),
                                );
                            }
                        } else {
                            return Err(Error::QualifierDoesntExist(qualifier.to_string()));
                        }
                    }
                    SelectItemQualifiedWildcardKind::Expr(_) => {
                        return Err(Error::Unsupported(
                            "Expression as qualifier for wildcard in RETURNING".to_string(),
                        ));
                    }
                },
                SelectItem::Wildcard(_) => {
                    for (column_name, column) in table.columns.iter() {
                        resolved.insert_output(
                            ColumnRef {
                                qualifier: Some(table_name.to_string()),
                                name: column_name.to_string(),
                            },
                            column.clone(),
                        );
                    }
                }
            }
        }

        Ok(())
    }
}
