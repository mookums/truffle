use sqlparser::ast::{Expr, Function, FunctionArg, FunctionArgExpr, FunctionArguments};

use crate::{
    Error, Simulator,
    column::Column,
    expr::{ColumnInferrer, InferContext},
    resolve::ResolvedQuery,
    ty::SqlType,
};

impl Simulator {
    pub(crate) fn infer_function_column<I: ColumnInferrer>(
        &self,
        func: &Function,
        context: InferContext,
        inferrer: &I,
        resolved: &mut ResolvedQuery,
    ) -> Result<Column, Error> {
        let func_name = func.name.0.first().unwrap().to_string().to_lowercase();

        match func_name.as_str() {
            "count" => self.sql_count(&func.args, context, inferrer, resolved),
            "coalesce" => self.sql_coalesce(&func.args, context, inferrer, resolved),
            _ => Err(Error::FunctionDoesntExist(func_name)),
        }
    }

    fn sql_count<I: ColumnInferrer>(
        &self,
        args: &FunctionArguments,
        _: InferContext,
        inferrer: &I,
        _: &mut ResolvedQuery,
    ) -> Result<Column, Error> {
        let count_column = Column::new(SqlType::Integer, false, false);

        match args {
            FunctionArguments::List(list) => {
                // COUNT can only take in one argument.
                if list.args.len() != 1 {
                    return Err(Error::FunctionArgumentCount {
                        expected: 1,
                        got: list.args.len(),
                    });
                }

                let arg = list.args.first().unwrap();
                match arg {
                    FunctionArg::Unnamed(arg_expr) => match arg_expr {
                        FunctionArgExpr::Expr(expr) => match expr {
                            Expr::Identifier(ident) => {
                                let column_name = ident.value.clone();

                                inferrer
                                    .infer_unqualified_column(self, &column_name)?
                                    .ok_or_else(|| Error::ColumnDoesntExist(column_name.clone()))?;
                            }
                            Expr::CompoundIdentifier(idents) => {
                                let qualifier = &idents.first().unwrap().value;
                                let column_name = &idents.get(1).unwrap().value;

                                inferrer.infer_qualified_column(self, qualifier, column_name)?;
                            }
                            _ => todo!(),
                        },
                        FunctionArgExpr::QualifiedWildcard(_) => {
                            // TODO: Must be able to look up the qualifier with the Inferrer and be able to
                            // determine if it is a valid table or not.
                        }
                        FunctionArgExpr::Wildcard => {}
                    },
                    _ => todo!(),
                }

                Ok(count_column)
            }
            _ => todo!(),
        }
    }

    fn sql_coalesce<I: ColumnInferrer>(
        &self,
        args: &FunctionArguments,
        context: InferContext,
        inferrer: &I,
        resolved: &mut ResolvedQuery,
    ) -> Result<Column, Error> {
        match args {
            FunctionArguments::List(list) => {
                let mut ty: Option<SqlType> = None;
                let mut nullable = true;

                for arg in &list.args {
                    match arg {
                        FunctionArg::Unnamed(expr) => match expr {
                            FunctionArgExpr::Expr(expr) => {
                                let ctx = ty
                                    .as_ref()
                                    .map(|t| {
                                        // placeholders and values to COALESCE can be null.
                                        InferContext::default()
                                            .with_type(t.clone())
                                            .with_nullable(true)
                                    })
                                    .unwrap_or_else(|| context.clone());

                                let col = self.infer_expr_column(expr, ctx, inferrer, resolved)?;

                                // Nullable only if all columns are nullable,
                                // otherwise coalesce collapses to not null.
                                if !col.nullable {
                                    nullable = false;
                                }

                                match ty {
                                    Some(ref ty) => {
                                        if &col.ty != ty {
                                            return Err(Error::TypeMismatch {
                                                expected: ty.clone(),
                                                got: col.ty,
                                            });
                                        }
                                    }
                                    None => ty = Some(col.ty),
                                }
                            }
                            FunctionArgExpr::QualifiedWildcard(_) => {
                                return Err(Error::FunctionCall(
                                    "Coalesce operates on individual columns/values.".to_string(),
                                ));
                            }
                            FunctionArgExpr::Wildcard => {
                                return Err(Error::FunctionCall(
                                    "Coalesce operates on individual columns/values.".to_string(),
                                ));
                            }
                        },
                        _ => todo!(),
                    }
                }

                if let Some(ty) = ty.as_ref() {
                    Ok(Column::new(ty.clone(), nullable, false))
                } else {
                    Err(Error::FunctionCall(
                        "Missing arguments for Coalesce".to_string(),
                    ))
                }
            }
            _ => todo!(),
        }
    }
}
