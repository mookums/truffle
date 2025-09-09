use sqlparser::ast::{Expr, Function, FunctionArg, FunctionArgExpr, FunctionArguments};

use crate::{
    Error, Simulator,
    column::Column,
    expr::{ColumnInferrer, InferContext, InferredColumn, Scope},
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
    ) -> Result<InferredColumn, Error> {
        let func_name = func.name.0.first().unwrap().to_string().to_lowercase();

        match func_name.as_str() {
            "count" => self.sql_count(&func.args, context, inferrer, resolved),
            "coalesce" => self.sql_coalesce(&func.args, context, inferrer, resolved),
            "avg" => self.sql_avg(&func.args, context, inferrer, resolved),
            "min" | "max" => self.sql_min_max(&func.args, context, inferrer, resolved),
            _ => Err(Error::FunctionDoesntExist(func_name)),
        }
    }

    fn sql_count<I: ColumnInferrer>(
        &self,
        args: &FunctionArguments,
        _: InferContext,
        inferrer: &I,
        _: &mut ResolvedQuery,
    ) -> Result<InferredColumn, Error> {
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

                Ok(InferredColumn {
                    column: count_column,
                    scope: Scope::Group,
                })
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
    ) -> Result<InferredColumn, Error> {
        let FunctionArguments::List(list) = args else {
            return Err(Error::FunctionCall(
                "Invalid arguments for COALESCE".to_string(),
            ));
        };

        let mut ty: Option<SqlType> = None;

        // First type pass, this gets the type to use.
        for arg in &list.args {
            let FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)) = arg else {
                return Err(Error::FunctionCall(
                    "COALESCE operates on individual columns/values.".to_string(),
                ));
            };

            let mut first_ctx = context.clone();
            first_ctx.constraints.ty = ty.clone();

            if let Ok(infer) = self.infer_expr_column(expr, first_ctx, inferrer, resolved) {
                match ty {
                    Some(ref ty) => {
                        if &infer.column.ty != ty {
                            return Err(Error::TypeMismatch {
                                expected: ty.clone(),
                                got: infer.column.ty,
                            });
                        }
                    }
                    None => ty = Some(infer.column.ty),
                }
            }
        }

        let mut nullable = true;
        let mut scope = Scope::Literal;

        let mut ctx = context.clone();
        ctx.constraints.ty = ty.clone();

        for arg in &list.args {
            let FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)) = arg else {
                unreachable!();
            };

            let mut ctx = context.clone();
            ctx.constraints.ty = ty.clone();
            ctx.constraints.nullable = Some(nullable);
            ctx.constraints.scope = Some(scope);

            let infer = self.infer_expr_column(expr, ctx, inferrer, resolved)?;

            // Nullable only if all columns are nullable,
            // otherwise coalesce collapses to not null.
            nullable &= infer.column.nullable;
            scope = scope.combine(&infer.scope)?;
        }

        if let Some(ty) = ty.as_ref() {
            Ok(InferredColumn {
                column: Column::new(ty.clone(), nullable, false),
                scope,
            })
        } else {
            Err(Error::FunctionCall(
                "Missing arguments for Coalesce".to_string(),
            ))
        }
    }

    fn sql_avg<I: ColumnInferrer>(
        &self,
        args: &FunctionArguments,
        context: InferContext,
        inferrer: &I,
        resolved: &mut ResolvedQuery,
    ) -> Result<InferredColumn, Error> {
        let FunctionArguments::List(list) = args else {
            return Err(Error::FunctionCall("Invalid arguments for AVG".to_string()));
        };

        // AVG can only take in one argument.
        if list.args.len() != 1 {
            return Err(Error::FunctionArgumentCount {
                expected: 1,
                got: list.args.len(),
            });
        }

        let arg = list.args.first().unwrap();
        let FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)) = arg else {
            return Err(Error::FunctionCall(
                "AVG operates only on individual rows/values.".to_string(),
            ));
        };

        let mut ctx = context.clone();
        ctx.constraints.scope = Some(Scope::Row);

        let infer = self.infer_expr_column(expr, ctx, inferrer, resolved)?;

        // Must be numeric.
        if !infer.column.ty.is_numeric() {
            return Err(Error::TypeNotNumeric(infer.column.ty));
        }

        Ok(InferredColumn {
            column: infer.column,
            scope: Scope::Group,
        })
    }

    fn sql_min_max<I: ColumnInferrer>(
        &self,
        args: &FunctionArguments,
        context: InferContext,
        inferrer: &I,
        resolved: &mut ResolvedQuery,
    ) -> Result<InferredColumn, Error> {
        let FunctionArguments::List(list) = args else {
            return Err(Error::FunctionCall(
                "Invalid arguments for MIN/MAX".to_string(),
            ));
        };

        // MIN/MAX can only take in one argument.
        if list.args.len() != 1 {
            return Err(Error::FunctionArgumentCount {
                expected: 1,
                got: list.args.len(),
            });
        }

        let arg = list.args.first().unwrap();

        let FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)) = arg else {
            return Err(Error::FunctionCall(
                "MIN/MAX operates only on individual rows/values.".to_string(),
            ));
        };

        let mut ctx = context.clone();
        ctx.constraints.scope = Some(Scope::Row);

        let infer = self.infer_expr_column(expr, ctx, inferrer, resolved)?;

        Ok(InferredColumn {
            column: infer.column,
            scope: Scope::Group,
        })
    }
}
