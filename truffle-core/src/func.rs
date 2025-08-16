use sqlparser::ast::{
    DuplicateTreatment, Expr, Function, FunctionArg, FunctionArgExpr, FunctionArguments,
};

use crate::{
    Error, Simulator,
    column::Column,
    expr::{ColumnInferrer, InferContext},
    object_name_to_strings,
    resolve::ResolvedQuery,
    ty::SqlType,
};

pub enum ColumnRef {
    Wildcard,
    QualifiedWildcard(String),
    Column(String),
    QualifiedColumn(String, String),
}

pub enum SqlFunction {
    Count { distinct: bool, column: ColumnRef },
    Sum { distinct: bool, column: ColumnRef },
}

impl Simulator {
    pub(crate) fn infer_function_column<I: ColumnInferrer>(
        &self,
        func: &Function,
        context: InferContext,
        inferrer: &I,
        resolved: &mut ResolvedQuery,
    ) -> Result<Column, Error> {
        let func_name = func.name.0.first().unwrap().to_string().to_lowercase();

        let sql_func = match func_name.as_str() {
            "count" => SqlFunction::parse_count_function(&func.args)?,
            _ => return Err(Error::FunctionDoesntExist(func_name)),
        };

        match sql_func {
            SqlFunction::Count { column, .. } => {
                match column {
                    ColumnRef::Wildcard => {}
                    ColumnRef::QualifiedWildcard(_) => todo!(),
                    ColumnRef::Column(column_name) => {
                        inferrer.infer_unqualified_column(self, &column_name)?;
                    }
                    ColumnRef::QualifiedColumn(qualifier, name) => {
                        inferrer.infer_qualified_column(self, &qualifier, &name)?;
                    }
                }

                Ok(Column::new(SqlType::Integer, false, false))
            }
            _ => todo!(),
        }
    }
}

impl SqlFunction {
    fn parse_count_function(args: &FunctionArguments) -> Result<SqlFunction, Error> {
        match args {
            FunctionArguments::List(list) => {
                let distinct = if let Some(duplicate_treatment) = list.duplicate_treatment {
                    match duplicate_treatment {
                        DuplicateTreatment::Distinct => true,
                        DuplicateTreatment::All => false,
                    }
                } else {
                    false
                };

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
                            Expr::Identifier(ident) => Ok(SqlFunction::Count {
                                distinct,
                                column: ColumnRef::Column(ident.to_string()),
                            }),
                            _ => todo!(),
                        },
                        FunctionArgExpr::QualifiedWildcard(object_name) => {
                            let name = &object_name_to_strings(object_name)[0];
                            Ok(SqlFunction::Count {
                                distinct,
                                column: ColumnRef::QualifiedWildcard(name.to_string()),
                            })
                        }
                        FunctionArgExpr::Wildcard => Ok(SqlFunction::Count {
                            distinct,
                            column: ColumnRef::Wildcard,
                        }),
                    },
                    _ => todo!(),
                }
            }
            _ => todo!(),
        }
    }
}
