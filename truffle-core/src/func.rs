use sqlparser::ast::{
    DuplicateTreatment, Expr, Function, FunctionArg, FunctionArgExpr, FunctionArguments,
};

use crate::{Error, object_name_to_strings};

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

impl SqlFunction {
    pub fn from_ast(func: Function) -> Result<SqlFunction, Error> {
        let func_name = func.name.0.first().unwrap().to_string().to_lowercase();

        match func_name.as_str() {
            "count" => Self::parse_count_function(func.args),
            _ => todo!(),
        }
    }

    fn parse_count_function(args: FunctionArguments) -> Result<SqlFunction, Error> {
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

                if list.args.len() == 1 {
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
                } else {
                    todo!("Error")
                }
            }
            _ => todo!(),
        }
    }
}
