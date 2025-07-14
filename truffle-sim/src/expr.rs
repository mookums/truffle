use sqlparser::ast::{BinaryOperator, CastKind, Expr, UnaryOperator, Value};

use crate::{Error, Simulator, ty::SqlType};

pub trait ColumnInferrer {
    fn infer_unqualified_type(
        &self,
        sim: &mut Simulator,
        column: &str,
    ) -> Result<Option<SqlType>, Error>;

    fn infer_qualified_type(
        &self,
        sim: &mut Simulator,
        qualifier: &str,
        column: &str,
    ) -> Result<SqlType, Error>;
}

pub fn infer_expr_type<I: ColumnInferrer>(
    expr: &Expr,
    sim: &mut Simulator,
    expected: Option<SqlType>,
    inferrer: &I,
) -> Result<SqlType, Error> {
    match expr {
        Expr::Value(val) => match val.value.clone() {
            Value::Number(str, _) => {
                // Initially, try to use the expected type.
                if let Some(expected_ty) = expected {
                    match expected_ty {
                        SqlType::SmallInt => {
                            if str.parse::<i16>().is_ok() {
                                return Ok(SqlType::SmallInt);
                            }
                        }
                        SqlType::Integer => {
                            if str.parse::<i32>().is_ok() {
                                return Ok(SqlType::Integer);
                            }
                        }
                        SqlType::BigInt => {
                            if str.parse::<i64>().is_ok() {
                                return Ok(SqlType::BigInt);
                            }
                        }
                        SqlType::Float => {
                            if str.parse::<f32>().is_ok() {
                                return Ok(SqlType::Float);
                            }
                        }
                        SqlType::Double => {
                            if str.parse::<f64>().is_ok() {
                                return Ok(SqlType::Float);
                            }
                        }
                        _ => {}
                    }
                }

                // Fallback to smallest type to biggest.
                if str.parse::<i16>().is_ok() {
                    Ok(SqlType::SmallInt)
                } else if str.parse::<i32>().is_ok() {
                    Ok(SqlType::Integer)
                } else if str.parse::<i64>().is_ok() {
                    Ok(SqlType::BigInt)
                } else if str.contains('.') || str.to_lowercase().contains('e') {
                    if str.parse::<f32>().is_ok() {
                        Ok(SqlType::Float)
                    } else if str.parse::<f64>().is_ok() {
                        Ok(SqlType::Double)
                    } else {
                        Err(Error::Sql("Invalid floating point number".to_string()))
                    }
                } else {
                    // Integer that's too large for i64
                    Err(Error::Sql("Number is too big".to_string()))
                }
            }
            Value::SingleQuotedString(_)
            | Value::DollarQuotedString(_)
            | Value::SingleQuotedByteStringLiteral(_)
            | Value::DoubleQuotedByteStringLiteral(_)
            | Value::NationalStringLiteral(_)
            | Value::HexStringLiteral(_)
            | Value::DoubleQuotedString(_) => Ok(SqlType::Text),
            Value::Boolean(_) => Ok(SqlType::Boolean),
            Value::Null => Ok(SqlType::Null),
            // Placeholder just takes the type of the expected.
            Value::Placeholder(_) => expected.ok_or(Error::Unsupported(
                "Cannot infer type of the placeholder".to_string(),
            )),
            _ => todo!(),
        },
        Expr::IsTrue(expr)
        | Expr::IsNotTrue(expr)
        | Expr::IsFalse(expr)
        | Expr::IsNotFalse(expr)
        | Expr::IsUnknown(expr)
        | Expr::IsNotUnknown(expr) => {
            let ty = infer_expr_type(expr, sim, Some(SqlType::Boolean), inferrer)?;
            if ty != SqlType::Boolean {
                return Err(Error::TypeMismatch {
                    expected: SqlType::Boolean,
                    got: ty,
                });
            }

            Ok(SqlType::Boolean)
        }

        Expr::IsNull(expr) | Expr::IsNotNull(expr) => {
            _ = infer_expr_type(expr, sim, expected, inferrer)?;
            Ok(SqlType::Boolean)
        }
        Expr::Identifier(ident) => {
            let name = &ident.value;

            let ty = inferrer
                .infer_unqualified_type(sim, name)?
                .ok_or(Error::ColumnDoesntExist(name.to_string()))?;

            Ok(ty)
        }
        Expr::CompoundIdentifier(idents) => {
            // validate that identifier is a column.
            let table_or_alias = &idents.first().unwrap().value;
            let column_name = &idents.get(1).unwrap().value;

            Ok(inferrer.infer_qualified_type(sim, &table_or_alias, &column_name)?)
        }
        Expr::BinaryOp { left, right, op } => {
            let left_ty = infer_expr_type(left, sim, expected, inferrer)?;
            let right_ty = infer_expr_type(right, sim, Some(left_ty.clone()), inferrer)?;

            match op {
                BinaryOperator::Plus
                | BinaryOperator::Minus
                | BinaryOperator::Multiply
                | BinaryOperator::Divide
                | BinaryOperator::Modulo => {
                    if left_ty != right_ty {
                        return Err(Error::TypeMismatch {
                            expected: left_ty,
                            got: right_ty,
                        });
                    }

                    Ok(left_ty)
                }
                BinaryOperator::Gt
                | BinaryOperator::Lt
                | BinaryOperator::GtEq
                | BinaryOperator::LtEq => {
                    if left_ty != right_ty {
                        return Err(Error::TypeMismatch {
                            expected: left_ty,
                            got: right_ty,
                        });
                    }

                    Ok(SqlType::Boolean)
                }

                BinaryOperator::StringConcat => todo!(),
                BinaryOperator::Spaceship => todo!(),
                BinaryOperator::Eq | BinaryOperator::NotEq => {
                    if left_ty != right_ty {
                        return Err(Error::TypeMismatch {
                            expected: left_ty,
                            got: right_ty,
                        });
                    }

                    Ok(SqlType::Boolean)
                }
                BinaryOperator::And | BinaryOperator::Or | BinaryOperator::Xor => {
                    if left_ty != SqlType::Boolean {
                        return Err(Error::TypeMismatch {
                            expected: SqlType::Boolean,
                            got: left_ty,
                        });
                    }
                    if right_ty != SqlType::Boolean {
                        return Err(Error::TypeMismatch {
                            expected: SqlType::Boolean,
                            got: right_ty,
                        });
                    }

                    Ok(SqlType::Boolean)
                }
                BinaryOperator::BitwiseOr
                | BinaryOperator::BitwiseAnd
                | BinaryOperator::BitwiseXor => {
                    if !matches!(
                        left_ty,
                        SqlType::SmallInt | SqlType::Integer | SqlType::BigInt
                    ) {
                        return Err(Error::TypeMismatch {
                            expected: SqlType::Integer,
                            got: left_ty,
                        });
                    }

                    if left_ty != right_ty {
                        return Err(Error::TypeMismatch {
                            expected: left_ty,
                            got: right_ty,
                        });
                    }

                    Ok(left_ty)
                }
                _ => {
                    todo!()
                }
            }
        }
        Expr::UnaryOp { expr, op } => {
            let ty = infer_expr_type(expr, sim, expected, inferrer)?;

            match op {
                UnaryOperator::Plus | UnaryOperator::Minus => {
                    if !ty.is_numeric() {
                        Err(Error::TypeNotNumeric(ty))
                    } else {
                        Ok(ty)
                    }
                }
                UnaryOperator::Not => {
                    if ty != SqlType::Boolean {
                        Err(Error::TypeMismatch {
                            expected: SqlType::Boolean,
                            got: ty,
                        })
                    } else {
                        Ok(SqlType::Boolean)
                    }
                }
                _ => todo!(),
            }
        }
        Expr::Nested(expr) => infer_expr_type(expr, sim, expected, inferrer),
        Expr::InList { expr, list, .. } => {
            let ty = infer_expr_type(expr, sim, expected, inferrer)?;
            for item in list {
                let item_ty = infer_expr_type(item, sim, Some(ty.clone()), inferrer)?;
                if ty != item_ty {
                    return Err(Error::TypeMismatch {
                        expected: ty,
                        got: item_ty,
                    });
                }
            }

            Ok(SqlType::Boolean)
        }
        Expr::Cast {
            kind,
            expr,
            data_type,
            ..
        } => {
            let ty: SqlType = data_type.clone().into();
            match kind {
                CastKind::Cast | CastKind::DoubleColon => {
                    let _inner_ty = infer_expr_type(expr, sim, Some(ty.clone()), inferrer)?;
                    // TODO: Ensure the two types are castable.

                    Ok(ty)
                }
                _ => todo!(),
            }
        }
        _ => Err(Error::Unsupported(format!(
            "Unsupported WHERE expr: {expr:#?}"
        ))),
    }
}
