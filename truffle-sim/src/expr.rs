use serde::de::IgnoredAny;
use sqlparser::ast::{BinaryOperator, CastKind, Expr, UnaryOperator, Value};
use time::{
    Date, OffsetDateTime, PrimitiveDateTime, Time,
    format_description::{
        self,
        well_known::{Iso8601, Rfc3339},
    },
};
use uuid::Uuid;

use crate::{Error, Simulator, ty::SqlType};

pub trait ColumnInferrer {
    fn infer_unqualified_type(
        &self,
        sim: &Simulator,
        column: &str,
    ) -> Result<Option<SqlType>, Error>;

    fn infer_qualified_type(
        &self,
        sim: &Simulator,
        qualifier: &str,
        column: &str,
    ) -> Result<SqlType, Error>;
}

pub fn infer_expr_type<I: ColumnInferrer>(
    expr: &Expr,
    sim: &Simulator,
    expected: Option<SqlType>,
    inferrer: &I,
) -> Result<SqlType, Error> {
    match expr {
        Expr::Value(val) => infer_value_type(&val.value, expected),
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

            Ok(inferrer
                .infer_unqualified_type(sim, name)?
                .ok_or_else(|| Error::ColumnDoesntExist(name.to_string()))?)
        }
        Expr::CompoundIdentifier(idents) => {
            // validate that identifier is a column.
            let table_or_alias = &idents.first().unwrap().value;
            let column_name = &idents.get(1).unwrap().value;

            Ok(inferrer.infer_qualified_type(sim, table_or_alias, column_name)?)
        }
        Expr::BinaryOp { left, right, op } => {
            infer_binary_op_type(left, right, op, sim, expected, inferrer)
        }
        Expr::UnaryOp { expr, op } => infer_unary_op_type(expr, op, sim, expected, inferrer),
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

fn infer_value_type(value: &Value, expected: Option<SqlType>) -> Result<SqlType, Error> {
    match value {
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
                            return Ok(SqlType::Double);
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
                Err(Error::Sql("Number is too big".to_string()))
            }
        }
        Value::SingleQuotedString(str)
        | Value::SingleQuotedByteStringLiteral(str)
        | Value::DoubleQuotedByteStringLiteral(str)
        | Value::NationalStringLiteral(str)
        | Value::HexStringLiteral(str)
        | Value::DoubleQuotedString(str) => {
            if let Some(expected_ty) = expected {
                match expected_ty {
                    SqlType::Timestamp => {
                        let format = format_description::parse(
                            "[year]-[month]-[day] [hour]:[minute]:[second]",
                        )
                        .unwrap();
                        if PrimitiveDateTime::parse(str, &format).is_ok() {
                            return Ok(SqlType::Timestamp);
                        }
                    }
                    SqlType::TimestampTz => {
                        if OffsetDateTime::parse(str, &Iso8601::DEFAULT).is_ok() {
                            return Ok(SqlType::TimestampTz);
                        }

                        if OffsetDateTime::parse(str, &Rfc3339).is_ok() {
                            return Ok(SqlType::TimestampTz);
                        }
                    }
                    SqlType::Time => {
                        if Time::parse(str, &Iso8601::DEFAULT).is_ok() {
                            return Ok(SqlType::Time);
                        }
                    }
                    SqlType::Date => {
                        if Date::parse(str, &Iso8601::DEFAULT).is_ok() {
                            return Ok(SqlType::Date);
                        }
                    }
                    SqlType::Uuid => {
                        if Uuid::parse_str(str).is_ok() {
                            return Ok(SqlType::Uuid);
                        }
                    }
                    SqlType::Json => {
                        if serde_json::from_str::<IgnoredAny>(str).is_ok() {
                            return Ok(SqlType::Json);
                        }
                    }
                    _ => {}
                }
            }

            Ok(SqlType::Text)
        }
        Value::Boolean(_) => Ok(SqlType::Boolean),
        Value::Null => Ok(SqlType::Null),
        Value::Placeholder(_) => expected
            .ok_or_else(|| Error::Unsupported("Cannot infer type of the placeholder".to_string())),
        _ => Err(Error::Unsupported(format!("Unsupported value: {value:?}"))),
    }
}

fn infer_binary_op_type<I: ColumnInferrer>(
    left: &Expr,
    right: &Expr,
    op: &BinaryOperator,
    sim: &Simulator,
    expected: Option<SqlType>,
    inferrer: &I,
) -> Result<SqlType, Error> {
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
        | BinaryOperator::LtEq
        | BinaryOperator::Eq
        | BinaryOperator::NotEq => {
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
        BinaryOperator::BitwiseOr | BinaryOperator::BitwiseAnd | BinaryOperator::BitwiseXor => {
            if left_ty.is_integer() {
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
        _ => Err(Error::Unsupported(format!(
            "Unsupported binary operator: {op:?}"
        ))),
    }
}

fn infer_unary_op_type<I: ColumnInferrer>(
    expr: &Expr,
    op: &UnaryOperator,
    sim: &Simulator,
    expected: Option<SqlType>,
    inferrer: &I,
) -> Result<SqlType, Error> {
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
        _ => Err(Error::Unsupported(format!(
            "Unsupported unary operator: {op:?}"
        ))),
    }
}
