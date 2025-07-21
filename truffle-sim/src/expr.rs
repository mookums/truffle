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

impl Simulator {
    pub(crate) fn infer_expr_type<I: ColumnInferrer>(
        &self,
        expr: &Expr,
        expected: Option<SqlType>,
        inferrer: &I,
    ) -> Result<SqlType, Error> {
        match expr {
            Expr::Value(val) => Self::infer_value_type(&val.value, expected),
            Expr::IsTrue(expr)
            | Expr::IsNotTrue(expr)
            | Expr::IsFalse(expr)
            | Expr::IsNotFalse(expr)
            | Expr::IsUnknown(expr)
            | Expr::IsNotUnknown(expr) => {
                let ty = self.infer_expr_type(expr, Some(SqlType::Boolean), inferrer)?;
                if ty != SqlType::Boolean {
                    return Err(Error::TypeMismatch {
                        expected: SqlType::Boolean,
                        got: ty,
                    });
                }

                Ok(SqlType::Boolean)
            }

            Expr::IsNull(expr) | Expr::IsNotNull(expr) => {
                _ = self.infer_expr_type(expr, expected, inferrer)?;
                Ok(SqlType::Boolean)
            }
            Expr::Identifier(ident) => {
                let name = &ident.value;

                Ok(inferrer
                    .infer_unqualified_type(self, name)?
                    .ok_or_else(|| Error::ColumnDoesntExist(name.to_string()))?)
            }
            Expr::CompoundIdentifier(idents) => {
                // validate that identifier is a column.
                let table_or_alias = &idents.first().unwrap().value;
                let column_name = &idents.get(1).unwrap().value;

                Ok(inferrer.infer_qualified_type(self, table_or_alias, column_name)?)
            }
            Expr::BinaryOp { left, right, op } => {
                self.infer_binary_op_type(left, right, op, expected, inferrer)
            }
            Expr::UnaryOp { expr, op } => self.infer_unary_op_type(expr, op, expected, inferrer),
            Expr::Nested(expr) => self.infer_expr_type(expr, expected, inferrer),
            Expr::InList { expr, list, .. } => {
                let ty = self.infer_expr_type(expr, expected, inferrer)?;
                for item in list {
                    let item_ty = self.infer_expr_type(item, Some(ty.clone()), inferrer)?;
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
                        let _inner_ty = self.infer_expr_type(expr, Some(ty.clone()), inferrer)?;
                        // TODO: Ensure the two types are castable.

                        Ok(ty)
                    }
                    _ => todo!(),
                }
            }
            Expr::Subquery(_) => {
                todo!()
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
            Value::Placeholder(_) => expected.ok_or_else(|| {
                Error::Unsupported("Cannot infer type of the placeholder".to_string())
            }),
            _ => Err(Error::Unsupported(format!("Unsupported value: {value:?}"))),
        }
    }

    fn infer_binary_op_type<I: ColumnInferrer>(
        &self,
        left: &Expr,
        right: &Expr,
        op: &BinaryOperator,
        expected: Option<SqlType>,
        inferrer: &I,
    ) -> Result<SqlType, Error> {
        let left_ty = self.infer_expr_type(left, expected, inferrer)?;
        let right_ty = self.infer_expr_type(right, Some(left_ty.clone()), inferrer)?;

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
        &self,
        expr: &Expr,
        op: &UnaryOperator,
        expected: Option<SqlType>,
        inferrer: &I,
    ) -> Result<SqlType, Error> {
        let ty = self.infer_expr_type(expr, expected, inferrer)?;

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
}
