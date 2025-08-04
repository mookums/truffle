use sqlparser::ast::{BinaryOperator, CastKind, Expr, UnaryOperator, Value};

#[cfg(feature = "time")]
use time::{
    Date, OffsetDateTime, PrimitiveDateTime, Time,
    format_description::{
        self,
        well_known::{Iso8601, Rfc3339},
    },
};

use crate::{Error, Simulator, resolve::ResolvedQuery, ty::SqlType};

#[derive(Debug, Clone)]
pub enum InferType {
    Required(SqlType),
    // Hint(SqlType),
    Unknown,
}

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
        expected: InferType,
        inferrer: &I,
        resolved: &mut ResolvedQuery,
    ) -> Result<SqlType, Error> {
        let expect = expected.clone();

        let ty: SqlType = match expr {
            Expr::Value(val) => Self::infer_value_type(&val.value, expected, resolved)?,
            Expr::IsTrue(expr)
            | Expr::IsNotTrue(expr)
            | Expr::IsFalse(expr)
            | Expr::IsNotFalse(expr) => {
                self.infer_expr_type(
                    expr,
                    InferType::Required(SqlType::Boolean),
                    inferrer,
                    resolved,
                )?;
                SqlType::Boolean
            }
            Expr::IsUnknown(expr) | Expr::IsNotUnknown(expr) => {
                self.infer_expr_type(expr, InferType::Unknown, inferrer, resolved)?;
                SqlType::Boolean
            }

            Expr::IsNull(expr) | Expr::IsNotNull(expr) => {
                self.infer_expr_type(expr, InferType::Unknown, inferrer, resolved)?;
                SqlType::Boolean
            }
            Expr::Identifier(ident) => {
                let name = &ident.value;

                inferrer
                    .infer_unqualified_type(self, name)?
                    .ok_or_else(|| Error::ColumnDoesntExist(name.to_string()))?
            }
            Expr::CompoundIdentifier(idents) => {
                // validate that identifier is a column.
                let qualifier = &idents.first().unwrap().value;
                let column_name = &idents.get(1).unwrap().value;

                inferrer.infer_qualified_type(self, qualifier, column_name)?
            }
            Expr::BinaryOp { left, right, op } => {
                self.infer_binary_op_type([left, right], op, expected, inferrer, resolved)?
            }
            Expr::UnaryOp { expr, op } => {
                self.infer_unary_op_type(expr, op, expected, inferrer, resolved)?
            }
            Expr::Nested(expr) => self.infer_expr_type(expr, expected, inferrer, resolved)?,
            Expr::InList { expr, list, .. } => {
                let ty = self.infer_expr_type(expr, InferType::Unknown, inferrer, resolved)?;
                for item in list {
                    assert_eq!(
                        self.infer_expr_type(
                            item,
                            InferType::Required(ty.clone()),
                            inferrer,
                            resolved,
                        )?,
                        ty
                    );
                }

                SqlType::Boolean
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
                        // TODO: Ensure the two types are castable.
                        let _inner_ty =
                            self.infer_expr_type(expr, InferType::Unknown, inferrer, resolved)?;

                        ty
                    }
                    _ => todo!(),
                }
            }
            Expr::Tuple(exprs) => match expected {
                InferType::Required(SqlType::Tuple(tys)) => {
                    if exprs.len() != tys.len() {
                        return Err(Error::ColumnCountMismatch {
                            expected: tys.len(),
                            got: exprs.len(),
                        });
                    }

                    let inner_tuple_tys: Result<Vec<SqlType>, Error> = exprs
                        .iter()
                        .zip(tys)
                        .map(|(e, ty)| {
                            self.infer_expr_type(e, InferType::Required(ty), inferrer, resolved)
                        })
                        .collect();

                    SqlType::Tuple(inner_tuple_tys?)
                }
                _ => SqlType::Tuple(
                    exprs
                        .iter()
                        .map(|e| {
                            self.infer_expr_type(e, InferType::Unknown, inferrer, resolved)
                                .unwrap()
                        })
                        .collect(),
                ),
            },
            Expr::Subquery(_) => {
                todo!()
            }
            _ => return Err(Error::Unsupported(format!("Unsupported Expr: {expr:#?}"))),
        };

        // Check the type here.
        if let InferType::Required(expected_ty) = expect {
            if expected_ty != ty {
                return Err(Error::TypeMismatch {
                    expected: expected_ty,
                    got: ty,
                });
            }
        }

        Ok(ty)
    }

    fn infer_value_type(
        value: &Value,
        expected: InferType,
        resolved: &mut ResolvedQuery,
    ) -> Result<SqlType, Error> {
        match value {
            Value::Number(str, _) => {
                // Initially, try to use the expected type.
                if let InferType::Required(expected_ty) = expected {
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
                };

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

            #[allow(unused_variables)]
            Value::SingleQuotedString(str)
            | Value::SingleQuotedByteStringLiteral(str)
            | Value::DoubleQuotedByteStringLiteral(str)
            | Value::NationalStringLiteral(str)
            | Value::HexStringLiteral(str)
            | Value::DoubleQuotedString(str) => {
                if let InferType::Required(expected_ty) = expected {
                    match expected_ty {
                        #[cfg(feature = "time")]
                        SqlType::Timestamp => {
                            let format = format_description::parse(
                                "[year]-[month]-[day] [hour]:[minute]:[second]",
                            )
                            .unwrap();
                            if PrimitiveDateTime::parse(str, &format).is_ok() {
                                return Ok(SqlType::Timestamp);
                            }
                        }
                        #[cfg(feature = "time")]
                        SqlType::TimestampTz => {
                            if OffsetDateTime::parse(str, &Iso8601::DEFAULT).is_ok() {
                                return Ok(SqlType::TimestampTz);
                            }

                            if OffsetDateTime::parse(str, &Rfc3339).is_ok() {
                                return Ok(SqlType::TimestampTz);
                            }
                        }
                        #[cfg(feature = "time")]
                        SqlType::Time => {
                            if Time::parse(str, &Iso8601::DEFAULT).is_ok() {
                                return Ok(SqlType::Time);
                            }
                        }
                        #[cfg(feature = "time")]
                        SqlType::Date => {
                            if Date::parse(str, &Iso8601::DEFAULT).is_ok() {
                                return Ok(SqlType::Date);
                            }
                        }
                        #[cfg(feature = "uuid")]
                        SqlType::Uuid => {
                            if uuid::Uuid::parse_str(str).is_ok() {
                                return Ok(SqlType::Uuid);
                            }
                        }
                        #[cfg(feature = "json")]
                        SqlType::Json => {
                            if serde_json::from_str::<serde::de::IgnoredAny>(str).is_ok() {
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
            Value::Placeholder(placeholder) => match expected {
                InferType::Required(ty) => {
                    resolved.insert_input(placeholder, ty.clone());
                    Ok(ty)
                }
                _ => Err(Error::Unsupported(
                    "Cannot infer type of the placeholder".to_string(),
                )),
            },
            _ => Err(Error::Unsupported(format!("Unsupported value: {value:?}"))),
        }
    }

    fn infer_binary_op_type<I: ColumnInferrer>(
        &self,
        exprs: [&Expr; 2],
        op: &BinaryOperator,
        expected: InferType,
        inferrer: &I,
        resolved: &mut ResolvedQuery,
    ) -> Result<SqlType, Error> {
        let [left, right] = exprs;
        match op {
            BinaryOperator::Plus
            | BinaryOperator::Minus
            | BinaryOperator::Multiply
            | BinaryOperator::Divide
            | BinaryOperator::Modulo => {
                let left_ty = self.infer_expr_type(left, expected, inferrer, resolved)?;
                let right_ty = self.infer_expr_type(
                    right,
                    InferType::Required(left_ty.clone()),
                    inferrer,
                    resolved,
                )?;

                assert_eq!(left_ty, right_ty);
                Ok(left_ty)
            }
            BinaryOperator::Gt
            | BinaryOperator::Lt
            | BinaryOperator::GtEq
            | BinaryOperator::LtEq
            | BinaryOperator::Eq
            | BinaryOperator::NotEq => {
                let left_ty = self.infer_expr_type(left, InferType::Unknown, inferrer, resolved)?;
                let right_ty = self.infer_expr_type(
                    right,
                    InferType::Required(left_ty.clone()),
                    inferrer,
                    resolved,
                )?;

                if left_ty != right_ty {
                    return Err(Error::TypeMismatch {
                        expected: left_ty,
                        got: right_ty,
                    });
                }

                Ok(SqlType::Boolean)
            }
            BinaryOperator::And | BinaryOperator::Or | BinaryOperator::Xor => {
                let left_ty = self.infer_expr_type(
                    left,
                    InferType::Required(SqlType::Boolean),
                    inferrer,
                    resolved,
                )?;
                let right_ty = self.infer_expr_type(
                    right,
                    InferType::Required(left_ty.clone()),
                    inferrer,
                    resolved,
                )?;

                assert_eq!(left_ty, right_ty);
                Ok(SqlType::Boolean)
            }
            BinaryOperator::BitwiseOr | BinaryOperator::BitwiseAnd | BinaryOperator::BitwiseXor => {
                let left_ty = self.infer_expr_type(left, expected, inferrer, resolved)?;
                let right_ty = self.infer_expr_type(
                    right,
                    InferType::Required(left_ty.clone()),
                    inferrer,
                    resolved,
                )?;

                if !left_ty.is_integer() {
                    return Err(Error::TypeMismatch {
                        expected: SqlType::Integer,
                        got: left_ty,
                    });
                }

                assert_eq!(left_ty, right_ty);
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
        expected: InferType,
        inferrer: &I,
        resolved: &mut ResolvedQuery,
    ) -> Result<SqlType, Error> {
        match op {
            UnaryOperator::Plus | UnaryOperator::Minus => {
                let ty = self.infer_expr_type(expr, expected, inferrer, resolved)?;
                if !ty.is_numeric() {
                    Err(Error::TypeNotNumeric(ty))
                } else {
                    Ok(ty)
                }
            }
            UnaryOperator::Not => {
                assert_eq!(
                    self.infer_expr_type(
                        expr,
                        InferType::Required(SqlType::Boolean),
                        inferrer,
                        resolved,
                    )?,
                    SqlType::Boolean
                );
                Ok(SqlType::Boolean)
            }
            _ => Err(Error::Unsupported(format!(
                "Unsupported unary operator: {op:?}"
            ))),
        }
    }
}
