use sqlparser::ast::{BinaryOperator, CastKind, Expr, UnaryOperator, Value};

#[cfg(feature = "time")]
use time::{
    Date, OffsetDateTime, PrimitiveDateTime, Time,
    format_description::{
        self,
        well_known::{Iso8601, Rfc3339},
    },
};

use crate::{Error, Simulator, column::Column, resolve::ResolvedQuery, ty::SqlType};

#[derive(Debug, Clone, Default)]
pub struct InferContext {
    ty: Option<SqlType>,
    nullable: Option<bool>,
    default: Option<bool>,
}

impl InferContext {
    pub fn with_type(self, ty: SqlType) -> Self {
        Self {
            ty: Some(ty),
            nullable: self.nullable,
            default: self.default,
        }
    }

    pub fn with_nullable(self, nullable: bool) -> Self {
        Self {
            ty: self.ty,
            nullable: Some(nullable),
            default: self.default,
        }
    }
}

pub trait ColumnInferrer {
    fn infer_unqualified_column(
        &self,
        sim: &Simulator,
        column: &str,
    ) -> Result<Option<Column>, Error>;

    fn infer_qualified_column(
        &self,
        sim: &Simulator,
        qualifier: &str,
        column: &str,
    ) -> Result<Column, Error>;
}

impl Simulator {
    pub(crate) fn infer_expr_column<I: ColumnInferrer>(
        &self,
        expr: &Expr,
        context: InferContext,
        inferrer: &I,
        resolved: &mut ResolvedQuery,
    ) -> Result<Column, Error> {
        let expect = context.clone();

        let col: Column = match expr {
            Expr::Value(val) => Self::infer_value_column(&val.value, context, resolved)?,
            Expr::IsTrue(expr) | Expr::IsFalse(expr) => {
                let col = self.infer_expr_column(
                    expr,
                    InferContext::default().with_type(SqlType::Boolean),
                    inferrer,
                    resolved,
                )?;

                Column::new(SqlType::Boolean, col.nullable, false)
            }
            Expr::IsNotTrue(expr)
            | Expr::IsNotFalse(expr)
            | Expr::IsUnknown(expr)
            | Expr::IsNotUnknown(expr)
            | Expr::IsNull(expr)
            | Expr::IsNotNull(expr) => {
                self.infer_expr_column(expr, InferContext::default(), inferrer, resolved)?;
                Column::new(SqlType::Boolean, false, false)
            }
            Expr::IsDistinctFrom(left, right) | Expr::IsNotDistinctFrom(left, right) => {
                let left_col =
                    self.infer_expr_column(left, InferContext::default(), inferrer, resolved)?;
                self.infer_expr_column(
                    right,
                    InferContext::default().with_type(left_col.ty),
                    inferrer,
                    resolved,
                )?;

                Column::new(SqlType::Boolean, false, false)
            }
            Expr::IsNormalized { expr, .. } => {
                let col = self.infer_expr_column(
                    expr,
                    InferContext::default().with_type(SqlType::Text),
                    inferrer,
                    resolved,
                )?;

                Column::new(SqlType::Boolean, col.nullable, false)
            }
            Expr::Like { expr, .. } | Expr::ILike { expr, .. } => {
                let col = self.infer_expr_column(
                    expr,
                    InferContext::default().with_type(SqlType::Text),
                    inferrer,
                    resolved,
                )?;

                Column::new(SqlType::Boolean, col.nullable, false)
            }
            Expr::Substring {
                expr,
                substring_from,
                substring_for,
                ..
            } => {
                let str_col = self.infer_expr_column(
                    expr,
                    InferContext::default().with_type(SqlType::Text),
                    inferrer,
                    resolved,
                )?;

                // Ensure that the from is an integer.
                if let Some(from_expr) = substring_from {
                    let from_col = self.infer_expr_column(
                        from_expr,
                        InferContext::default(),
                        inferrer,
                        resolved,
                    )?;
                    if !from_col.ty.is_integer() {
                        return Err(Error::TypeNotNumeric(from_col.ty));
                    }
                }

                // Ensure that the for is an integer.
                if let Some(for_expr) = substring_for {
                    let for_col = self.infer_expr_column(
                        for_expr,
                        InferContext::default(),
                        inferrer,
                        resolved,
                    )?;
                    if !for_col.ty.is_integer() {
                        return Err(Error::TypeNotNumeric(for_col.ty));
                    }
                }

                Column::new(SqlType::Text, str_col.nullable, false)
            }
            Expr::Identifier(ident) => {
                let name = &ident.value;

                inferrer
                    .infer_unqualified_column(self, name)?
                    .ok_or_else(|| Error::ColumnDoesntExist(name.to_string()))?
            }
            Expr::CompoundIdentifier(idents) => {
                // validate that identifier is a column.
                let qualifier = &idents.first().unwrap().value;
                let column_name = &idents.get(1).unwrap().value;

                inferrer.infer_qualified_column(self, qualifier, column_name)?
            }
            Expr::BinaryOp { left, right, op } => {
                self.infer_binary_op_column([left, right], op, context, inferrer, resolved)?
            }
            Expr::UnaryOp { expr, op } => {
                self.infer_unary_op_column(expr, op, context, inferrer, resolved)?
            }
            Expr::Nested(expr) => self.infer_expr_column(expr, context, inferrer, resolved)?,
            Expr::InList { expr, list, .. } => {
                let mut nullable = false;
                let col =
                    self.infer_expr_column(expr, InferContext::default(), inferrer, resolved)?;

                for item in list {
                    let inner_col = self.infer_expr_column(
                        item,
                        InferContext::default().with_type(col.ty.clone()),
                        inferrer,
                        resolved,
                    )?;

                    nullable |= inner_col.nullable;
                }

                Column::new(col.ty, nullable, false)
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
                        let inner_col = self.infer_expr_column(
                            expr,
                            InferContext::default(),
                            inferrer,
                            resolved,
                        )?;

                        Column::new(ty, inner_col.nullable, inner_col.default)
                    }
                    _ => todo!(),
                }
            }
            Expr::Tuple(exprs) => match context.ty {
                Some(SqlType::Tuple(cols)) => {
                    if exprs.len() != cols.len() {
                        return Err(Error::ColumnCountMismatch {
                            expected: cols.len(),
                            got: exprs.len(),
                        });
                    }

                    let inner_tuple_cols: Result<Vec<Column>, Error> = exprs
                        .iter()
                        .zip(cols)
                        .map(|(e, col)| {
                            self.infer_expr_column(
                                e,
                                InferContext::default().with_type(col.ty.clone()),
                                inferrer,
                                resolved,
                            )
                        })
                        .collect();

                    Column::new(
                        SqlType::Tuple(inner_tuple_cols?),
                        context.nullable.unwrap_or(false),
                        context.default.unwrap_or(false),
                    )
                }
                _ => {
                    let ty = SqlType::Tuple(
                        exprs
                            .iter()
                            .map(|e| {
                                self.infer_expr_column(
                                    e,
                                    InferContext::default(),
                                    inferrer,
                                    resolved,
                                )
                                .unwrap()
                            })
                            .collect(),
                    );

                    Column::new(ty, false, false)
                }
            },
            Expr::Function(func) => {
                self.infer_function_column(func, context, inferrer, resolved)?
            }
            Expr::Subquery(_) => {
                todo!()
            }
            Expr::Between {
                expr, low, high, ..
            } => {
                let value =
                    self.infer_expr_column(expr, InferContext::default(), inferrer, resolved)?;

                let low_col = self.infer_expr_column(
                    low,
                    InferContext::default().with_type(value.ty.clone()),
                    inferrer,
                    resolved,
                )?;

                let high_col = self.infer_expr_column(
                    high,
                    InferContext::default().with_type(value.ty.clone()),
                    inferrer,
                    resolved,
                )?;

                assert_eq!(value.ty, low_col.ty);
                assert_eq!(value.ty, high_col.ty);

                // TODO: Only allow integers, text and dates.

                Column::new(SqlType::Boolean, false, false)
            }
            Expr::Case {
                operand,
                conditions,
                else_result,
                ..
            } => {
                let operand_col = operand
                    .as_ref()
                    .map(|o| self.infer_expr_column(o, InferContext::default(), inferrer, resolved))
                    .transpose()?;

                let mut result_ty: Option<SqlType> = None;
                let mut nullable = false;

                // Conditions list be empty.
                assert!(!conditions.is_empty());

                for condition in conditions {
                    let context = match &operand_col {
                        Some(col) => InferContext::default().with_type(col.ty.clone()),
                        None => InferContext::default().with_type(SqlType::Boolean),
                    };

                    // Validation Condition.
                    self.infer_expr_column(&condition.condition, context, inferrer, resolved)?;

                    // Validate Result, ensure that they are all the same type.
                    match result_ty {
                        Some(ref ty) => {
                            let val_col = self.infer_expr_column(
                                &condition.result,
                                InferContext::default().with_type(ty.clone()),
                                inferrer,
                                resolved,
                            )?;

                            nullable |= val_col.nullable;
                        }
                        None => {
                            let val_col = self.infer_expr_column(
                                &condition.result,
                                InferContext::default(),
                                inferrer,
                                resolved,
                            )?;

                            result_ty = Some(val_col.ty);
                            nullable |= val_col.nullable;
                        }
                    }
                }

                if let Some(else_result) = &else_result {
                    self.infer_expr_column(
                        else_result,
                        InferContext::default().with_type(result_ty.as_ref().unwrap().clone()),
                        inferrer,
                        resolved,
                    )?;
                }

                Column::new(result_ty.unwrap(), nullable, false)
            }
            _ => return Err(Error::Unsupported(format!("Unsupported Expr: {expr:#?}"))),
        };

        // Check the type here.
        if let Some(expected_ty) = expect.ty {
            if expected_ty != col.ty {
                return Err(Error::TypeMismatch {
                    expected: expected_ty,
                    got: col.ty,
                });
            }
        }

        Ok(col)
    }

    pub(crate) fn infer_value_column(
        value: &Value,
        context: InferContext,
        resolved: &mut ResolvedQuery,
    ) -> Result<Column, Error> {
        match value {
            Value::Number(str, _) => {
                // Initially, try to use the expected type.
                if let Some(expected_ty) = context.ty {
                    match expected_ty {
                        SqlType::SmallInt => {
                            if str.parse::<i16>().is_ok() {
                                return Ok(Column::new(SqlType::SmallInt, false, false));
                            }
                        }
                        SqlType::Integer => {
                            if str.parse::<i32>().is_ok() {
                                return Ok(Column::new(SqlType::Integer, false, false));
                            }
                        }
                        SqlType::BigInt => {
                            if str.parse::<i64>().is_ok() {
                                return Ok(Column::new(SqlType::BigInt, false, false));
                            }
                        }
                        SqlType::Float => {
                            if str.parse::<f32>().is_ok() {
                                return Ok(Column::new(SqlType::Float, false, false));
                            }
                        }
                        SqlType::Double => {
                            if str.parse::<f64>().is_ok() {
                                return Ok(Column::new(SqlType::Double, false, false));
                            }
                        }
                        _ => {}
                    }
                };

                // Fallback to smallest type to biggest.
                if str.parse::<i16>().is_ok() {
                    Ok(Column::new(SqlType::SmallInt, false, false))
                } else if str.parse::<i32>().is_ok() {
                    Ok(Column::new(SqlType::Integer, false, false))
                } else if str.parse::<i64>().is_ok() {
                    Ok(Column::new(SqlType::BigInt, false, false))
                } else if str.contains('.') || str.to_lowercase().contains('e') {
                    if str.parse::<f32>().is_ok() {
                        Ok(Column::new(SqlType::Float, false, false))
                    } else if str.parse::<f64>().is_ok() {
                        Ok(Column::new(SqlType::Double, false, false))
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
                if let Some(expected_ty) = context.ty {
                    match expected_ty {
                        #[cfg(feature = "time")]
                        SqlType::Timestamp => {
                            let format = format_description::parse(
                                "[year]-[month]-[day] [hour]:[minute]:[second]",
                            )
                            .unwrap();

                            if PrimitiveDateTime::parse(str, &format).is_ok() {
                                return Ok(Column::new(SqlType::Timestamp, false, false));
                            }
                        }
                        #[cfg(feature = "time")]
                        SqlType::TimestampTz => {
                            if OffsetDateTime::parse(str, &Iso8601::DEFAULT).is_ok() {
                                return Ok(Column::new(SqlType::TimestampTz, false, false));
                            }

                            if OffsetDateTime::parse(str, &Rfc3339).is_ok() {
                                return Ok(Column::new(SqlType::TimestampTz, false, false));
                            }
                        }
                        #[cfg(feature = "time")]
                        SqlType::Time => {
                            if Time::parse(str, &Iso8601::DEFAULT).is_ok() {
                                return Ok(Column::new(SqlType::Time, false, false));
                            }
                        }
                        #[cfg(feature = "time")]
                        SqlType::Date => {
                            if Date::parse(str, &Iso8601::DEFAULT).is_ok() {
                                return Ok(Column::new(SqlType::Date, false, false));
                            }
                        }
                        #[cfg(feature = "uuid")]
                        SqlType::Uuid => {
                            if uuid::Uuid::parse_str(str).is_ok() {
                                return Ok(Column::new(SqlType::Uuid, false, false));
                            }
                        }
                        #[cfg(feature = "json")]
                        SqlType::Json => {
                            if serde_json::from_str::<serde::de::IgnoredAny>(str).is_ok() {
                                return Ok(Column::new(SqlType::Json, false, false));
                            }
                        }
                        _ => {}
                    }
                }

                Ok(Column::new(SqlType::Text, false, false))
            }
            Value::Boolean(_) => Ok(Column::new(SqlType::Boolean, false, false)),
            Value::Null => {
                if let Some(ty) = context.ty {
                    // Can't assign null to non-nullable column.
                    if context.nullable.is_some_and(|n| !n) {
                        return Err(Error::NullOnNotNullColumn("".to_string()));
                    }

                    Ok(Column::new(ty, true, false))
                } else {
                    Err(Error::Unsupported(
                        "Cannot infer type of the NULL".to_string(),
                    ))
                }
            }
            Value::Placeholder(placeholder) => match context.ty {
                Some(ty) => {
                    let col = Column::new(
                        ty,
                        context.nullable.unwrap_or(false),
                        context.default.unwrap_or(false),
                    );
                    resolved.insert_input(placeholder, col.clone());

                    Ok(col)
                }
                None => Err(Error::Unsupported(
                    "Cannot infer type of the placeholder".to_string(),
                )),
            },
            _ => Err(Error::Unsupported(format!("Unsupported value: {value:?}"))),
        }
    }

    fn infer_binary_op_column<I: ColumnInferrer>(
        &self,
        exprs: [&Expr; 2],
        op: &BinaryOperator,
        context: InferContext,
        inferrer: &I,
        resolved: &mut ResolvedQuery,
    ) -> Result<Column, Error> {
        let [left, right] = exprs;
        match op {
            BinaryOperator::Plus
            | BinaryOperator::Minus
            | BinaryOperator::Multiply
            | BinaryOperator::Divide
            | BinaryOperator::Modulo => {
                let left_col = self.infer_expr_column(left, context, inferrer, resolved)?;
                let right_col = self.infer_expr_column(
                    right,
                    InferContext::default().with_type(left_col.ty.clone()),
                    inferrer,
                    resolved,
                )?;

                assert_eq!(left_col.ty, right_col.ty);

                let nullable = left_col.nullable | right_col.nullable;
                Ok(Column::new(left_col.ty, nullable, false))
            }
            BinaryOperator::Gt
            | BinaryOperator::Lt
            | BinaryOperator::GtEq
            | BinaryOperator::LtEq
            | BinaryOperator::Eq
            | BinaryOperator::NotEq => {
                let left_col =
                    self.infer_expr_column(left, InferContext::default(), inferrer, resolved)?;

                let right_col = self.infer_expr_column(
                    right,
                    InferContext::default()
                        .with_type(left_col.ty.clone())
                        .with_nullable(left_col.nullable),
                    inferrer,
                    resolved,
                )?;

                assert_eq!(left_col.ty, right_col.ty);

                // Resulting column is only nullable if either of the two are.
                let nullable = left_col.nullable | right_col.nullable;
                Ok(Column::new(SqlType::Boolean, nullable, false))
            }
            BinaryOperator::Spaceship => {
                let left_col =
                    self.infer_expr_column(left, InferContext::default(), inferrer, resolved)?;

                let right_col = self.infer_expr_column(
                    right,
                    InferContext::default()
                        .with_type(left_col.ty.clone())
                        .with_nullable(left_col.nullable),
                    inferrer,
                    resolved,
                )?;

                assert_eq!(left_col.ty, right_col.ty);

                // Spaceship operator collapses nullability.
                // Both NULL -> true
                // One NULL -> false
                // Both NOT NULL -> comparison
                Ok(Column::new(SqlType::Boolean, false, false))
            }
            BinaryOperator::And | BinaryOperator::Or | BinaryOperator::Xor => {
                let left_col =
                    self.infer_expr_column(left, InferContext::default(), inferrer, resolved)?;

                let right_col = self.infer_expr_column(
                    right,
                    InferContext::default().with_type(left_col.ty.clone()),
                    inferrer,
                    resolved,
                )?;

                assert_eq!(left_col.ty, right_col.ty);

                let nullable = left_col.nullable | right_col.nullable;
                Ok(Column::new(SqlType::Boolean, nullable, false))
            }
            BinaryOperator::BitwiseOr | BinaryOperator::BitwiseAnd | BinaryOperator::BitwiseXor => {
                let left_col = self.infer_expr_column(left, context, inferrer, resolved)?;

                if !left_col.ty.is_integer() {
                    return Err(Error::TypeMismatch {
                        expected: SqlType::Integer,
                        got: left_col.ty,
                    });
                }

                let right_col = self.infer_expr_column(
                    right,
                    InferContext::default()
                        .with_type(left_col.ty.clone())
                        .with_nullable(left_col.nullable),
                    inferrer,
                    resolved,
                )?;

                assert_eq!(left_col.ty, right_col.ty);

                let nullable = left_col.nullable | right_col.nullable;
                Ok(Column::new(left_col.ty, nullable, false))
            }
            BinaryOperator::StringConcat => {
                let left_col = self.infer_expr_column(
                    left,
                    InferContext::default().with_type(SqlType::Text),
                    inferrer,
                    resolved,
                )?;
                let right_col = self.infer_expr_column(
                    right,
                    InferContext::default().with_type(SqlType::Text),
                    inferrer,
                    resolved,
                )?;

                let nullable = left_col.nullable | right_col.nullable;

                Ok(Column::new(SqlType::Text, nullable, false))
            }
            _ => Err(Error::Unsupported(format!(
                "Unsupported binary operator: {op:?}"
            ))),
        }
    }

    fn infer_unary_op_column<I: ColumnInferrer>(
        &self,
        expr: &Expr,
        op: &UnaryOperator,
        _: InferContext,
        inferrer: &I,
        resolved: &mut ResolvedQuery,
    ) -> Result<Column, Error> {
        match op {
            UnaryOperator::Plus | UnaryOperator::Minus => {
                let col =
                    self.infer_expr_column(expr, InferContext::default(), inferrer, resolved)?;

                if !col.ty.is_numeric() {
                    Err(Error::TypeNotNumeric(col.ty))
                } else {
                    Ok(col)
                }
            }
            UnaryOperator::Not => {
                let col = self.infer_expr_column(
                    expr,
                    InferContext::default().with_type(SqlType::Boolean),
                    inferrer,
                    resolved,
                )?;

                assert_eq!(col.ty, SqlType::Boolean);
                Ok(col)
            }
            _ => Err(Error::Unsupported(format!(
                "Unsupported unary operator: {op:?}"
            ))),
        }
    }
}
