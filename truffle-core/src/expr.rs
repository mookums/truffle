use sqlparser::ast::{BinaryOperator, CastKind, Expr, UnaryOperator, Value};

#[cfg(feature = "time")]
use time::{
    Date, OffsetDateTime, PrimitiveDateTime, Time,
    format_description::{
        self,
        well_known::{Iso8601, Rfc3339},
    },
};

use crate::{
    Error, Simulator,
    column::Column,
    resolve::{ColumnRef, ResolvedQuery},
    ty::SqlType,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Scope {
    Row,
    Group,
    Literal,
}

impl Scope {
    pub fn combine(&self, other: &Scope) -> Result<Scope, Error> {
        match (self, other) {
            (Scope::Row, Scope::Row) => Ok(Scope::Row),
            (Scope::Group, Scope::Group) => Ok(Scope::Group),
            (Scope::Literal, other) | (other, Scope::Literal) => Ok(*other),
            _ => Err(Error::IncompatibleScope),
        }
    }
}

#[derive(Debug)]
pub struct InferredColumn {
    pub column: Column,
    pub scope: Scope,
}

#[derive(Debug, Clone, Default)]
pub struct InferConstraints {
    // If we are inferring that it has this type.
    pub ty: Option<SqlType>,
    // If we are inferring that is has this nullability.
    pub nullable: Option<bool>,
    // This is the inferred scope of the given Expr.
    pub scope: Option<Scope>,
}

#[derive(Debug, Clone, Default)]
pub struct InferHints {
    // Preferred default value availability (can be overridden).
    pub default: Option<bool>,
    // Scope that the expr can be coerced to.
    pub scope: Option<Scope>,
}

#[derive(Debug, Clone, Default)]
pub struct InferContext<'a> {
    pub constraints: InferConstraints,
    pub hints: InferHints,
    // This is a slice of all of the grouped Exprs.
    pub grouped: &'a [Expr],
}

impl<'a> InferContext<'a> {
    pub fn inherit_constraints_from_inferred_column(&mut self, inferred: &InferredColumn) {
        self.constraints = InferConstraints {
            ty: Some(inferred.column.ty.clone()),
            nullable: Some(inferred.column.nullable),
            scope: Some(inferred.scope),
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
    ) -> Result<InferredColumn, Error> {
        let mut ctx = context;

        // If this expression is grouped, set the constraint and inherited hint.
        if ctx.grouped.contains(expr) {
            ctx.constraints.scope = Some(Scope::Group);
            ctx.hints.scope = Some(Scope::Group);
        }

        let constraints = ctx.constraints.clone();

        let inferred: InferredColumn = match expr {
            Expr::Value(val) => Self::infer_value_column(&val.value, &ctx, resolved)?,
            Expr::IsTrue(expr) | Expr::IsFalse(expr) => {
                ctx.constraints.ty = Some(SqlType::Boolean);

                let infer = self.infer_expr_column(expr, ctx, inferrer, resolved)?;

                InferredColumn {
                    column: Column::new(SqlType::Boolean, infer.column.nullable, false),
                    scope: infer.scope,
                }
            }
            Expr::IsNotTrue(expr)
            | Expr::IsNotFalse(expr)
            | Expr::IsUnknown(expr)
            | Expr::IsNotUnknown(expr) => {
                ctx.constraints.ty = Some(SqlType::Boolean);

                let infer = self.infer_expr_column(expr, ctx, inferrer, resolved)?;

                InferredColumn {
                    column: Column::new(SqlType::Boolean, false, false),
                    scope: infer.scope,
                }
            }
            Expr::IsNull(expr) | Expr::IsNotNull(expr) => {
                ctx.constraints.ty = None;
                let infer = self.infer_expr_column(expr, ctx, inferrer, resolved)?;

                InferredColumn {
                    column: Column::new(SqlType::Boolean, false, false),
                    scope: infer.scope,
                }
            }
            Expr::IsNormalized { expr, .. } => {
                ctx.constraints.ty = Some(SqlType::Text);
                let infer = self.infer_expr_column(expr, ctx, inferrer, resolved)?;

                InferredColumn {
                    column: Column::new(SqlType::Boolean, infer.column.nullable, false),
                    scope: infer.scope,
                }
            }
            Expr::IsDistinctFrom(left, right) | Expr::IsNotDistinctFrom(left, right) => {
                ctx.constraints.ty = None;

                let mut right_ctx = ctx.clone();
                let left_infer = self.infer_expr_column(left, ctx, inferrer, resolved)?;
                right_ctx.constraints.ty = Some(left_infer.column.ty.clone());
                let right_infer = self.infer_expr_column(right, right_ctx, inferrer, resolved)?;

                assert_eq!(left_infer.column.ty, right_infer.column.ty);

                let scope = left_infer.scope.combine(&right_infer.scope)?;

                InferredColumn {
                    column: Column::new(SqlType::Boolean, false, false),
                    scope,
                }
            }
            Expr::Like { expr, .. } | Expr::ILike { expr, .. } => {
                ctx.constraints.ty = Some(SqlType::Text);

                let infer = self.infer_expr_column(expr, ctx, inferrer, resolved)?;

                InferredColumn {
                    column: Column::new(SqlType::Boolean, infer.column.nullable, false),
                    scope: infer.scope,
                }
            }
            Expr::Substring {
                expr,
                substring_from,
                substring_for,
                ..
            } => {
                ctx.constraints.ty = Some(SqlType::Text);
                // Substring is Row ONLY.
                ctx.constraints.scope = Some(Scope::Row);

                let mut from_ctx = ctx.clone();
                let mut for_ctx = ctx.clone();

                let str_infer = self.infer_expr_column(expr, ctx, inferrer, resolved)?;

                let mut scope = str_infer.scope;
                let mut nullable = str_infer.column.nullable;

                from_ctx.constraints.ty = None;
                from_ctx.constraints.nullable = Some(nullable);

                // Ensure that the from is an integer.
                if let Some(from_expr) = substring_from {
                    let from_infer =
                        self.infer_expr_column(from_expr, from_ctx, inferrer, resolved)?;

                    if !from_infer.column.ty.is_integer() {
                        return Err(Error::TypeNotNumeric(from_infer.column.ty));
                    }

                    scope = scope.combine(&from_infer.scope)?;
                    nullable |= from_infer.column.nullable;
                }

                for_ctx.constraints.ty = None;
                for_ctx.constraints.nullable = Some(nullable);

                // Ensure that the for is an integer.
                if let Some(for_expr) = substring_for {
                    let for_infer =
                        self.infer_expr_column(for_expr, for_ctx, inferrer, resolved)?;

                    if !for_infer.column.ty.is_integer() {
                        return Err(Error::TypeNotNumeric(for_infer.column.ty));
                    }

                    scope = scope.combine(&for_infer.scope)?;
                    nullable |= for_infer.column.nullable;
                }

                InferredColumn {
                    column: Column::new(SqlType::Text, nullable, false),
                    scope,
                }
            }
            Expr::Identifier(ident) => {
                let name = &ident.value;

                let column = inferrer
                    .infer_unqualified_column(self, name)?
                    .ok_or_else(|| Error::ColumnDoesntExist(name.to_string()))?;

                let scope = if ctx.hints.scope.is_some_and(|is| is == Scope::Group) {
                    Scope::Group
                } else {
                    Scope::Row
                };

                InferredColumn { column, scope }
            }
            Expr::CompoundIdentifier(idents) => {
                // validate that identifier is a column.
                let qualifier = &idents.first().unwrap().value;
                let column_name = &idents.get(1).unwrap().value;

                let column = inferrer.infer_qualified_column(self, qualifier, column_name)?;

                let scope = if ctx.hints.scope.is_some_and(|is| is == Scope::Group) {
                    Scope::Group
                } else {
                    Scope::Row
                };

                InferredColumn { column, scope }
            }
            Expr::BinaryOp { left, right, op } => {
                self.infer_binary_op_column([left, right], op, ctx, inferrer, resolved)?
            }
            Expr::UnaryOp { expr, op } => {
                self.infer_unary_op_column(expr, op, ctx, inferrer, resolved)?
            }
            Expr::Nested(expr) => self.infer_expr_column(expr, ctx, inferrer, resolved)?,
            Expr::InList { expr, list, .. } => {
                ctx.constraints.ty = None;

                let mut list_item_ctx = ctx.clone();
                let infer = self.infer_expr_column(expr, ctx, inferrer, resolved)?;

                let mut nullable = false;
                let mut scope = infer.scope;

                list_item_ctx.constraints.ty = Some(infer.column.ty.clone());

                for item in list {
                    let inner_infer =
                        self.infer_expr_column(item, list_item_ctx.clone(), inferrer, resolved)?;

                    nullable |= inner_infer.column.nullable;
                    scope = scope.combine(&inner_infer.scope)?;

                    list_item_ctx.constraints.scope = Some(scope);
                }

                InferredColumn {
                    column: Column::new(infer.column.ty, nullable, false),
                    scope,
                }
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
                        ctx.constraints.ty = None;
                        let infer = self.infer_expr_column(expr, ctx, inferrer, resolved)?;

                        InferredColumn {
                            column: Column::new(ty, infer.column.nullable, infer.column.default),
                            scope: infer.scope,
                        }
                    }
                    _ => todo!(),
                }
            }
            Expr::Tuple(exprs) => match ctx.constraints.ty.as_ref() {
                Some(SqlType::Tuple(cols)) => {
                    if exprs.len() != cols.len() {
                        return Err(Error::ColumnCountMismatch {
                            expected: cols.len(),
                            got: exprs.len(),
                        });
                    }

                    let inner_tuple_infer: Vec<InferredColumn> = exprs
                        .iter()
                        .zip(cols)
                        .map(|(e, col)| {
                            let mut col_ctx = ctx.clone();
                            col_ctx.constraints.ty = Some(col.ty.clone());
                            self.infer_expr_column(e, col_ctx, inferrer, resolved)
                        })
                        .collect::<Result<Vec<InferredColumn>, Error>>()?;

                    let tuple_columns: Vec<_> =
                        inner_tuple_infer.iter().map(|t| t.column.clone()).collect();

                    let scope = inner_tuple_infer
                        .iter()
                        .try_fold(Scope::Literal, |scope, infer| scope.combine(&infer.scope))?;

                    InferredColumn {
                        column: Column::new(
                            SqlType::Tuple(tuple_columns),
                            ctx.constraints.nullable.unwrap_or(false),
                            ctx.hints.default.unwrap_or(false),
                        ),
                        scope,
                    }
                }
                _ => {
                    ctx.constraints.ty = None;

                    let inner_tuple_infer: Vec<InferredColumn> = exprs
                        .iter()
                        .map(|e| self.infer_expr_column(e, ctx.clone(), inferrer, resolved))
                        .collect::<Result<Vec<InferredColumn>, Error>>()?;

                    let tuple_columns: Vec<_> =
                        inner_tuple_infer.iter().map(|t| t.column.clone()).collect();

                    let scope = inner_tuple_infer
                        .iter()
                        .try_fold(Scope::Literal, |scope, infer| scope.combine(&infer.scope))?;

                    InferredColumn {
                        column: Column::new(SqlType::Tuple(tuple_columns), false, false),
                        scope,
                    }
                }
            },
            Expr::Function(func) => self.infer_function_column(func, ctx, inferrer, resolved)?,
            Expr::Subquery(_) => {
                // Need to basically seperate out the `self.query()` so it can take some additional parameters
                // like the infer and the resolved.
                //
                // this allows us to have a subquery and query that share the same bones.
                todo!()
            }
            Expr::Between {
                expr, low, high, ..
            } => {
                ctx.constraints.ty = None;

                let mut low_ctx = ctx.clone();
                let mut high_ctx = ctx.clone();

                let value_infer = self.infer_expr_column(expr, ctx, inferrer, resolved)?;

                let mut nullable = value_infer.column.nullable;
                let mut scope = value_infer.scope;

                low_ctx.constraints.ty = Some(value_infer.column.ty.clone());
                low_ctx.constraints.nullable = Some(nullable);
                low_ctx.constraints.scope = Some(scope);

                let low_infer = self.infer_expr_column(low, low_ctx, inferrer, resolved)?;
                scope = scope.combine(&low_infer.scope)?;
                nullable |= low_infer.column.nullable;

                high_ctx.constraints.ty = Some(value_infer.column.ty.clone());
                high_ctx.constraints.nullable = Some(nullable);
                high_ctx.constraints.scope = Some(scope);

                let high_infer = self.infer_expr_column(high, high_ctx, inferrer, resolved)?;
                scope = scope.combine(&high_infer.scope)?;
                nullable |= high_infer.column.nullable;

                assert_eq!(value_infer.column.ty, low_infer.column.ty);
                assert_eq!(value_infer.column.ty, high_infer.column.ty);

                // TODO: Only allow integers, text and dates.

                InferredColumn {
                    column: Column::new(SqlType::Boolean, nullable, false),
                    scope,
                }
            }
            Expr::Case {
                operand,
                conditions,
                else_result,
                ..
            } => {
                let mut value_ctx = ctx.clone();
                let mut condition_ctx = ctx.clone();

                let operand_infer = operand
                    .as_ref()
                    .map(|o| self.infer_expr_column(o, ctx, inferrer, resolved))
                    .transpose()?;

                let mut nullable = false;
                let mut scope = operand_infer
                    .as_ref()
                    .map(|o| o.scope)
                    .unwrap_or(Scope::Literal);

                // Conditions list be empty.
                assert!(!conditions.is_empty());

                condition_ctx.constraints.ty = operand_infer
                    .as_ref()
                    .map(|o| o.column.ty.clone())
                    .or(Some(SqlType::Boolean));

                condition_ctx.constraints.scope = operand_infer
                    .as_ref()
                    .map(|o| o.scope)
                    .or(Some(Scope::Literal));

                for condition in conditions {
                    // Validation Condition.
                    let condition_infer = self.infer_expr_column(
                        &condition.condition,
                        condition_ctx.clone(),
                        inferrer,
                        resolved,
                    )?;

                    scope = scope.combine(&condition_infer.scope)?;
                    condition_ctx.constraints.scope = Some(scope);

                    // Validate Result, ensure that they are all the same type.
                    match value_ctx.constraints.ty {
                        Some(_) => {
                            let val_infer = self.infer_expr_column(
                                &condition.result,
                                value_ctx.clone(),
                                inferrer,
                                resolved,
                            )?;

                            nullable |= val_infer.column.nullable;
                            scope = scope.combine(&val_infer.scope)?;
                        }
                        None => {
                            let val_infer = self.infer_expr_column(
                                &condition.result,
                                value_ctx.clone(),
                                inferrer,
                                resolved,
                            )?;

                            value_ctx.constraints.ty = Some(val_infer.column.ty);
                            nullable |= val_infer.column.nullable;
                            scope = scope.combine(&val_infer.scope)?;
                        }
                    }
                }

                if let Some(else_result) = &else_result {
                    let else_infer =
                        self.infer_expr_column(else_result, value_ctx.clone(), inferrer, resolved)?;

                    scope = scope.combine(&else_infer.scope)?;
                    nullable |= else_infer.column.nullable;
                }

                InferredColumn {
                    column: Column::new(value_ctx.constraints.ty.unwrap(), nullable, false),
                    scope,
                }
            }
            _ => return Err(Error::Unsupported(format!("Unsupported Expr: {expr:#?}"))),
        };

        // Check the type here.
        if let Some(expected_ty) = constraints.ty
            && expected_ty != inferred.column.ty
        {
            return Err(Error::TypeMismatch {
                expected: expected_ty,
                got: inferred.column.ty,
            });
        }

        // Ensure scope compatibility.
        if let Some(expected_scope) = constraints.scope
            && matches!(
                (expected_scope, inferred.scope),
                (Scope::Row, Scope::Group) | (Scope::Group, Scope::Row)
            )
        {
            return Err(Error::IncompatibleScope);
        }

        Ok(inferred)
    }

    pub(crate) fn infer_expr_name(expr: &Expr) -> Result<Option<ColumnRef>, Error> {
        match expr {
            Expr::Identifier(ident) => Ok(Some(ColumnRef::new(None, ident.value.to_string()))),
            Expr::CompoundIdentifier(idents) => Ok(Some(ColumnRef::new(
                Some(idents.first().unwrap().value.to_string()),
                idents.get(1).unwrap().value.to_string(),
            ))),
            Expr::Nested(nested) => Self::infer_expr_name(nested),
            Expr::Wildcard(_) | Expr::QualifiedWildcard(_, _) => unreachable!(),
            _ => Ok(None),
        }
    }

    pub(crate) fn infer_value_column(
        value: &Value,
        context: &InferContext,
        resolved: &mut ResolvedQuery,
    ) -> Result<InferredColumn, Error> {
        match value {
            Value::Number(str, _) => {
                // Initially, try to use the expected type.
                if let Some(ref expected_ty) = context.constraints.ty {
                    let ty = match expected_ty {
                        SqlType::SmallInt => {
                            if str.parse::<i16>().is_ok() {
                                Some(SqlType::SmallInt)
                            } else {
                                None
                            }
                        }
                        SqlType::Integer => {
                            if str.parse::<i32>().is_ok() {
                                Some(SqlType::Integer)
                            } else {
                                None
                            }
                        }
                        SqlType::BigInt => {
                            if str.parse::<i64>().is_ok() {
                                Some(SqlType::BigInt)
                            } else {
                                None
                            }
                        }
                        SqlType::Float => {
                            if str.parse::<f32>().is_ok() {
                                Some(SqlType::Float)
                            } else {
                                None
                            }
                        }
                        SqlType::Double => {
                            if str.parse::<f64>().is_ok() {
                                Some(SqlType::Double)
                            } else {
                                None
                            }
                        }
                        _ => None,
                    };

                    if let Some(ty) = ty {
                        return Ok(InferredColumn {
                            column: Column::new(ty, false, false),
                            scope: Scope::Literal,
                        });
                    }
                };

                // Fallback to smallest type to biggest.
                let ty = if str.parse::<i16>().is_ok() {
                    SqlType::SmallInt
                } else if str.parse::<i32>().is_ok() {
                    SqlType::Integer
                } else if str.parse::<i64>().is_ok() {
                    SqlType::BigInt
                } else if str.contains('.') || str.to_lowercase().contains('e') {
                    if str.parse::<f32>().is_ok() {
                        SqlType::Float
                    } else if str.parse::<f64>().is_ok() {
                        SqlType::Double
                    } else {
                        return Err(Error::Sql("Invalid floating point number".to_string()));
                    }
                } else {
                    return Err(Error::Sql("Number is too big".to_string()));
                };

                Ok(InferredColumn {
                    column: Column::new(ty, false, false),
                    scope: Scope::Literal,
                })
            }

            #[allow(unused_variables)]
            Value::SingleQuotedString(str)
            | Value::SingleQuotedByteStringLiteral(str)
            | Value::DoubleQuotedByteStringLiteral(str)
            | Value::NationalStringLiteral(str)
            | Value::HexStringLiteral(str)
            | Value::DoubleQuotedString(str) => {
                let ty = if let Some(ref expected_ty) = context.constraints.ty {
                    match expected_ty {
                        #[cfg(feature = "time")]
                        SqlType::Timestamp => {
                            let format = format_description::parse(
                                "[year]-[month]-[day] [hour]:[minute]:[second]",
                            )
                            .unwrap();

                            PrimitiveDateTime::parse(str, &format)
                                .ok()
                                .map(|_| SqlType::Timestamp)
                        }
                        #[cfg(feature = "time")]
                        SqlType::TimestampTz => {
                            if OffsetDateTime::parse(str, &Iso8601::DEFAULT).is_ok()
                                || OffsetDateTime::parse(str, &Rfc3339).is_ok()
                            {
                                Some(SqlType::TimestampTz)
                            } else {
                                None
                            }
                        }
                        #[cfg(feature = "time")]
                        SqlType::Time => Time::parse(str, &Iso8601::DEFAULT)
                            .ok()
                            .map(|_| SqlType::Time),
                        #[cfg(feature = "time")]
                        SqlType::Date => Date::parse(str, &Iso8601::DEFAULT)
                            .ok()
                            .map(|_| SqlType::Date),
                        #[cfg(feature = "uuid")]
                        SqlType::Uuid => uuid::Uuid::parse_str(str).ok().map(|_| SqlType::Uuid),
                        #[cfg(feature = "json")]
                        SqlType::Json => serde_json::from_str::<serde::de::IgnoredAny>(str)
                            .ok()
                            .map(|_| SqlType::Json),
                        _ => None,
                    }
                } else {
                    None
                };

                let real_ty = ty.unwrap_or(SqlType::Text);

                Ok(InferredColumn {
                    column: Column::new(real_ty, false, false),
                    scope: Scope::Literal,
                })
            }
            Value::Boolean(_) => Ok(InferredColumn {
                column: Column::new(SqlType::Boolean, false, false),
                scope: Scope::Literal,
            }),
            Value::Null => {
                if let Some(ty) = context.constraints.ty.as_ref() {
                    // Can't assign null to non-nullable column.
                    if context.constraints.nullable.is_some_and(|n| !n) {
                        return Err(Error::NullOnNotNullColumn("".to_string()));
                    }

                    Ok(InferredColumn {
                        column: Column::new(ty.clone(), true, false),
                        scope: Scope::Row,
                    })
                } else {
                    Err(Error::Unsupported(
                        "Cannot infer type of the NULL".to_string(),
                    ))
                }
            }
            Value::Placeholder(placeholder) => match context.constraints.ty.as_ref() {
                Some(ty) => {
                    let col = Column::new(
                        ty.clone(),
                        context.constraints.nullable.unwrap_or(false),
                        context.hints.default.unwrap_or(false),
                    );

                    resolved.insert_input(placeholder, col.clone());

                    Ok(InferredColumn {
                        column: col,
                        scope: Scope::Row,
                    })
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
    ) -> Result<InferredColumn, Error> {
        let mut ctx = context;
        let [left, right] = exprs;

        match op {
            BinaryOperator::Plus
            | BinaryOperator::Minus
            | BinaryOperator::Multiply
            | BinaryOperator::Divide
            | BinaryOperator::Modulo => {
                let mut right_ctx = ctx.clone();
                let left_infer = self.infer_expr_column(left, ctx, inferrer, resolved)?;
                right_ctx.inherit_constraints_from_inferred_column(&left_infer);
                let right_infer = self.infer_expr_column(right, right_ctx, inferrer, resolved)?;

                assert_eq!(left_infer.column.ty, right_infer.column.ty);

                let nullable = left_infer.column.nullable | right_infer.column.nullable;
                let scope = left_infer.scope.combine(&right_infer.scope)?;

                Ok(InferredColumn {
                    column: Column::new(left_infer.column.ty, nullable, false),
                    scope,
                })
            }
            BinaryOperator::Gt
            | BinaryOperator::Lt
            | BinaryOperator::GtEq
            | BinaryOperator::LtEq
            | BinaryOperator::Eq
            | BinaryOperator::NotEq => {
                ctx.constraints.ty = None;
                let mut right_ctx = ctx.clone();

                let left_infer = self.infer_expr_column(left, ctx, inferrer, resolved)?;
                right_ctx.inherit_constraints_from_inferred_column(&left_infer);
                let right_infer = self.infer_expr_column(right, right_ctx, inferrer, resolved)?;

                assert_eq!(left_infer.column.ty, right_infer.column.ty);

                // Resulting column is only nullable if either of the two are.
                let nullable = left_infer.column.nullable | right_infer.column.nullable;
                let scope = left_infer.scope.combine(&right_infer.scope)?;

                Ok(InferredColumn {
                    column: Column::new(SqlType::Boolean, nullable, false),
                    scope,
                })
            }
            BinaryOperator::Spaceship => {
                ctx.constraints.ty = None;
                let mut right_ctx = ctx.clone();

                let left_infer = self.infer_expr_column(left, ctx, inferrer, resolved)?;
                right_ctx.inherit_constraints_from_inferred_column(&left_infer);
                let right_infer = self.infer_expr_column(right, right_ctx, inferrer, resolved)?;

                assert_eq!(left_infer.column.ty, right_infer.column.ty);

                let scope = left_infer.scope.combine(&right_infer.scope)?;

                // Spaceship operator collapses nullability.
                // Both NULL -> true
                // One NULL -> false
                // Both NOT NULL -> comparison
                Ok(InferredColumn {
                    column: Column::new(SqlType::Boolean, false, false),
                    scope,
                })
            }
            BinaryOperator::And | BinaryOperator::Or | BinaryOperator::Xor => {
                ctx.constraints.ty = None;
                let mut right_ctx = ctx.clone();

                let left_infer = self.infer_expr_column(left, ctx, inferrer, resolved)?;
                right_ctx.inherit_constraints_from_inferred_column(&left_infer);
                let right_infer = self.infer_expr_column(right, right_ctx, inferrer, resolved)?;

                assert_eq!(left_infer.column.ty, right_infer.column.ty);

                let nullable = left_infer.column.nullable | right_infer.column.nullable;
                let scope = left_infer.scope.combine(&right_infer.scope)?;

                Ok(InferredColumn {
                    column: Column::new(SqlType::Boolean, nullable, false),
                    scope,
                })
            }
            BinaryOperator::BitwiseOr | BinaryOperator::BitwiseAnd | BinaryOperator::BitwiseXor => {
                let mut right_ctx = ctx.clone();
                let left_infer = self.infer_expr_column(left, ctx, inferrer, resolved)?;

                if !left_infer.column.ty.is_integer() {
                    return Err(Error::TypeMismatch {
                        expected: SqlType::Integer,
                        got: left_infer.column.ty,
                    });
                }

                right_ctx.inherit_constraints_from_inferred_column(&left_infer);
                let right_infer = self.infer_expr_column(right, right_ctx, inferrer, resolved)?;

                assert_eq!(left_infer.column.ty, right_infer.column.ty);

                let nullable = left_infer.column.nullable | right_infer.column.nullable;
                let scope = left_infer.scope.combine(&right_infer.scope)?;

                Ok(InferredColumn {
                    column: Column::new(left_infer.column.ty, nullable, false),
                    scope,
                })
            }
            BinaryOperator::StringConcat => {
                ctx.constraints.ty = Some(SqlType::Text);
                let mut right_ctx = ctx.clone();

                let left_infer = self.infer_expr_column(left, ctx, inferrer, resolved)?;
                right_ctx.inherit_constraints_from_inferred_column(&left_infer);
                let right_infer = self.infer_expr_column(right, right_ctx, inferrer, resolved)?;

                let nullable = left_infer.column.nullable | right_infer.column.nullable;
                let scope = left_infer.scope.combine(&right_infer.scope)?;

                Ok(InferredColumn {
                    column: Column::new(SqlType::Text, nullable, false),
                    scope,
                })
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
        context: InferContext,
        inferrer: &I,
        resolved: &mut ResolvedQuery,
    ) -> Result<InferredColumn, Error> {
        let mut ctx = context;
        match op {
            UnaryOperator::Plus | UnaryOperator::Minus => {
                ctx.constraints.ty = None;

                let infer = self.infer_expr_column(expr, ctx, inferrer, resolved)?;

                if !infer.column.ty.is_numeric() {
                    Err(Error::TypeNotNumeric(infer.column.ty))
                } else {
                    Ok(infer)
                }
            }
            UnaryOperator::Not => {
                ctx.constraints.ty = Some(SqlType::Boolean);
                let infer = self.infer_expr_column(expr, ctx, inferrer, resolved)?;
                Ok(infer)
            }
            _ => Err(Error::Unsupported(format!(
                "Unsupported unary operator: {op:?}"
            ))),
        }
    }
}
