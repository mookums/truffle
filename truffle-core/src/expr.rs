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
pub struct InferContext<'a> {
    ty: Option<SqlType>,
    nullable: Option<bool>,
    default: Option<bool>,
    scope: Option<Scope>,
    inherited_scope: Option<Scope>,
    grouped: &'a [Expr],
}

impl<'a> InferContext<'a> {
    pub fn with_type(self, ty: SqlType) -> Self {
        Self {
            ty: Some(ty),
            nullable: self.nullable,
            default: self.default,
            scope: self.scope,
            inherited_scope: self.inherited_scope,
            grouped: self.grouped,
        }
    }

    pub fn with_nullable(self, nullable: bool) -> Self {
        Self {
            ty: self.ty,
            nullable: Some(nullable),
            default: self.default,
            scope: self.scope,
            inherited_scope: self.inherited_scope,
            grouped: self.grouped,
        }
    }

    pub fn with_scope(self, scope: Scope) -> Self {
        Self {
            ty: self.ty,
            nullable: self.nullable,
            default: self.default,
            scope: Some(scope),
            inherited_scope: self.inherited_scope,
            grouped: self.grouped,
        }
    }

    pub fn with_inherited_scope(self, inherited_scope: Scope) -> Self {
        Self {
            ty: self.ty,
            nullable: self.nullable,
            default: self.default,
            scope: self.scope,
            inherited_scope: Some(inherited_scope),
            grouped: self.grouped,
        }
    }

    pub fn with_grouped(self, grouped: &'a [Expr]) -> Self {
        Self {
            ty: self.ty,
            nullable: self.nullable,
            default: self.default,
            scope: self.scope,
            inherited_scope: self.inherited_scope,
            grouped,
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
        let expect = context.clone();

        let mut context = context;

        if context.grouped.contains(expr) {
            context.scope = Some(Scope::Group);
            context.inherited_scope = Some(Scope::Group);
        }

        let inferred: InferredColumn = match expr {
            Expr::Value(val) => Self::infer_value_column(&val.value, context, resolved)?,
            Expr::IsTrue(expr) | Expr::IsFalse(expr) => {
                let infer = self.infer_expr_column(
                    expr,
                    InferContext::default().with_type(SqlType::Boolean),
                    inferrer,
                    resolved,
                )?;

                InferredColumn {
                    column: Column::new(SqlType::Boolean, infer.column.nullable, false),
                    scope: infer.scope,
                }
            }
            Expr::IsNotTrue(expr)
            | Expr::IsNotFalse(expr)
            | Expr::IsUnknown(expr)
            | Expr::IsNotUnknown(expr) => {
                let infer = self.infer_expr_column(
                    expr,
                    InferContext::default().with_type(SqlType::Boolean),
                    inferrer,
                    resolved,
                )?;

                InferredColumn {
                    column: Column::new(SqlType::Boolean, false, false),
                    scope: infer.scope,
                }
            }
            Expr::IsNull(expr) | Expr::IsNotNull(expr) => {
                let infer =
                    self.infer_expr_column(expr, InferContext::default(), inferrer, resolved)?;

                InferredColumn {
                    column: Column::new(SqlType::Boolean, false, false),
                    scope: infer.scope,
                }
            }
            Expr::IsNormalized { expr, .. } => {
                let infer = self.infer_expr_column(
                    expr,
                    InferContext::default().with_type(SqlType::Text),
                    inferrer,
                    resolved,
                )?;

                InferredColumn {
                    column: Column::new(SqlType::Boolean, infer.column.nullable, false),
                    scope: infer.scope,
                }
            }
            Expr::IsDistinctFrom(left, right) | Expr::IsNotDistinctFrom(left, right) => {
                let left_infer =
                    self.infer_expr_column(left, InferContext::default(), inferrer, resolved)?;

                let right_infer = self.infer_expr_column(
                    right,
                    InferContext::default()
                        .with_type(left_infer.column.ty.clone())
                        .with_scope(left_infer.scope),
                    inferrer,
                    resolved,
                )?;

                assert_eq!(left_infer.column.ty, right_infer.column.ty);

                let scope = left_infer.scope.combine(&right_infer.scope)?;

                InferredColumn {
                    column: Column::new(SqlType::Boolean, false, false),
                    scope,
                }
            }
            Expr::Like { expr, .. } | Expr::ILike { expr, .. } => {
                let infer = self.infer_expr_column(
                    expr,
                    InferContext::default().with_type(SqlType::Text),
                    inferrer,
                    resolved,
                )?;

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
                let str_infer = self.infer_expr_column(
                    expr,
                    InferContext::default().with_type(SqlType::Text),
                    inferrer,
                    resolved,
                )?;

                let mut scope = str_infer.scope;

                // Ensure that the from is an integer.
                if let Some(from_expr) = substring_from {
                    let from_infer = self.infer_expr_column(
                        from_expr,
                        InferContext::default().with_scope(scope),
                        inferrer,
                        resolved,
                    )?;

                    if !from_infer.column.ty.is_integer() {
                        return Err(Error::TypeNotNumeric(from_infer.column.ty));
                    }

                    scope = scope.combine(&from_infer.scope)?;
                }

                // Ensure that the for is an integer.
                if let Some(for_expr) = substring_for {
                    let for_infer = self.infer_expr_column(
                        for_expr,
                        InferContext::default().with_scope(scope),
                        inferrer,
                        resolved,
                    )?;

                    if !for_infer.column.ty.is_integer() {
                        return Err(Error::TypeNotNumeric(for_infer.column.ty));
                    }

                    scope = scope.combine(&for_infer.scope)?;
                }

                InferredColumn {
                    column: Column::new(SqlType::Text, str_infer.column.nullable, false),
                    scope,
                }
            }
            Expr::Identifier(ident) => {
                let name = &ident.value;

                let column = inferrer
                    .infer_unqualified_column(self, name)?
                    .ok_or_else(|| Error::ColumnDoesntExist(name.to_string()))?;

                let scope = if context.grouped.contains(expr)
                    || context.inherited_scope.is_some_and(|is| is == Scope::Group)
                {
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

                let scope = if context.grouped.contains(expr)
                    || context.inherited_scope.is_some_and(|is| is == Scope::Group)
                {
                    Scope::Group
                } else {
                    Scope::Row
                };

                InferredColumn { column, scope }
            }
            Expr::BinaryOp { left, right, op } => {
                self.infer_binary_op_column([left, right], op, context, inferrer, resolved)?
            }
            Expr::UnaryOp { expr, op } => {
                self.infer_unary_op_column(expr, op, context, inferrer, resolved)?
            }
            Expr::Nested(expr) => self.infer_expr_column(expr, context, inferrer, resolved)?,
            Expr::InList { expr, list, .. } => {
                let infer =
                    self.infer_expr_column(expr, InferContext::default(), inferrer, resolved)?;

                let mut nullable = false;
                let mut scope = infer.scope;

                for item in list {
                    let inner_infer = self.infer_expr_column(
                        item,
                        InferContext::default()
                            .with_type(infer.column.ty.clone())
                            .with_scope(infer.scope),
                        inferrer,
                        resolved,
                    )?;

                    nullable |= inner_infer.column.nullable;
                    scope = scope.combine(&inner_infer.scope)?;
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
                        let infer = self.infer_expr_column(
                            expr,
                            InferContext::default(),
                            inferrer,
                            resolved,
                        )?;

                        InferredColumn {
                            column: Column::new(ty, infer.column.nullable, infer.column.default),
                            scope: infer.scope,
                        }
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

                    let inner_tuple_infer: Vec<InferredColumn> = exprs
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
                        .collect::<Result<Vec<InferredColumn>, Error>>()?;

                    let tuple_columns: Vec<_> =
                        inner_tuple_infer.iter().map(|t| t.column.clone()).collect();

                    let scope = inner_tuple_infer
                        .iter()
                        .try_fold(Scope::Literal, |scope, infer| scope.combine(&infer.scope))?;

                    InferredColumn {
                        column: Column::new(
                            SqlType::Tuple(tuple_columns),
                            context.nullable.unwrap_or(false),
                            context.default.unwrap_or(false),
                        ),
                        scope,
                    }
                }
                _ => {
                    let inner_tuple_infer: Vec<InferredColumn> = exprs
                        .iter()
                        .map(|e| {
                            self.infer_expr_column(e, InferContext::default(), inferrer, resolved)
                        })
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
            Expr::Function(func) => {
                self.infer_function_column(func, context, inferrer, resolved)?
            }
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
                let value_infer =
                    self.infer_expr_column(expr, InferContext::default(), inferrer, resolved)?;

                let mut scope = value_infer.scope;

                let low_infer = self.infer_expr_column(
                    low,
                    InferContext::default()
                        .with_type(value_infer.column.ty.clone())
                        .with_scope(scope),
                    inferrer,
                    resolved,
                )?;

                scope = scope.combine(&low_infer.scope)?;

                let high_infer = self.infer_expr_column(
                    high,
                    InferContext::default()
                        .with_type(value_infer.column.ty.clone())
                        .with_scope(scope),
                    inferrer,
                    resolved,
                )?;

                scope = scope.combine(&high_infer.scope)?;

                assert_eq!(value_infer.column.ty, low_infer.column.ty);
                assert_eq!(value_infer.column.ty, high_infer.column.ty);

                // TODO: Only allow integers, text and dates.

                InferredColumn {
                    column: Column::new(SqlType::Boolean, false, false),
                    scope,
                }
            }
            Expr::Case {
                operand,
                conditions,
                else_result,
                ..
            } => {
                let operand_infer = operand
                    .as_ref()
                    .map(|o| self.infer_expr_column(o, InferContext::default(), inferrer, resolved))
                    .transpose()?;

                let mut result_ty: Option<SqlType> = None;
                let mut nullable = false;
                let mut scope = operand_infer
                    .as_ref()
                    .map(|o| o.scope)
                    .unwrap_or(Scope::Literal);

                // Conditions list be empty.
                assert!(!conditions.is_empty());

                for condition in conditions {
                    let context = match &operand_infer {
                        Some(infer) => InferContext::default()
                            .with_type(infer.column.ty.clone())
                            .with_scope(scope),
                        None => InferContext::default().with_type(SqlType::Boolean),
                    };

                    // Validation Condition.
                    let condition_infer =
                        self.infer_expr_column(&condition.condition, context, inferrer, resolved)?;

                    scope = scope.combine(&condition_infer.scope)?;

                    // Validate Result, ensure that they are all the same type.
                    match result_ty {
                        Some(ref ty) => {
                            let val_infer = self.infer_expr_column(
                                &condition.result,
                                InferContext::default()
                                    .with_type(ty.clone())
                                    .with_scope(scope),
                                inferrer,
                                resolved,
                            )?;

                            nullable |= val_infer.column.nullable;
                            scope = scope.combine(&val_infer.scope)?;
                        }
                        None => {
                            let val_infer = self.infer_expr_column(
                                &condition.result,
                                InferContext::default().with_scope(scope),
                                inferrer,
                                resolved,
                            )?;

                            result_ty = Some(val_infer.column.ty);
                            nullable |= val_infer.column.nullable;
                            scope = scope.combine(&val_infer.scope)?;
                        }
                    }
                }

                if let Some(else_result) = &else_result {
                    let else_infer = self.infer_expr_column(
                        else_result,
                        InferContext::default()
                            .with_type(result_ty.as_ref().unwrap().clone())
                            .with_scope(scope),
                        inferrer,
                        resolved,
                    )?;

                    scope = scope.combine(&else_infer.scope)?;
                }

                InferredColumn {
                    column: Column::new(result_ty.unwrap(), nullable, false),
                    scope,
                }
            }
            _ => return Err(Error::Unsupported(format!("Unsupported Expr: {expr:#?}"))),
        };

        // Check the type here.
        if let Some(expected_ty) = expect.ty
            && expected_ty != inferred.column.ty
        {
            return Err(Error::TypeMismatch {
                expected: expected_ty,
                got: inferred.column.ty,
            });
        }

        // Ensure scope compatibility.
        if let Some(expected_scope) = expect.scope
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
        context: InferContext,
        resolved: &mut ResolvedQuery,
    ) -> Result<InferredColumn, Error> {
        match value {
            Value::Number(str, _) => {
                // Initially, try to use the expected type.
                if let Some(expected_ty) = context.ty {
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
                let ty = if let Some(expected_ty) = context.ty {
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
                if let Some(ty) = context.ty {
                    // Can't assign null to non-nullable column.
                    if context.nullable.is_some_and(|n| !n) {
                        return Err(Error::NullOnNotNullColumn("".to_string()));
                    }

                    Ok(InferredColumn {
                        column: Column::new(ty, true, false),
                        scope: Scope::Row,
                    })
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
        let [left, right] = exprs;
        match op {
            BinaryOperator::Plus
            | BinaryOperator::Minus
            | BinaryOperator::Multiply
            | BinaryOperator::Divide
            | BinaryOperator::Modulo => {
                let left_infer =
                    self.infer_expr_column(left, context.clone(), inferrer, resolved)?;

                let right_infer = self.infer_expr_column(
                    right,
                    InferContext::default()
                        .with_type(left_infer.column.ty.clone())
                        .with_nullable(left_infer.column.nullable)
                        .with_scope(left_infer.scope)
                        .with_grouped(context.grouped),
                    inferrer,
                    resolved,
                )?;

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
                let left_infer =
                    self.infer_expr_column(left, InferContext::default(), inferrer, resolved)?;

                let right_infer = self.infer_expr_column(
                    right,
                    InferContext::default()
                        .with_type(left_infer.column.ty.clone())
                        .with_nullable(left_infer.column.nullable)
                        .with_scope(left_infer.scope),
                    inferrer,
                    resolved,
                )?;

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
                let left_infer =
                    self.infer_expr_column(left, InferContext::default(), inferrer, resolved)?;

                let right_infer = self.infer_expr_column(
                    right,
                    InferContext::default()
                        .with_type(left_infer.column.ty.clone())
                        .with_nullable(left_infer.column.nullable)
                        .with_scope(left_infer.scope),
                    inferrer,
                    resolved,
                )?;

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
                let left_infer =
                    self.infer_expr_column(left, InferContext::default(), inferrer, resolved)?;

                let right_infer = self.infer_expr_column(
                    right,
                    InferContext::default()
                        .with_type(left_infer.column.ty.clone())
                        .with_nullable(left_infer.column.nullable)
                        .with_scope(left_infer.scope),
                    inferrer,
                    resolved,
                )?;

                assert_eq!(left_infer.column.ty, right_infer.column.ty);

                let nullable = left_infer.column.nullable | right_infer.column.nullable;
                let scope = left_infer.scope.combine(&right_infer.scope)?;

                Ok(InferredColumn {
                    column: Column::new(SqlType::Boolean, nullable, false),
                    scope,
                })
            }
            BinaryOperator::BitwiseOr | BinaryOperator::BitwiseAnd | BinaryOperator::BitwiseXor => {
                let left_infer = self.infer_expr_column(left, context, inferrer, resolved)?;

                if !left_infer.column.ty.is_integer() {
                    return Err(Error::TypeMismatch {
                        expected: SqlType::Integer,
                        got: left_infer.column.ty,
                    });
                }

                let right_infer = self.infer_expr_column(
                    right,
                    InferContext::default()
                        .with_type(left_infer.column.ty.clone())
                        .with_nullable(left_infer.column.nullable)
                        .with_scope(left_infer.scope),
                    inferrer,
                    resolved,
                )?;

                assert_eq!(left_infer.column.ty, right_infer.column.ty);

                let nullable = left_infer.column.nullable | right_infer.column.nullable;
                let scope = left_infer.scope.combine(&right_infer.scope)?;

                Ok(InferredColumn {
                    column: Column::new(left_infer.column.ty, nullable, false),
                    scope,
                })
            }
            BinaryOperator::StringConcat => {
                let left_infer = self.infer_expr_column(
                    left,
                    InferContext::default().with_type(SqlType::Text),
                    inferrer,
                    resolved,
                )?;

                let right_infer = self.infer_expr_column(
                    right,
                    InferContext::default()
                        .with_type(SqlType::Text)
                        .with_nullable(left_infer.column.nullable)
                        .with_scope(left_infer.scope),
                    inferrer,
                    resolved,
                )?;

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
        _: InferContext,
        inferrer: &I,
        resolved: &mut ResolvedQuery,
    ) -> Result<InferredColumn, Error> {
        match op {
            UnaryOperator::Plus | UnaryOperator::Minus => {
                let infer =
                    self.infer_expr_column(expr, InferContext::default(), inferrer, resolved)?;

                if !infer.column.ty.is_numeric() {
                    Err(Error::TypeNotNumeric(infer.column.ty))
                } else {
                    Ok(infer)
                }
            }
            UnaryOperator::Not => {
                let infer = self.infer_expr_column(
                    expr,
                    InferContext::default().with_type(SqlType::Boolean),
                    inferrer,
                    resolved,
                )?;

                assert_eq!(infer.column.ty, SqlType::Boolean);
                Ok(infer)
            }
            _ => Err(Error::Unsupported(format!(
                "Unsupported unary operator: {op:?}"
            ))),
        }
    }
}
