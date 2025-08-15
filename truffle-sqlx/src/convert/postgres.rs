#[cfg(feature = "uuid")]
impl_transparent_compat!(PostgreSqlDialect, uuid::Uuid);

#[cfg(feature = "time")]
impl_transparent_compat!(
    PostgreSqlDialect,
    time::PrimitiveDateTime,
    time::OffsetDateTime,
    time::Date,
    time::Time
);
