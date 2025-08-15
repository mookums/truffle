use truffle::dialect::Dialect;

// pub mod postgres;
pub mod sqlite;

pub trait IntoSql<T, D: Dialect> {
    fn into_sql_type(self) -> T;
}

pub trait FromSql<T, D: Dialect> {
    fn from_sql_type(value: T) -> Self;
}

#[macro_export]
macro_rules! impl_string_compat {
    ($d:ty, $($t:ty),*) => {
        $(
            impl IntoSql<String, $d> for $t {
                fn into_sql_type(self) -> String {
                    self.to_string()
                }
            }

            impl FromSql<String, $d> for $t {
                fn from_sql_type(value: String) -> Self {
                    value.parse().expect("Failed to parse from string")
                }
            }
        )*
    };
}

#[macro_export]
macro_rules! impl_transparent_compat {
    ($d:ty, $($t:ty),*) => {
        $(
            impl IntoSql<$t, $d> for $t {
                fn into_sql_type(self) -> $t {
                    self
                }
            }

            impl FromSql<$t, $d> for $t {
                fn from_sql_type(value: $t) -> Self {
                    value
                }
            }
        )*
    };
}
