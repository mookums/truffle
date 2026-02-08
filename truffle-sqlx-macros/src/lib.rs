use proc_macro2::{Span, TokenStream};
use quote::quote;
use std::{
    hash::{DefaultHasher, Hash, Hasher},
    sync::LazyLock,
};
use syn::{
    Error, Token,
    parse::{Parse, discouraged::Speculative},
    parse_quote,
};
use truffle::{DialectKind, Simulator, ty::SqlType};
use truffle_loader::{
    config::load_config,
    migrations::{apply_migrations, load_migrations},
};

static SIMULATOR: LazyLock<Result<Simulator, String>> = LazyLock::new(|| {
    let config = load_config().map_err(|e| e.to_string())?;
    let mut sim = Simulator::with_dialect(config.dialect);
    let migrations = load_migrations(&config).map_err(|e| e.to_string())?;
    apply_migrations(&mut sim, &migrations).map_err(|e| e.to_string())?;

    Ok(sim)
});

fn get_simulator() -> Result<Simulator, proc_macro::TokenStream> {
    SIMULATOR.as_ref().map(|sim| sim.clone()).map_err(|e| {
        Error::new(Span::call_site(), e.as_str())
            .to_compile_error()
            .into()
    })
}

struct QueryInput {
    sql_lit: syn::LitStr,
    placeholders: Vec<syn::Expr>,
}

impl Parse for QueryInput {
    fn parse(input: syn::parse::ParseStream) -> syn::Result<Self> {
        let sql_lit = input.parse()?;

        let placeholders: Vec<_> = if input.is_empty() {
            Vec::new()
        } else {
            // Take the comma after SQL.
            input.parse::<Token![,]>()?;

            input
                .parse_terminated(syn::Expr::parse, Token![,])?
                .into_iter()
                .collect()
        };

        Ok(QueryInput {
            sql_lit,
            placeholders,
        })
    }
}

struct QueryAsInput {
    ty: Option<syn::Type>,
    sql_lit: syn::LitStr,
    placeholders: Vec<syn::Expr>,
}

impl Parse for QueryAsInput {
    fn parse(input: syn::parse::ParseStream) -> syn::Result<Self> {
        let fork = input.fork();

        let (ty, sql_lit) = if let Ok(ty) = fork.parse::<syn::Type>() {
            if fork.parse::<Token![,]>().is_ok() {
                if let Ok(sql_lit) = fork.parse::<syn::LitStr>() {
                    input.advance_to(&fork);
                    (Some(ty), sql_lit)
                } else {
                    (None, input.parse()?)
                }
            } else {
                (None, input.parse()?)
            }
        } else {
            (None, input.parse()?)
        };

        let placeholders: Vec<_> = if input.is_empty() {
            Vec::new()
        } else {
            // Take the comma after SQL.

            input.parse::<Token![,]>()?;

            input
                .parse_terminated(syn::Expr::parse, Token![,])?
                .into_iter()
                .collect()
        };

        Ok(QueryAsInput {
            ty,
            sql_lit,
            placeholders,
        })
    }
}

fn sql_type_to_rust_type(sql_type: &SqlType, dialect: &DialectKind) -> syn::Type {
    match sql_type {
        SqlType::SmallInt => parse_quote!(i16),
        SqlType::Integer => match dialect {
            DialectKind::Sqlite => parse_quote!(i64),
            _ => parse_quote!(i32),
        },
        SqlType::BigInt => parse_quote!(i64),
        SqlType::Float => parse_quote!(f32),
        SqlType::Double => parse_quote!(f64),
        SqlType::Text => parse_quote!(String),
        SqlType::Boolean => match dialect {
            DialectKind::Generic | DialectKind::Ansi | DialectKind::Postgres => parse_quote!(bool),
            DialectKind::Sqlite => parse_quote!(i32),
        },
        #[cfg(feature = "time")]
        SqlType::Date => parse_quote!(time::Date),
        #[cfg(feature = "time")]
        SqlType::Time => parse_quote!(time::Time),
        #[cfg(feature = "time")]
        SqlType::Timestamp => parse_quote!(time::PrimitiveDateTime),
        #[cfg(feature = "time")]
        SqlType::TimestampTz => parse_quote!(time::OffsetDateTime),
        #[cfg(feature = "uuid")]
        SqlType::Uuid => parse_quote!(uuid::Uuid),
        #[cfg(feature = "json")]
        SqlType::Json => parse_quote!(serde_json::Value),
        _ => panic!("Unsupported Type: {sql_type:?}"),
    }
}

fn sql_type_into(
    name: &syn::Ident,
    sql_type: &SqlType,
    nullable: bool,
    expr: &syn::Expr,
    dialect: &DialectKind,
) -> TokenStream {
    let storage_type = sql_type_to_rust_type(sql_type, dialect);
    let dialect_type: syn::Type = match dialect {
        DialectKind::Sqlite => parse_quote!(truffle_sqlx::dialect::SqliteDialect),
        DialectKind::Postgres => parse_quote!(truffle_sqlx::dialect::PostgreSqlDialect),
        _ => panic!("Unsupported dialect: {dialect:?}"),
    };

    if nullable {
        quote! {
            let #name: Option<#storage_type> = (#expr).map(|a| <_ as truffle_sqlx::convert::IntoSql<#storage_type, #dialect_type>>::into_sql_type(a));
        }
    } else {
        quote! {
            let #name: #storage_type = <_ as truffle_sqlx::convert::IntoSql<#storage_type, #dialect_type>>::into_sql_type(#expr);
        }
    }
}

fn sql_type_from(
    field_name: &str,
    sql_type: &SqlType,
    nullable: bool,
    dialect: &DialectKind,
) -> TokenStream {
    let storage_type = sql_type_to_rust_type(sql_type, dialect);
    let dialect_type: syn::Type = match dialect {
        DialectKind::Sqlite => parse_quote!(truffle_sqlx::dialect::SqliteDialect),
        DialectKind::Postgres => parse_quote!(truffle_sqlx::dialect::PostgreSqlDialect),
        _ => panic!("Unsupported dialect: {dialect:?}"),
    };

    if nullable {
        quote! {
            row.try_get::<Option<#storage_type>, _>(#field_name)?
                .map(|v| <_ as truffle_sqlx::convert::FromSql<#storage_type, #dialect_type>>::from_sql_type(v))
        }
    } else {
        quote! {
            <_ as truffle_sqlx::convert::FromSql<#storage_type, #dialect_type>>::from_sql_type(
                row.try_get::<#storage_type, _>(#field_name)?
            )
        }
    }
}

// Validates the syntax and semantics of your SQL at compile time.
#[proc_macro]
pub fn query(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    let parsed = syn::parse_macro_input!(input as QueryInput);
    let sql = parsed.sql_lit.value();

    let mut sim = match get_simulator() {
        Ok(sim) => sim,
        Err(tokens) => return tokens,
    };

    let resolve = match sim.execute(&sql) {
        Ok(resolve) => resolve,
        Err(e) => {
            return Error::new(parsed.sql_lit.span(), e.to_string())
                .to_compile_error()
                .into();
        }
    };

    // Ensure that we have matched all of the placeholders.
    // TODO: we only really only care if they are different as multiple `$1` is 1.
    if resolve.inputs.len() != parsed.placeholders.len() {
        return Error::new(
            parsed.sql_lit.span(),
            format!(
                "Expected {} placeholders but got {}",
                resolve.inputs.len(),
                parsed.placeholders.len()
            ),
        )
        .to_compile_error()
        .into();
    }

    let bindings: Vec<_> = resolve
        .inputs
        .iter()
        .zip(parsed.placeholders.iter())
        .enumerate()
        .map(|(i, (column, rust_expr))| {
            let binding = syn::Ident::new(&format!("_arg_{i}"), Span::call_site());
            let conversion = sql_type_into(
                &binding,
                &column.ty,
                column.nullable,
                rust_expr,
                &sim.dialect.kind(),
            );

            (conversion, binding)
        })
        .collect::<Vec<_>>();

    let (conversions, binding_names): (Vec<_>, Vec<_>) = bindings.into_iter().unzip();

    quote! {
        {
            #(#conversions)*
            sqlx::query(#sql)#(.bind(#binding_names))*
        }
    }
    .into()
}

// Validates the syntax and semantics of your SQL at compile time.
#[proc_macro]
pub fn query_as(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    let parsed = syn::parse_macro_input!(input as QueryAsInput);
    let sql = parsed.sql_lit.value();

    let mut sim = match get_simulator() {
        Ok(sim) => sim,
        Err(tokens) => return tokens,
    };

    let resolve = match sim.execute(&sql) {
        Ok(resolve) => resolve,
        Err(e) => {
            return Error::new(parsed.sql_lit.span(), e.to_string())
                .to_compile_error()
                .into();
        }
    };

    // Ensure that we have matched all of the placeholders.
    if resolve.inputs.len() != parsed.placeholders.len() {
        return Error::new(
            parsed.sql_lit.span(),
            format!(
                "Expected {} placeholders but got {}",
                resolve.inputs.len(),
                parsed.placeholders.len()
            ),
        )
        .to_compile_error()
        .into();
    }

    let bindings: Vec<_> = resolve
        .inputs
        .iter()
        .zip(parsed.placeholders.iter())
        .enumerate()
        .map(|(i, (column, rust_expr))| {
            let binding = syn::Ident::new(&format!("_arg_{i}"), Span::call_site());
            let conversion = sql_type_into(
                &binding,
                &column.ty,
                column.nullable,
                rust_expr,
                &sim.dialect.kind(),
            );

            (conversion, binding)
        })
        .collect::<Vec<_>>();

    let (conversions, binding_names): (Vec<_>, Vec<_>) = bindings.into_iter().unzip();

    if let Some(ty) = parsed.ty {
        let fields: Vec<_> = resolve
            .outputs
            .iter()
            .map(|(name, col)| {
                let field_name = &name.name;
                let field_ident = syn::Ident::new(field_name, Span::call_site());

                let conversion =
                    sql_type_from(field_name, &col.ty, col.nullable, &sim.dialect.kind());

                quote! {
                    #field_ident: #conversion,
                }
            })
            .collect();

        let row_type: syn::Type = match sim.dialect.kind() {
            DialectKind::Generic | DialectKind::Ansi => {
                panic!(
                    "Must use a real database dialect instead of {:?}",
                    sim.dialect.kind()
                )
            }
            DialectKind::Sqlite => parse_quote!(sqlx::sqlite::SqliteRow),
            DialectKind::Postgres => parse_quote!(sqlx::postgres::PgRow),
        };

        // Run your SQL.
        quote! {
            {
                #(#conversions)*
                sqlx::query(#sql)#(.bind(#binding_names))*.try_map(|row: #row_type| {
                    use sqlx::Row as _;
                    Ok(#ty { #(#fields)* })
                })
            }
        }
        .into()
    } else {
        let result_fields: Vec<_> = resolve
            .outputs
            .iter()
            .map(|(name, col)| {
                let true_type = sql_type_to_rust_type(&col.ty, &sim.dialect.kind());
                let field_name = syn::Ident::new(&name.name, Span::call_site());

                if col.nullable {
                    quote! {
                        pub #field_name: Option<#true_type>,
                    }
                } else {
                    quote! {
                        pub #field_name: #true_type,
                    }
                }
            })
            .collect();

        let mut hasher = DefaultHasher::new();
        sql.hash(&mut hasher);
        let hashed = hasher.finish();

        let result_struct_name =
            syn::Ident::new(&format!("QueryResult_{hashed}"), Span::call_site());

        // Run your SQL.
        quote! {
            {
                #[derive(Debug, Clone, sqlx::FromRow)]
                pub struct #result_struct_name {
                    #(#result_fields)*
                }

                #(#conversions)*
                sqlx::query_as::<_, #result_struct_name>(#sql)#(.bind(#binding_names))*
            }
        }
        .into()
    }
}

// #[proc_macro]
// pub fn query_scalar(input: TokenStream) -> TokenStream {
//     let parsed = syn::parse_macro_input!(input as QueryInput);
//     let sql = parsed.sql_lit.value();

//     let mut sim = match SIMULATOR.as_ref() {
//         Ok(simulator) => simulator.clone(),
//         Err(e) => return e.to_compile_error().into(),
//     };

//     if let Err(e) = sim.execute(&sql) {
//         return Error::new(parsed.sql_lit.span(), e.to_string())
//             .to_compile_error()
//             .into();
//     }

//     // Run your SQL.
//     match parsed.ty {
//         Some(ty) => TokenStream::from(quote! {
//             sqlx::query_scalar::<_, #ty>(#sql)
//         }),
//         None => TokenStream::from(quote! {
//             sqlx::query_scalar(#sql)
//         }),
//     }
// }
