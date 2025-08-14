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

static SIMULATOR: LazyLock<Result<Simulator, Error>> = LazyLock::new(|| {
    let config = load_config().map_err(|e| Error::new(Span::call_site(), e.to_string()))?;

    let mut sim = Simulator::with_dialect(config.dialect);

    let migrations =
        load_migrations(&config).map_err(|e| Error::new(Span::call_site(), e.to_string()))?;

    apply_migrations(&mut sim, &migrations)
        .map_err(|e| Error::new(Span::call_site(), e.to_string()))?;

    Ok(sim)
});

struct QueryInput {
    sql_lit: syn::LitStr,
    placeholders: Vec<syn::Expr>,
}

impl Parse for QueryInput {
    fn parse(input: syn::parse::ParseStream) -> syn::Result<Self> {
        let sql_lit = input.parse().unwrap();

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

        let (ty, sql_lit) = fork
            .parse::<syn::Type>()
            .ok()
            .and_then(|ty| {
                fork.parse::<Token![,]>().ok().and_then(|_| {
                    fork.parse::<syn::LitStr>().ok().map(|sql_lit| {
                        input.advance_to(&fork);

                        (Some(ty), sql_lit)
                    })
                })
            })
            .unwrap_or_else(|| (None, input.parse().unwrap()));

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

fn sql_type_to_rust_type(sql_type: &SqlType, nullable: bool) -> syn::Type {
    let base_type: syn::Type = match sql_type {
        SqlType::SmallInt => parse_quote!(i16),
        SqlType::Integer => parse_quote!(i32),
        SqlType::BigInt => parse_quote!(i64),
        SqlType::Float => parse_quote!(f32),
        SqlType::Double => parse_quote!(f64),
        SqlType::Text => parse_quote!(String),
        SqlType::Boolean => parse_quote!(bool),
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
    };

    if nullable {
        parse_quote!(Option<#base_type>)
    } else {
        base_type
    }
}

fn sql_type_to_conversion(
    name: &syn::Ident,
    sql_type: &SqlType,
    nullable: bool,
    expr: &syn::Expr,
) -> TokenStream {
    let rust_type = sql_type_to_rust_type(sql_type, nullable);
    let is_text = sql_type == &SqlType::Text;

    match (is_text, nullable) {
        (true, true) => {
            quote! { let #name: #rust_type = (#expr).map(|a| a.to_string()); }
        }
        (true, false) => {
            quote! { let #name: #rust_type = (#expr).to_string(); }
        }
        (false, true) => {
            quote! { let #name: #rust_type = (#expr).map(|a| a.into()); }
        }
        (false, false) => {
            quote! { let #name: #rust_type = (#expr).into(); }
        }
    }
}

// Validates the syntax and semantics of your SQL at compile time.
#[proc_macro]
pub fn query(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    let parsed = syn::parse_macro_input!(input as QueryInput);
    let sql = parsed.sql_lit.value();

    let mut sim = match SIMULATOR.as_ref() {
        Ok(simulator) => simulator.clone(),
        Err(e) => return e.to_compile_error().into(),
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
            let conversion =
                sql_type_to_conversion(&binding, &column.ty, column.nullable, rust_expr);

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

    let mut sim = match SIMULATOR.as_ref() {
        Ok(simulator) => simulator.clone(),
        Err(e) => return e.to_compile_error().into(),
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
            let conversion =
                sql_type_to_conversion(&binding, &column.ty, column.nullable, rust_expr);

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
                let rust_type = sql_type_to_rust_type(&col.ty, col.nullable);

                quote! {
                    #field_ident: row.try_get_unchecked::<#rust_type, _>(#field_name)?.into(),
                }
            })
            .collect();

        let row_type: syn::Type = match sim.kind {
            DialectKind::Generic | DialectKind::Ansi => {
                panic!("Must use a real database dialect instead of {:?}", sim.kind)
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
                let true_type = sql_type_to_rust_type(&col.ty, col.nullable);
                let field_name = syn::Ident::new(&name.name, Span::call_site());

                quote! {
                    pub #field_name: #true_type,
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
