use proc_macro::{Span, TokenStream};
use quote::quote;
use std::{
    hash::{DefaultHasher, Hash, Hasher},
    sync::LazyLock,
};
use syn::{Error, Token, parse::Parse};
use truffle::{Simulator, ty::SqlType};
use truffle_loader::{
    config::load_config,
    migrations::{apply_migrations, load_migrations},
};

static SIMULATOR: LazyLock<Result<Simulator, Error>> = LazyLock::new(|| {
    let config = load_config().map_err(|e| Error::new(Span::call_site().into(), e.to_string()))?;

    let mut sim = Simulator::with_config(&config);

    let migrations = load_migrations(&config)
        .map_err(|e| Error::new(Span::call_site().into(), e.to_string()))?;

    apply_migrations(&mut sim, &migrations)
        .map_err(|e| Error::new(Span::call_site().into(), e.to_string()))?;

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

/// Validates the syntax and semantics of your SQL at compile time.
#[proc_macro]
pub fn query(input: TokenStream) -> TokenStream {
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
    if resolve.inputs.len() != parsed.placeholders.len() {
        return Error::new(parsed.sql_lit.span(), "Unmatched placeholders".to_string())
            .to_compile_error()
            .into();
    }

    let bindings: Vec<_> = resolve
        .input_iter()
        .zip(parsed.placeholders.iter())
        .enumerate()
        .map(|(i, (sql_type, rust_expr))| {
            let binding = syn::Ident::new(&format!("_arg_{i}"), Span::call_site().into());

            let conversion = match sql_type {
                SqlType::SmallInt => quote! { let #binding: i16 = (#rust_expr).into(); },
                SqlType::Integer => quote! { let #binding: i32 = (#rust_expr).into(); },
                SqlType::BigInt => quote! { let #binding: i64 = (#rust_expr).into(); },
                SqlType::Float => quote! { let #binding: f32 = (#rust_expr).into(); },
                SqlType::Double => quote! { let #binding: f64 = (#rust_expr).into(); },
                SqlType::Text => quote! { let #binding: String = (#rust_expr).to_string(); },
                SqlType::Boolean => quote! { let #binding: bool = (#rust_expr).into(); },
                _ => quote! { let #binding = #rust_expr; },
            };

            (conversion, binding)
        })
        .collect::<Vec<_>>();

    let (conversions, binding_names): (Vec<_>, Vec<_>) = bindings.into_iter().unzip();

    TokenStream::from(quote! {
        {
            #(#conversions)*
            sqlx::query(#sql)#(.bind(#binding_names))*
        }
    })
}

/// Validates the syntax and semantics of your SQL at compile time.
#[proc_macro]
pub fn query_as(input: TokenStream) -> TokenStream {
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
    if resolve.inputs.len() != parsed.placeholders.len() {
        return Error::new(parsed.sql_lit.span(), "Unmatched placeholders".to_string())
            .to_compile_error()
            .into();
    }

    let bindings: Vec<_> = resolve
        .input_iter()
        .zip(parsed.placeholders.iter())
        .enumerate()
        .map(|(i, (sql_type, rust_expr))| {
            let binding = syn::Ident::new(&format!("_arg_{i}"), Span::call_site().into());

            let conversion = match sql_type {
                SqlType::SmallInt => quote! { let #binding: i16 = (#rust_expr).into(); },
                SqlType::Integer => quote! { let #binding: i32 = (#rust_expr).into(); },
                SqlType::BigInt => quote! { let #binding: i64 = (#rust_expr).into(); },
                SqlType::Float => quote! { let #binding: f32 = (#rust_expr).into(); },
                SqlType::Double => quote! { let #binding: f64 = (#rust_expr).into(); },
                SqlType::Text => quote! { let #binding: String = (#rust_expr).to_string(); },
                SqlType::Boolean => quote! { let #binding: bool = (#rust_expr).into(); },
                _ => quote! { let #binding = #rust_expr; },
            };

            (conversion, binding)
        })
        .collect::<Vec<_>>();

    let (conversions, binding_names): (Vec<_>, Vec<_>) = bindings.into_iter().unzip();

    let result_fields: Vec<_> = resolve
        .output_iter()
        .map(|(name, col)| {
            let base_type = match col.ty {
                SqlType::SmallInt => quote!(i16),
                SqlType::Integer => quote!(i32),
                SqlType::BigInt => quote!(i64),
                SqlType::Float => quote!(f32),
                SqlType::Double => quote!(f64),
                SqlType::Text => quote!(String),
                SqlType::Boolean => quote!(bool),
                _ => panic!("Unsupported Type"),
            };

            let true_type = if col.nullable {
                quote! { Option<#base_type> }
            } else {
                base_type
            };

            let field_name = syn::Ident::new(&name.name, Span::call_site().into());

            quote! {
                pub #field_name: #true_type,
            }
        })
        .collect();

    let mut hasher = DefaultHasher::new();
    sql.hash(&mut hasher);
    let hashed = hasher.finish();

    let result_struct_name =
        syn::Ident::new(&format!("QueryResult_{hashed}"), Span::call_site().into());

    // Run your SQL.
    TokenStream::from(quote! {
        {
            #[derive(Debug, Clone, sqlx::FromRow)]
            pub struct #result_struct_name {
                #(#result_fields)*
            }

            #(#conversions)*
            sqlx::query_as::<_, #result_struct_name>(#sql)#(.bind(#binding_names))*
        }
    })
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
