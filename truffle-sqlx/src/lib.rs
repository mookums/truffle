use proc_macro::{Span, TokenStream};
use quote::quote;
use std::sync::LazyLock;
use syn::{
    Error, Token,
    parse::{Parse, discouraged::Speculative},
};
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
    ty: Option<syn::Type>,
    sql_lit: syn::LitStr,
    placeholders: Vec<syn::Expr>,
}

impl Parse for QueryInput {
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

        Ok(QueryInput {
            ty,
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

            // let conversion = match sql_type {
            //     SqlType::SmallInt => quote! { let #binding: i16 = (#rust_expr); },
            //     SqlType::Integer => quote! { let #binding: i32 = (#rust_expr); },
            //     SqlType::BigInt => quote! { let #binding: i64 = (#rust_expr); },
            //     SqlType::Float => quote! { let #binding: f32 = (#rust_expr); },
            //     SqlType::Double => quote! { let #binding: f64 = (#rust_expr); },
            //     SqlType::Text => quote! { let #binding: &str = (#rust_expr); },
            //     SqlType::Boolean => quote! { let #binding: bool = (#rust_expr); },
            //     _ => quote! { let #binding = #rust_expr; },
            // };

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

    // Create type checks.
    let type_checks: Vec<_> = resolve
        .input_iter()
        .zip(parsed.placeholders.iter())
        .enumerate()
        .map(|(i, (sql_type, rust_type))| {
            let check_fn = syn::Ident::new(&format!("_check_param_{i}"), Span::call_site().into());

            match sql_type {
                SqlType::SmallInt => quote! {
                    fn #check_fn<T>() where T: Into<i16> {}
                    #check_fn::<#rust_type>();
                },
                SqlType::Integer => quote! {
                    fn #check_fn<T>() where T: Into<i32> {}
                    #check_fn::<#rust_type>();
                },
                SqlType::BigInt => quote! {
                    fn #check_fn<T>() where T: Into<i64> {}
                    #check_fn::<#rust_type>();
                },
                SqlType::Float => quote! {
                    fn #check_fn<T>() where T: Into<f32> {}
                    #check_fn::<#rust_type>();
                },
                SqlType::Double => quote! {
                    fn #check_fn<T>() where T: Into<f64> {}
                    #check_fn::<#rust_type>();
                },
                SqlType::Text => quote! {
                    fn #check_fn<T>() where T: ToString {}
                    #check_fn::<#rust_type>();
                },
                SqlType::Boolean => quote! {
                    fn #check_fn<T>() where T: Into<bool> {}
                    #check_fn::<#rust_type>();
                },
                _ => quote! {},
            }
        })
        .collect();

    // Run your SQL.
    match parsed.ty {
        Some(ty) => TokenStream::from(quote! {
            #(#type_checks)*
            sqlx::query_as::<_, #ty>(#sql)
        }),
        None => TokenStream::from(quote! {
            #(#type_checks)*
            sqlx::query_as(#sql)
        }),
    }
}

#[proc_macro]
pub fn query_scalar(input: TokenStream) -> TokenStream {
    let parsed = syn::parse_macro_input!(input as QueryInput);
    let sql = parsed.sql_lit.value();

    let mut sim = match SIMULATOR.as_ref() {
        Ok(simulator) => simulator.clone(),
        Err(e) => return e.to_compile_error().into(),
    };

    if let Err(e) = sim.execute(&sql) {
        return Error::new(parsed.sql_lit.span(), e.to_string())
            .to_compile_error()
            .into();
    }

    // Run your SQL.
    match parsed.ty {
        Some(ty) => TokenStream::from(quote! {
            sqlx::query_scalar::<_, #ty>(#sql)
        }),
        None => TokenStream::from(quote! {
            sqlx::query_scalar(#sql)
        }),
    }
}
