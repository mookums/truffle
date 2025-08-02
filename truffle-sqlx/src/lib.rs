use proc_macro::{Span, TokenStream};
use quote::quote;
use std::sync::LazyLock;
use syn::{
    Error, LitStr, Token,
    parse::{Parse, discouraged::Speculative},
    parse_macro_input,
};
use truffle::Simulator;
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

/// Validates the syntax and semantics of your SQL at compile time.
#[proc_macro]
pub fn query(input: TokenStream) -> TokenStream {
    let sql_lit = parse_macro_input!(input as LitStr);
    let sql = sql_lit.value();

    let mut sim = match SIMULATOR.as_ref() {
        Ok(simulator) => simulator.clone(),
        Err(e) => return e.to_compile_error().into(),
    };

    // Run your SQL.
    if let Err(e) = sim.execute(&sql) {
        return Error::new(sql_lit.span(), e.to_string())
            .to_compile_error()
            .into();
    }

    TokenStream::from(quote! {
        sqlx::query(#sql)
    })
}

struct QueryInput {
    sql_lit: syn::LitStr,
    ty: Option<syn::Type>,
}

impl Parse for QueryInput {
    fn parse(input: syn::parse::ParseStream) -> syn::Result<Self> {
        let fork = input.fork();

        if let Ok(ty) = fork.parse::<syn::Type>() {
            if fork.parse::<Token![,]>().is_ok() {
                if let Ok(sql_lit) = fork.parse::<syn::LitStr>() {
                    input.advance_to(&fork);
                    return Ok(QueryInput {
                        sql_lit,
                        ty: Some(ty),
                    });
                }
            }
        }

        let sql_lit: LitStr = input.parse()?;
        Ok(QueryInput { sql_lit, ty: None })
    }
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

    if let Err(e) = sim.execute(&sql) {
        return Error::new(parsed.sql_lit.span(), e.to_string())
            .to_compile_error()
            .into();
    }

    // Run your SQL.
    match parsed.ty {
        Some(ty) => TokenStream::from(quote! {
            sqlx::query_as::<_, #ty>(#sql)
        }),
        None => TokenStream::from(quote! {
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
