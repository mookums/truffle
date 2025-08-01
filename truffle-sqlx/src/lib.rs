use proc_macro::{Span, TokenStream};
use quote::quote;
use std::{
    fs,
    path::{Path, PathBuf},
    sync::LazyLock,
};
use syn::{
    Error, LitStr, Token,
    parse::{Parse, discouraged::Speculative},
    parse_macro_input,
};
use truffle_sim::{Dialect, Simulator};

static MIGRATIONS: LazyLock<Result<Vec<(PathBuf, String)>, String>> =
    LazyLock::new(load_migrations);

fn load_migrations() -> Result<Vec<(PathBuf, String)>, String> {
    let manifest_str = std::env::var("CARGO_MANIFEST_DIR").unwrap();
    let manifest_dir = Path::new(&manifest_str);
    let migrations_dir = manifest_dir
        .join("migrations")
        .to_str()
        .unwrap()
        .to_string();

    let mut migration_contents = Vec::new();

    if Path::new(&migrations_dir).exists() {
        let entries_result = fs::read_dir(&migrations_dir);
        let entries = match entries_result {
            Ok(entries) => entries,
            Err(e) => {
                return Err(format!(
                    "Failed to read migrations directory '{migrations_dir}': {e}"
                ));
            }
        };

        let mut migration_paths: Vec<_> = Vec::new();
        for entry in entries {
            let entry = match entry {
                Ok(entry) => entry,
                Err(e) => {
                    return Err(format!("Failed to read directory entry in migrations: {e}"));
                }
            };

            let path = entry.path();
            if path.is_file() && path.extension().map(|ext| ext == "sql").unwrap_or_default() {
                migration_paths.push(path);
            }
        }

        // Migrations will be processed in alphabetical order.
        migration_paths.sort();

        for migration_path in migration_paths {
            let content = match fs::read_to_string(&migration_path) {
                Ok(content) => content,
                Err(e) => {
                    return Err(format!(
                        "Failed to read Migration file '{migration_path:?}': {e}"
                    ));
                }
            };

            migration_contents.push((migration_path, content));
        }
    }

    Ok(migration_contents)
}

fn apply_migrations<D: Dialect>(sim: &mut Simulator<D>) -> Option<TokenStream> {
    let migrations = match MIGRATIONS.as_ref() {
        Ok(migrations) => migrations,
        Err(e) => {
            return Some(
                Error::new(Span::call_site().into(), e)
                    .to_compile_error()
                    .into(),
            );
        }
    };

    for migration_pair in migrations {
        if let Err(e) = sim.execute(&migration_pair.1) {
            return Some(
                Error::new(
                    Span::call_site().into(),
                    format!("Migration {:#?}: {}", migration_pair.0, e),
                )
                .to_compile_error()
                .into(),
            );
        }
    }

    None
}

/// Validates the syntax and semantics of your SQL at compile time.
#[proc_macro]
pub fn query(input: TokenStream) -> TokenStream {
    let sql_lit = parse_macro_input!(input as LitStr);
    let sql = sql_lit.value();

    // Operate on the root of this Crate.
    use truffle_sim::{GenericDialect, Simulator};
    let mut sim = Simulator::new(GenericDialect {});

    if let Some(e) = apply_migrations(&mut sim) {
        return e;
    }

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
///
///
#[proc_macro]
pub fn query_as(input: TokenStream) -> TokenStream {
    let parsed = syn::parse_macro_input!(input as QueryInput);

    // Operate on the root of this Crate.
    use truffle_sim::{GenericDialect, Simulator};
    let mut sim = Simulator::new(GenericDialect {});

    if let Some(e) = apply_migrations(&mut sim) {
        return e;
    }

    let sql = parsed.sql_lit.value();
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

    // Operate on the root of this Crate.
    use truffle_sim::{GenericDialect, Simulator};
    let mut sim = Simulator::new(GenericDialect {});

    if let Some(e) = apply_migrations(&mut sim) {
        return e;
    }

    let sql = parsed.sql_lit.value();
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
