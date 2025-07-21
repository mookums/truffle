use proc_macro::TokenStream;
use quote::quote;
use std::{fs, path::Path};
use syn::{Error, LitStr, parse_macro_input};
use truffle_sim::Simulator;

fn apply_migrations(sql_lit: &LitStr, sim: &mut Simulator) -> Option<TokenStream> {
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
                return Some(
                    Error::new(
                        sql_lit.span(),
                        format!("Failed to read migrations directory '{migrations_dir}': {e}"),
                    )
                    .to_compile_error()
                    .into(),
                );
            }
        };

        let mut migration_paths: Vec<_> = Vec::new();
        for entry in entries {
            let entry = match entry {
                Ok(entry) => entry,
                Err(e) => {
                    return Some(
                        Error::new(
                            sql_lit.span(),
                            format!("Failed to read directory entry in migrations: {e}"),
                        )
                        .to_compile_error()
                        .into(),
                    );
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
                    return Some(
                        Error::new(
                            sql_lit.span(),
                            format!("Failed to read Migration file '{migration_path:?}': {e}"),
                        )
                        .to_compile_error()
                        .into(),
                    );
                }
            };
            migration_contents.push((migration_path, content));
        }
    }

    // Run all of the migrations on the simulator.
    for migration_content in migration_contents.iter() {
        if let Err(e) = sim.execute(&migration_content.1) {
            return Some(
                Error::new(
                    sql_lit.span(),
                    format!("Migration {:#?}: {}", migration_content.0, e),
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
    let mut sim = Simulator::new(Box::new(GenericDialect {}));

    if let Some(e) = apply_migrations(&sql_lit, &mut sim) {
        return e;
    }

    // Run your SQL.
    if let Err(e) = sim.execute(&sql) {
        return Error::new(sql_lit.span(), format!("{e}"))
            .to_compile_error()
            .into();
    }

    TokenStream::from(quote! {
        sqlx::query(#sql)
    })
}

#[proc_macro]
pub fn query_as(input: TokenStream) -> TokenStream {
    let sql_lit = parse_macro_input!(input as LitStr);
    let sql = sql_lit.value();

    // Operate on the root of this Crate.
    use truffle_sim::{GenericDialect, Simulator};
    let mut sim = Simulator::new(Box::new(GenericDialect {}));

    if let Some(e) = apply_migrations(&sql_lit, &mut sim) {
        return e;
    }

    // Run your SQL.
    if let Err(e) = sim.execute(&sql) {
        return Error::new(sql_lit.span(), format!("{e}"))
            .to_compile_error()
            .into();
    }

    TokenStream::from(quote! {
        sqlx::query_as(#sql)
    })
}

#[proc_macro]
pub fn query_scalar(input: TokenStream) -> TokenStream {
    let sql_lit = parse_macro_input!(input as LitStr);
    let sql = sql_lit.value();

    // Operate on the root of this Crate.
    use truffle_sim::{GenericDialect, Simulator};
    let mut sim = Simulator::new(Box::new(GenericDialect {}));

    if let Some(e) = apply_migrations(&sql_lit, &mut sim) {
        return e;
    }

    // Run your SQL.
    if let Err(e) = sim.execute(&sql) {
        return Error::new(sql_lit.span(), format!("{e}"))
            .to_compile_error()
            .into();
    }

    TokenStream::from(quote! {
        sqlx::query_scalar(#sql)
    })
}
