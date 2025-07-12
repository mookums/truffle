use proc_macro::TokenStream;
use std::{fs, path::Path};
use syn::{Error, LitStr, parse_macro_input};

/// Validates the syntax and semantics of your SQL at compile time.
#[proc_macro]
pub fn query(input: TokenStream) -> TokenStream {
    let sql_lit = parse_macro_input!(input as LitStr);
    let sql = sql_lit.value();

    // Operate on the root of this Crate.
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
                return Error::new(
                    sql_lit.span(),
                    format!("Failed to read migrations directory '{migrations_dir}': {e}"),
                )
                .to_compile_error()
                .into();
            }
        };

        let mut migration_paths: Vec<_> = Vec::new();
        for entry in entries {
            let entry = match entry {
                Ok(entry) => entry,
                Err(e) => {
                    return Error::new(
                        sql_lit.span(),
                        format!("Failed to read directory entry in migrations: {e}"),
                    )
                    .to_compile_error()
                    .into();
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
                    return Error::new(
                        sql_lit.span(),
                        format!("Failed to read Migration file '{migration_path:?}': {e}"),
                    )
                    .to_compile_error()
                    .into();
                }
            };
            migration_contents.push((migration_path, content));
        }
    }

    use truffle_sim::{GenericDialect, Simulator};
    let mut sim = Simulator::new(Box::new(GenericDialect {}));

    // Run all of the migrations on the simulator.
    for migration_content in migration_contents.iter() {
        if let Err(e) = sim.execute(&migration_content.1) {
            return Error::new(
                sql_lit.span(),
                format!("Migration {:#?}: {}", migration_content.0, e),
            )
            .to_compile_error()
            .into();
        }
    }

    // Run your SQL.
    if let Err(e) = sim.execute(&sql) {
        return Error::new(sql_lit.span(), format!("Query: {e}"))
            .to_compile_error()
            .into();
    }

    TokenStream::default()
}
