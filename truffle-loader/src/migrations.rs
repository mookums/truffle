use std::{
    fs,
    path::{Path, PathBuf},
};

use truffle::{Simulator, config::Config};

pub fn load_migrations(config: &Config) -> Result<Vec<(PathBuf, String)>, String> {
    let manifest_str = std::env::var("CARGO_MANIFEST_DIR").unwrap();
    let manifest_dir = Path::new(&manifest_str);
    let migrations_dir = manifest_dir
        .join(&config.migrations)
        .to_str()
        .unwrap()
        .to_string();

    let mut migration_contents = Vec::new();

    if Path::new(&migrations_dir).exists() {
        let entries = fs::read_dir(&migrations_dir)
            .map_err(|e| format!("Failed to read migrations diretory '{migrations_dir}': {e}"))?;

        let mut migration_paths: Vec<_> = Vec::new();
        for entry in entries {
            let entry =
                entry.map_err(|e| format!("Failed to read directory entry in migrations: {e}"))?;

            let path = entry.path();
            if path.is_file() && path.extension().map(|ext| ext == "sql").unwrap_or_default() {
                migration_paths.push(path);
            }
        }

        // Migrations will be processed in alphabetical order.
        migration_paths.sort();

        for migration_path in migration_paths {
            let content = fs::read_to_string(&migration_path)
                .map_err(|e| format!("Failed to read Migration file '{migration_path:?}': {e}"))?;

            migration_contents.push((migration_path, content));
        }
    }

    Ok(migration_contents)
}

pub fn apply_migrations(
    sim: &mut Simulator,
    migrations: &[(PathBuf, String)],
) -> Result<(), String> {
    for migration_pair in migrations {
        if let Err(e) = sim.execute(&migration_pair.1) {
            return Err(format!("Migration {:#?}: {}", migration_pair.0, e));
        }
    }

    Ok(())
}
