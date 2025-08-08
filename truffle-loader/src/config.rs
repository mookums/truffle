use std::{fs, path::Path};
use truffle::Config;

pub fn load_config() -> Result<Config, String> {
    let manifest_str =
        std::env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR must be defined");
    let manifest_dir = Path::new(&manifest_str);
    let config_file = manifest_dir
        .join("truffle.toml")
        .to_str()
        .unwrap()
        .to_string();

    let config = if Path::new(&config_file).exists() {
        let config_data = fs::read_to_string(&config_file)
            .map_err(|e| format!("Failed to read config file '{config_file}: {e}"))?;

        let config: Config = toml::from_str(&config_data)
            .map_err(|e| format!("Failed to parse config file: {e}"))?;

        config
    } else {
        Config::default()
    };

    Ok(config)
}
