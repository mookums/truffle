use serde::Deserialize;

#[derive(Debug, Copy, Clone, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum DialectKind {
    Generic,
    Ansi,
    Sqlite,
    Postgres,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct Config {
    pub dialect: DialectKind,
    pub migrations: String,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            dialect: DialectKind::Generic,
            migrations: "./migrations".into(),
        }
    }
}
