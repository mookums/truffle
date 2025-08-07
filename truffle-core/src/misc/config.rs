use serde::Deserialize;

#[derive(Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum DialectKind {
    Generic,
    Ansi,
    Sqlite,
    Postgres,
}

#[derive(Deserialize)]
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
