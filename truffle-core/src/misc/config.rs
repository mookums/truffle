use serde::Deserialize;

#[derive(Deserialize)]
pub enum DialectKind {
    Generic,
    Ansi,
    Sqlite,
    Postgres,
}

#[derive(Deserialize)]
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
