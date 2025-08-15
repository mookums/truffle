use serde::Deserialize;

use crate::dialect::DialectKind;

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
