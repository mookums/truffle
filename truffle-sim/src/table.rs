use std::collections::HashMap;

use serde::Serialize;

use crate::column::Column;

#[derive(Debug, Default, Serialize)]
pub struct Table {
    pub columns: HashMap<String, Column>,
}

impl Table {
    pub fn has_column(&self, name: &str) -> bool {
        self.columns.contains_key(name)
    }
}
