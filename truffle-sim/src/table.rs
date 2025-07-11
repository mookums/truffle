use std::collections::HashMap;

use serde::Serialize;

use crate::column::Column;

#[derive(Debug, Default, Serialize)]
pub struct Table {
    pub columns: HashMap<String, Column>,
}
