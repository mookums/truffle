use std::collections::HashMap;

use crate::column::Column;

#[derive(Debug, Default)]
pub struct Table {
    pub columns: HashMap<String, Column>,
}
