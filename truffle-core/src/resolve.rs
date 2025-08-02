use indexmap::IndexMap;

use crate::column::Column;

#[derive(Debug, Clone, Default)]
pub struct ResolvedQuery {
    pub inputs: IndexMap<String, Column>,
    // outputs: IndexMap<String, SqlType>,
}
