use std::{
    collections::{HashMap, hash_map},
    slice,
};

use crate::ty::SqlType;
use itertools::Itertools;

#[derive(Debug, Clone, Default, PartialEq, Eq, Hash)]
pub struct ResolveOutputKey {
    pub qualifier: Option<String>,
    pub name: String,
}

impl ResolveOutputKey {
    pub fn new(qualifier: Option<String>, name: impl ToString) -> Self {
        Self {
            qualifier: qualifier.map(|q| q.to_string()),
            name: name.to_string(),
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ResolvedQuery {
    pub inputs: Vec<SqlType>,
    pub outputs: HashMap<ResolveOutputKey, SqlType>,
}

impl ResolvedQuery {
    pub fn get_input(&self, index: usize) -> Option<&SqlType> {
        self.inputs.get(index)
    }

    pub fn insert_input(&mut self, placeholder: impl AsRef<str>, sql_type: SqlType) {
        if let Some(index) = parse_placeholder(placeholder) {
            let true_index = (index - 1).min(self.inputs.len());
            self.inputs.insert(true_index, sql_type);
        } else {
            self.inputs.push(sql_type);
        }
    }

    pub fn insert_input_at(&mut self, index: usize, sql_type: SqlType) {
        self.inputs.insert(index.min(self.inputs.len()), sql_type);
    }

    pub fn input_iter(&self) -> slice::Iter<'_, SqlType> {
        self.inputs.iter()
    }

    pub fn insert_output(&mut self, key: ResolveOutputKey, sql_type: SqlType) {
        _ = self.outputs.insert(key, sql_type)
    }

    pub fn get_output(&self, key: &ResolveOutputKey) -> Option<&SqlType> {
        self.outputs.get(key)
    }

    /// This will attempt to match the name with the output columns.
    ///
    /// If there are multiple output columns with the same name, it will return None.
    /// If there are no output columns with the name, it will return None.
    pub fn get_output_with_name(&self, name: impl AsRef<str>) -> Option<&SqlType> {
        self.outputs
            .iter()
            .filter(|o| o.0.name == name.as_ref())
            .at_most_one()
            .ok()
            .flatten()
            .map(|c| c.1)
    }

    pub fn output_iter(&self) -> hash_map::Iter<'_, ResolveOutputKey, SqlType> {
        self.outputs.iter()
    }
}

fn parse_placeholder(placeholder: impl AsRef<str>) -> Option<usize> {
    let place = placeholder.as_ref();
    if place == "?" {
        return None;
    }
    place.split_at(1).1.parse().ok()
}

#[cfg(test)]
mod tests {
    use crate::resolve::parse_placeholder;

    #[test]
    fn parse_unnumbered_placeholder() {
        let placeholder = "?";
        assert_eq!(parse_placeholder(placeholder), None)
    }

    #[test]
    fn parse_numbered_placeholder() {
        let placeholder = "$5";
        assert_eq!(parse_placeholder(placeholder), Some(5))
    }
}
