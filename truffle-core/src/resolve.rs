use std::{
    collections::{HashMap, hash_map},
    fmt::Display,
    slice,
};

use crate::{column::Column, ty::SqlType};
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

impl Display for ResolveOutputKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.qualifier {
            Some(qualifier) => write!(f, "{}.{}", qualifier, self.name),
            None => write!(f, "{}", self.name),
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ResolvedQuery {
    pub inputs: Vec<SqlType>,
    pub outputs: HashMap<ResolveOutputKey, Column>,
}

impl Display for ResolvedQuery {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Format inputs
        writeln!(f, "Inputs:")?;
        if self.inputs.is_empty() {
            writeln!(f, "  (none)")?;
        } else {
            for (i, input) in self.inputs.iter().enumerate() {
                writeln!(f, "  ${}: {}", i + 1, input)?;
            }
        }

        // Format outputs
        writeln!(f, "Outputs:")?;
        if self.outputs.is_empty() {
            writeln!(f, "  (none)")?;
        } else {
            for (key, column) in &self.outputs {
                writeln!(f, "  {key}: {column}")?;
            }
        }

        Ok(())
    }
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

    pub fn insert_output(&mut self, key: ResolveOutputKey, col: Column) {
        _ = self.outputs.insert(key, col)
    }

    pub fn get_output(&self, qualifier: impl ToString, column: impl ToString) -> Option<&Column> {
        self.outputs.get(&ResolveOutputKey {
            qualifier: Some(qualifier.to_string()),
            name: column.to_string(),
        })
    }

    /// This will attempt to match the name with the output columns.
    ///
    /// If there are multiple output columns with the same name, it will return None.
    /// If there are no output columns with the name, it will return None.
    pub fn get_output_with_name(&self, name: impl AsRef<str>) -> Option<&Column> {
        self.outputs
            .iter()
            .filter(|o| o.0.name == name.as_ref())
            .at_most_one()
            .ok()
            .flatten()
            .map(|c| c.1)
    }

    pub fn output_iter(&self) -> hash_map::Iter<'_, ResolveOutputKey, Column> {
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
