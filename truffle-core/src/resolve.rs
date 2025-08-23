use std::fmt::Display;

use crate::{column::Column, ty::SqlType};
use indexmap::IndexMap;
use itertools::Itertools;

#[derive(Debug, Clone, Default, PartialEq, Eq, Hash)]
pub struct ColumnRef {
    pub qualifier: Option<String>,
    pub name: String,
}

impl ColumnRef {
    pub fn new(qualifier: Option<String>, name: impl ToString) -> Self {
        Self {
            qualifier: qualifier.map(|q| q.to_string()),
            name: name.to_string(),
        }
    }
}

impl Display for ColumnRef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.qualifier {
            Some(qualifier) => write!(f, "{}.{}", qualifier, self.name),
            None => write!(f, "{}", self.name),
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ResolvedQuery {
    // TODO: Consider logging if the query will return One or Many result columns?
    pub inputs: Vec<Column>,
    pub outputs: IndexMap<ColumnRef, Column>,
}

impl Display for ResolvedQuery {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Format inputs
        writeln!(f, "Inputs:")?;
        if self.inputs.is_empty() {
            writeln!(f, "  (none)")?;
        } else {
            for (i, column) in self.inputs.iter().enumerate() {
                writeln!(f, "  ${}: {column}", i + 1)?;
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
    pub fn get_input(&self, index: usize) -> Option<&Column> {
        self.inputs.get(index)
    }

    pub fn insert_input(&mut self, placeholder: impl AsRef<str>, col: Column) {
        if let Some(index) = parse_placeholder(placeholder) {
            let idx = index - 1;

            if idx < self.inputs.len() {
                // Replace existing entry at index.
                //
                // TODO: Ensure that the sql types here are identical INSTEAD of replacing it.
                // It should then error if they are different types as they can't share a placeholder.
                _ = std::mem::replace(&mut self.inputs[idx], col);
            } else {
                // Extend the Vec then insert.
                self.inputs.resize_with(index, || Column {
                    ty: SqlType::Unknown("".to_string()),
                    nullable: false,
                    default: false,
                });
                self.inputs[idx] = col;
            }
        } else {
            self.inputs.push(col);
        }
    }

    pub fn insert_input_at(&mut self, index: usize, col: Column) {
        self.inputs.insert(index.min(self.inputs.len()), col);
    }

    pub fn insert_output(&mut self, key: ColumnRef, col: Column) {
        _ = self.outputs.insert(key, col)
    }

    pub fn get_output(&self, qualifier: impl ToString, column: impl ToString) -> Option<&Column> {
        self.outputs.get(&ColumnRef {
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
