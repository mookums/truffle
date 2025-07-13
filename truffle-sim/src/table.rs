use std::collections::{HashMap, HashSet, hash_map::Entry};

use sqlparser::ast::ReferentialAction;

use crate::column::Column;

#[derive(Debug, Hash, PartialEq, Eq, Default)]
pub enum OnAction {
    #[default]
    Nothing,
    Restrict,
    Cascade,
    SetNull,
    SetDefault,
}

impl From<ReferentialAction> for OnAction {
    fn from(value: ReferentialAction) -> Self {
        match value {
            ReferentialAction::Restrict => OnAction::Restrict,
            ReferentialAction::Cascade => OnAction::Cascade,
            ReferentialAction::SetNull => OnAction::SetNull,
            ReferentialAction::NoAction => OnAction::Nothing,
            ReferentialAction::SetDefault => OnAction::SetDefault,
        }
    }
}

#[derive(Debug, Hash, PartialEq, Eq)]
pub enum Constraint {
    ForeignKey {
        foreign_table: String,
        foreign_columns: Vec<String>,
        on_delete: OnAction,
        on_update: OnAction,
    },
    Unique,
    PrimaryKey,
    Index,
}

impl Constraint {
    pub fn is_unique(constraints: &HashSet<Constraint>) -> bool {
        constraints.iter().any(|c| matches!(c, Constraint::Unique))
    }
}

#[derive(Debug, Default)]
pub struct Table {
    pub columns: HashMap<String, Column>,
    pub constraints: HashMap<String, HashSet<Constraint>>,
}

impl Table {
    pub fn create_compound_key(columns: &[String]) -> String {
        columns.join(":").to_lowercase()
    }

    pub fn has_column(&self, name: &str) -> bool {
        self.columns.contains_key(name)
    }

    pub fn get_column(&self, name: &str) -> Option<&Column> {
        self.columns.get(name)
    }

    pub fn insert_constraint(&mut self, columns: &[impl ToString], constraint: Constraint) {
        let columns: Vec<String> = columns.iter().map(|c| c.to_string()).collect();
        let key = Table::create_compound_key(&columns);

        match self.constraints.entry(key) {
            Entry::Vacant(e) => {
                e.insert(HashSet::from([constraint]));
            }
            Entry::Occupied(mut e) => {
                assert!(e.get_mut().insert(constraint));
            }
        };
    }

    pub fn get_all_constraints(&self) -> &HashMap<String, HashSet<Constraint>> {
        &self.constraints
    }

    pub fn get_constraints(&self, columns: &[impl ToString]) -> Option<&HashSet<Constraint>> {
        let columns: Vec<String> = columns.iter().map(|c| c.to_string()).collect();
        let key = Table::create_compound_key(&columns);
        self.constraints.get(&key)
    }

    pub fn is_unique(&self, columns: &[impl ToString]) -> bool {
        let columns: Vec<String> = columns.iter().map(|c| c.to_string()).collect();
        let key = Table::create_compound_key(&columns);
        self.constraints
            .get(&key)
            .is_some_and(|c| c.iter().any(|o| matches!(o, Constraint::Unique)))
    }
}
