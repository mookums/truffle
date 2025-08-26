use sqlparser::ast::{ObjectName, ObjectType};
use tracing::{debug, warn};

use crate::{Error, Simulator, object_name_to_strings, resolve::ResolvedQuery, table::Constraint};

impl Simulator {
    pub(crate) fn drop(
        &mut self,
        object_type: &ObjectType,
        names: Vec<ObjectName>,
    ) -> Result<ResolvedQuery, Error> {
        if matches!(object_type, ObjectType::Table) {
            for name in names.iter().flat_map(object_name_to_strings) {
                // Ensure that the table being dropped exists.
                if !self.tables.contains_key(&name) {
                    return Err(Error::TableDoesntExist(name.to_string()));
                }

                // Ensure that this table isn't a constraint on another table.
                for (_, constraints) in self.tables.iter().flat_map(|t| &t.1.constraints) {
                    for constraint in constraints {
                        if let Constraint::ForeignKey { foreign_table, .. } = constraint
                            && foreign_table == &name
                        {
                            return Err(Error::ForeignKeyConstraint(name.to_string()));
                        }
                    }
                }

                debug!(name = %name, "Dropping Table");
                self.tables.remove(&name);
            }
        } else {
            warn!(object = %object_type, "Unsupported Drop");
        }

        Ok(ResolvedQuery::default())
    }
}
