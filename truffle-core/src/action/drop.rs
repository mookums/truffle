use sqlparser::ast::{ObjectName, ObjectType};
use tracing::{debug, warn};

use crate::{Error, Simulator, object_name_to_strings, table::Constraint};

impl Simulator {
    pub(crate) fn drop(
        &mut self,
        object_type: &ObjectType,
        names: Vec<ObjectName>,
    ) -> Result<(), Error> {
        if matches!(object_type, ObjectType::Table) {
            for name in names.iter().flat_map(object_name_to_strings) {
                // Ensure that the table being dropped exists.
                if !self.tables.contains_key(&name) {
                    return Err(Error::TableDoesntExist(name.to_string()));
                }

                // Ensure that this table isn't a constraint on another table.
                for (_, constraints) in self.tables.iter().flat_map(|t| &t.1.constraints) {
                    for constraint in constraints {
                        if let Constraint::ForeignKey { foreign_table, .. } = constraint {
                            if foreign_table == &name {
                                return Err(Error::ForeignKeyConstraint(name.to_string()));
                            }
                        }
                    }
                }

                debug!(name = %name, "Dropping Table");
                self.tables.remove(&name);
            }
        } else {
            warn!(object = %object_type, "Unsupported Drop");
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::*;

    #[test]
    fn drop_table_success() {
        let mut sim = Simulator::new(GenericDialect {});
        sim.execute("create table person (id uuid, name text, weight real);")
            .unwrap();
        assert_eq!(sim.tables.len(), 1);
        sim.execute("drop table person;").unwrap();
        assert_eq!(sim.tables.len(), 0);
    }

    #[test]
    fn drop_table_doesnt_exist() {
        let mut sim = Simulator::new(GenericDialect {});
        assert_eq!(
            sim.execute("drop table person;"),
            Err(Error::TableDoesntExist("person".to_string()))
        );
    }

    #[test]
    fn drop_table_foreign_key_constaint() {
        let mut sim = Simulator::new(GenericDialect {});
        sim.execute("create table person (id int primary key, name text)")
            .unwrap();
        sim.execute("create table order (id int primary key, person_id int references person(id))")
            .unwrap();

        assert_eq!(
            sim.execute("drop table person"),
            Err(Error::ForeignKeyConstraint("person".to_string()))
        )
    }
}
