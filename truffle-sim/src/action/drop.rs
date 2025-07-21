use sqlparser::ast::{ObjectName, ObjectType};
use tracing::{debug, warn};

use crate::{Error, Simulator, object_name_to_strings};

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
                    return Err(Error::TableDoesntExist(name));
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
        let mut sim = Simulator::new(Box::new(GenericDialect {}));
        sim.execute("create table person (id uuid, name text, weight real);")
            .unwrap();
        assert_eq!(sim.tables.len(), 1);
        sim.execute("drop table person;").unwrap();
        assert_eq!(sim.tables.len(), 0);
    }

    #[test]
    fn drop_table_doesnt_exist() {
        let mut sim = Simulator::new(Box::new(GenericDialect {}));
        assert_eq!(
            sim.execute("drop table person;"),
            Err(Error::TableDoesntExist("person".to_string()))
        );
    }
}
