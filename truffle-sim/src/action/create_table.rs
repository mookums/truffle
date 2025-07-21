use sqlparser::ast::CreateTable;
use tracing::debug;

use crate::{Error, Simulator, column::Column, object_name_to_strings, table::Table};

pub fn handle_create_table(sim: &mut Simulator, create_table: CreateTable) -> Result<(), Error> {
    let name = object_name_to_strings(&create_table.name).pop().unwrap();

    // Ensure that this table doesn't already exist.
    if !create_table.if_not_exists && sim.tables.contains_key(&name) {
        return Err(Error::TableAlreadyExists(name));
    }

    let mut table = Table::default();
    for column in create_table.columns {
        let col_name = column.name.value.clone();
        let column = Column::new(column.data_type.into());

        // Ensure that this column doen't already exist.
        if table.columns.contains_key(&col_name) {
            return Err(Error::ColumnAlreadyExists(col_name));
        }

        table.columns.insert(col_name, column);
    }

    debug!(name = %name, "Creating Table");
    sim.tables.insert(name, table);

    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::{column::ColumnKind, *};

    #[test]
    fn invalid_sql() {
        let mut sim = Simulator::new(Box::new(GenericDialect {}));
        assert!(matches!(
            sim.execute("create eveyrthing (id int);"),
            Err(Error::Parsing(_))
        ))
    }

    #[test]
    fn create_table() {
        let mut sim = Simulator::new(Box::new(GenericDialect {}));
        sim.execute("create table abc (id int);").unwrap();
        assert_eq!(sim.tables.len(), 1);
        assert!(sim.tables.contains_key("abc"));
    }

    #[test]
    fn create_table_duplicate() {
        let mut sim = Simulator::new(Box::new(GenericDialect {}));
        sim.execute("create table abc (id int);").unwrap();
        assert_eq!(sim.tables.len(), 1);
        assert!(sim.tables.contains_key("abc"));
        assert_eq!(
            sim.execute("create table abc (id integer);"),
            Err(Error::TableAlreadyExists("abc".to_string()))
        )
    }

    #[test]
    fn create_table_if_not_exists_duplicate() {
        let mut sim = Simulator::new(Box::new(GenericDialect {}));
        sim.execute("create table abc (id int);").unwrap();
        assert_eq!(sim.tables.len(), 1);
        assert!(sim.tables.contains_key("abc"));
        sim.execute("create table if not exists abc (id int);")
            .unwrap();
        assert_eq!(sim.tables.len(), 1);
    }

    #[test]
    fn create_table_columns() {
        let mut sim = Simulator::new(Box::new(GenericDialect {}));
        sim.execute("create table person (id uuid, name text, weight real);")
            .unwrap();
        assert_eq!(sim.tables.len(), 1);
        let table = sim.tables.get("person").unwrap();
        assert_eq!(
            table.columns.get("id").unwrap().get_kind(),
            &ColumnKind::Uuid
        );
        assert_eq!(
            table.columns.get("name").unwrap().get_kind(),
            &ColumnKind::Text
        );
        assert_eq!(
            table.columns.get("weight").unwrap().get_kind(),
            &ColumnKind::Float
        );
    }

    #[test]
    fn create_table_columns_duplicate() {
        let mut sim = Simulator::new(Box::new(GenericDialect {}));
        assert_eq!(
            sim.execute("create table person (id uuid, id int);"),
            Err(Error::ColumnAlreadyExists("id".to_string()))
        );
    }
}
