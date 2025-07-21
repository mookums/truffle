use sqlparser::ast::{ColumnOption, CreateTable, TableConstraint};
use tracing::debug;

use crate::{
    Error, Simulator,
    column::{Column, ColumnType},
    object_name_to_strings,
    table::{Constraint, Table},
};

pub fn handle_create_table(sim: &mut Simulator, create_table: CreateTable) -> Result<(), Error> {
    let name = object_name_to_strings(&create_table.name).pop().unwrap();

    // Ensure that this table doesn't already exist.
    if !create_table.if_not_exists && sim.tables.contains_key(&name) {
        return Err(Error::TableAlreadyExists(name));
    }

    let mut table = Table::default();
    for column in create_table.columns {
        let column_name = &column.name.value;
        let mut nullable = false;
        let mut default = false;
        let kind: ColumnType = column.data_type.into();

        // Handle options/constraints on a column level.
        for option in column.options {
            match option.option {
                ColumnOption::Null => {
                    nullable = true;
                }
                ColumnOption::NotNull => {
                    nullable = false;
                }
                ColumnOption::Default(expr) => {
                    // TODO: Verify the type of the expr is correct.
                    // TODO: Verify that the expr is correct.
                    _ = expr;
                    default = true;
                }
                ColumnOption::Unique { is_primary, .. } => {
                    table.insert_constraint(&[column_name], Constraint::Unique);
                    if is_primary {
                        table.insert_constraint(&[column_name], Constraint::PrimaryKey);
                    }
                }
                ColumnOption::ForeignKey {
                    foreign_table,
                    referred_columns,
                    ..
                } => {
                    let foreign_table_name = object_name_to_strings(&foreign_table)
                        .first()
                        .unwrap()
                        .to_string();

                    // Verify that foreign table exists.
                    let f_table = sim
                        .get_table(&foreign_table_name)
                        .ok_or_else(|| Error::TableDoesntExist(foreign_table_name.to_string()))?;

                    let mut foreign_columns = vec![];
                    for ref_column in referred_columns {
                        let ref_column_name = &ref_column.value;

                        // Verify that foreign column exists.
                        let f_column = f_table
                            .get_column(ref_column_name)
                            .ok_or_else(|| Error::ColumnDoesntExist(ref_column_name.to_string()))?;

                        // Verify that the foreign column is UNIQUE.
                        if !f_table.is_unique(&[ref_column_name]) {
                            return Err(Error::ForeignKeyConstraint(ref_column_name.to_string()));
                        }

                        // Verify that they are of the same type.
                        if kind != f_column.ty {
                            return Err(Error::TypeMismatch {
                                expected: f_column.ty.clone(),
                                got: kind,
                            });
                        }

                        foreign_columns.push(ref_column_name.to_string());
                    }

                    table.insert_constraint(
                        &[column_name],
                        Constraint::ForeignKey {
                            foreign_table: foreign_table_name,
                            foreign_columns,
                        },
                    );
                }
                _ => {
                    return Err(Error::Unsupported(format!(
                        "Unsupported option in CREATE TABLE: {option:#?}"
                    )));
                }
            }
        }

        let col = Column {
            ty: kind,
            nullable,
            default,
        };

        // Ensure that this column doen't already exist.
        if table.columns.contains_key(column_name) {
            return Err(Error::ColumnAlreadyExists(column_name.to_string()));
        }

        table.columns.insert(column_name.to_string(), col);
    }

    // Handle table level constraints.
    for constraint in create_table.constraints {
        match constraint {
            TableConstraint::ForeignKey {
                columns,
                foreign_table,
                referred_columns,
                ..
            } => {
                // TODO: Properly support foreign key names.

                let foreign_table_name = object_name_to_strings(&foreign_table)
                    .first()
                    .unwrap()
                    .to_string();

                let f_table = sim
                    .get_table(&foreign_table_name)
                    .ok_or_else(|| Error::TableDoesntExist(foreign_table_name.clone()))?;

                let local_column_names: Vec<String> =
                    columns.iter().map(|c| c.value.to_string()).collect();

                let foreign_column_names: Vec<String> = referred_columns
                    .iter()
                    .map(|c| c.value.to_string())
                    .collect();

                for (local_col_name, foreign_col_name) in
                    local_column_names.iter().zip(foreign_column_names.iter())
                {
                    let local_column = table
                        .get_column(local_col_name)
                        .ok_or_else(|| Error::ColumnDoesntExist(local_col_name.to_string()))?;

                    let foreign_column = f_table
                        .get_column(foreign_col_name)
                        .ok_or_else(|| Error::ColumnDoesntExist(foreign_col_name.to_string()))?;

                    if local_column.ty != foreign_column.ty {
                        return Err(Error::TypeMismatch {
                            expected: foreign_column.ty.clone(),
                            got: local_column.ty.clone(),
                        });
                    }
                }

                if !f_table.is_unique(&foreign_column_names) {
                    return Err(Error::ForeignKeyConstraint(format!(
                        "({})",
                        foreign_column_names.join(", ")
                    )));
                }

                table.insert_constraint(
                    &local_column_names,
                    Constraint::ForeignKey {
                        foreign_table: foreign_table_name,
                        foreign_columns: foreign_column_names,
                    },
                );
            }
            _ => {
                return Err(Error::Unsupported(format!(
                    "Unsupported table constraint on CREATE TABLE: {constraint:#?}"
                )));
            }
        }
    }

    debug!(name = %name, "Creating Table");
    sim.tables.insert(name, table);

    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::{column::ColumnType, *};

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
            &ColumnType::Uuid
        );
        assert_eq!(
            table.columns.get("name").unwrap().get_kind(),
            &ColumnType::Text
        );
        assert_eq!(
            table.columns.get("weight").unwrap().get_kind(),
            &ColumnType::Float
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

    #[test]
    fn create_table_with_col_foreign_key() {
        let mut sim = Simulator::new(Box::new(GenericDialect {}));
        sim.execute("create table person (id uuid primary key, name text unique, phone int);")
            .unwrap();
        sim.execute(
            r#"
                create table order(
                    order_id uuid primary key,
                    person_id uuid references person(id)
                );
            "#,
        )
        .unwrap();
    }

    #[test]
    fn create_table_with_col_foreign_key_table_doesnt_exist() {
        let mut sim = Simulator::new(Box::new(GenericDialect {}));
        assert_eq!(
            sim.execute(
                r#"
                create table order(
                    order_id uuid primary key,
                    person_id uuid references person(id)
                );
            "#,
            ),
            Err(Error::TableDoesntExist("person".to_string()))
        );
    }

    #[test]
    fn create_table_with_col_foreign_key_column_doesnt_exist() {
        let mut sim = Simulator::new(Box::new(GenericDialect {}));
        sim.execute("create table person (id uuid primary key, name text unique, phone int);")
            .unwrap();
        assert_eq!(
            sim.execute(
                r#"
                create table order(
                    order_id uuid primary key,
                    person_id uuid references person(weight)
                );
            "#,
            ),
            Err(Error::ColumnDoesntExist("weight".to_string()))
        );
        assert_eq!(
            sim.execute(
                r#"
                create table order(
                    order_id uuid primary key,
                    person_id uuid references person(weight)
                );
            "#,
            ),
            Err(Error::ColumnDoesntExist("weight".to_string()))
        );
    }

    #[test]
    fn create_table_with_col_foreign_key_type_mismatch() {
        let mut sim = Simulator::new(Box::new(GenericDialect {}));
        sim.execute("create table person (id uuid primary key, name text unique, phone int);")
            .unwrap();

        assert_eq!(
            sim.execute(
                r#"
                create table order(
                    order_id uuid primary key,
                    person_id text references person(id)
                );
            "#,
            ),
            Err(Error::TypeMismatch {
                expected: ColumnType::Uuid,
                got: ColumnType::Text
            })
        );
    }

    #[test]
    fn create_table_with_table_foreign_key() {
        let mut sim = Simulator::new(Box::new(GenericDialect {}));
        sim.execute("create table person (id uuid primary key, name text unique, phone int);")
            .unwrap();
        sim.execute(
            r#"
                create table order(
                    order_id uuid primary key,
                    person_id uuid,
                    foreign key (person_id) references person(id)
                );
            "#,
        )
        .unwrap();
    }

    #[test]
    fn create_table_with_table_foreign_key_table_doesnt_exist() {
        let mut sim = Simulator::new(Box::new(GenericDialect {}));
        assert_eq!(
            sim.execute(
                r#"
                create table order(
                    order_id uuid primary key,
                    person_id uuid,
                    foreign key(person_id) references person(id)
                );
            "#,
            ),
            Err(Error::TableDoesntExist("person".to_string()))
        );
    }

    #[test]
    fn create_table_with_table_foreign_key_column_doesnt_exist() {
        let mut sim = Simulator::new(Box::new(GenericDialect {}));
        sim.execute("create table person (id uuid primary key, name text unique, phone int);")
            .unwrap();
        assert_eq!(
            sim.execute(
                r#"
                create table order(
                    order_id uuid primary key,
                    person_id uuid,
                    foreign key (person_id) references person(weight)
                );
            "#,
            ),
            Err(Error::ColumnDoesntExist("weight".to_string()))
        );
        assert_eq!(
            sim.execute(
                r#"
                create table order(
                    order_id uuid primary key,
                    person_id uuid,
                    foreign key (weight) references person(id)
                );
            "#,
            ),
            Err(Error::ColumnDoesntExist("weight".to_string()))
        );
    }

    #[test]
    fn create_table_with_table_foreign_key_type_mismatch() {
        let mut sim = Simulator::new(Box::new(GenericDialect {}));
        sim.execute("create table person (id uuid primary key, name text unique, phone int);")
            .unwrap();

        assert_eq!(
            sim.execute(
                r#"
                create table order(
                    order_id uuid primary key,
                    person_id text,
                    foreign key (person_id) references person(id)
                );
            "#,
            ),
            Err(Error::TypeMismatch {
                expected: ColumnType::Uuid,
                got: ColumnType::Text
            })
        );
    }
}
