use sqlparser::{
    ast::{ColumnOption, CreateTable, ReferentialAction, TableConstraint},
    dialect::Dialect,
};
use tracing::debug;

use crate::{
    Error, Simulator,
    column::Column,
    expr::ColumnInferrer,
    object_name_to_strings,
    table::{Constraint, Table},
    ty::SqlType,
};

impl<D: Dialect> Simulator<D> {
    pub(crate) fn create_table(&mut self, create_table: CreateTable) -> Result<(), Error> {
        let name = object_name_to_strings(&create_table.name).pop().unwrap();

        // Ensure that this table doesn't already exist.
        if !create_table.if_not_exists && self.tables.contains_key(&name) {
            return Err(Error::TableAlreadyExists(name));
        }

        let mut table = Table::default();
        for column in create_table.columns {
            let column_name = &column.name.value;
            let mut nullable = true;
            let mut default = false;
            let ty: SqlType = column.data_type.into();

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
                        let inferrer = CreateTableInferrer::default();
                        let default_ty =
                            self.infer_expr_type(&expr, Some(ty.clone()), &inferrer)?;
                        if ty != default_ty {
                            return Err(Error::TypeMismatch {
                                expected: ty,
                                got: default_ty,
                            });
                        }

                        default = true;
                    }
                    ColumnOption::Unique { is_primary, .. } => {
                        table.insert_constraint(&[column_name], Constraint::Unique);
                        if is_primary {
                            nullable = false;
                            table.insert_constraint(&[column_name], Constraint::PrimaryKey);
                        }
                    }
                    ColumnOption::ForeignKey {
                        foreign_table,
                        referred_columns,
                        on_delete,
                        on_update,
                        ..
                    } => {
                        let foreign_table_name = object_name_to_strings(&foreign_table)
                            .first()
                            .unwrap()
                            .to_string();

                        // Verify that foreign table exists.
                        let f_table = self.get_table(&foreign_table_name).ok_or_else(|| {
                            Error::TableDoesntExist(foreign_table_name.to_string())
                        })?;

                        if referred_columns.len() > 1 {
                            return Err(Error::Sql(
                                "Cannot have more than 1 foreign column".to_string(),
                            ));
                        }

                        let mut foreign_columns = vec![];

                        if let Some(foreign_column) = referred_columns.first() {
                            let foreign_column_name = &foreign_column.value;

                            // Verify that foreign column exists.
                            let f_column =
                                f_table.get_column(foreign_column_name).ok_or_else(|| {
                                    Error::ColumnDoesntExist(foreign_column_name.to_string())
                                })?;

                            // Verify that the foreign column is UNIQUE.
                            if !f_table.is_unique(&[foreign_column_name]) {
                                return Err(Error::ForeignKeyConstraint(
                                    foreign_column_name.to_string(),
                                ));
                            }

                            // Verify that they are of the same type.
                            if ty != f_column.ty {
                                return Err(Error::TypeMismatch {
                                    expected: f_column.ty.clone(),
                                    got: ty,
                                });
                            }

                            if let Some(on_delete) = on_delete {
                                validate_on_action(&on_delete, column_name, nullable, default)?;
                            }

                            if let Some(on_update) = on_update {
                                validate_on_action(&on_update, column_name, nullable, default)?;
                            }

                            foreign_columns.push(foreign_column_name.to_string());
                        }

                        table.insert_constraint(
                            &[column_name],
                            Constraint::ForeignKey {
                                foreign_table: foreign_table_name,
                                foreign_columns,
                                on_delete: on_delete.map(|od| od.into()).unwrap_or_default(),
                                on_update: on_update.map(|ou| ou.into()).unwrap_or_default(),
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
                ty,
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
                TableConstraint::Unique { columns, .. } => {
                    // TODO: Properly support unique constraint names
                    let column_names: Vec<String> =
                        columns.iter().map(|c| c.value.to_string()).collect();

                    for column_name in column_names.iter() {
                        if !table.has_column(column_name) {
                            return Err(Error::ColumnDoesntExist(column_name.clone()));
                        }
                    }

                    table.insert_constraint(&column_names, Constraint::Unique);
                }
                TableConstraint::PrimaryKey { columns, .. } => {
                    let column_names: Vec<String> =
                        columns.iter().map(|c| c.value.to_string()).collect();

                    if column_names.len() == 1 {
                        let name = column_names.first().unwrap();
                        let column = table.columns.get_mut(name).unwrap();
                        column.nullable = false;
                    }

                    for column_name in column_names.iter() {
                        if !table.has_column(column_name) {
                            return Err(Error::ColumnDoesntExist(column_name.clone()));
                        }
                    }

                    table.insert_constraint(&column_names, Constraint::Unique);
                    table.insert_constraint(&column_names, Constraint::PrimaryKey);
                }
                TableConstraint::ForeignKey {
                    columns,
                    foreign_table,
                    referred_columns,
                    on_delete,
                    on_update,
                    ..
                } => {
                    // TODO: Properly support foreign key names.

                    let foreign_table_name = object_name_to_strings(&foreign_table)
                        .first()
                        .unwrap()
                        .to_string();

                    let f_table = self
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

                        let foreign_column =
                            f_table.get_column(foreign_col_name).ok_or_else(|| {
                                Error::ColumnDoesntExist(foreign_col_name.to_string())
                            })?;

                        if local_column.ty != foreign_column.ty {
                            return Err(Error::TypeMismatch {
                                expected: foreign_column.ty.clone(),
                                got: local_column.ty.clone(),
                            });
                        }

                        if let Some(on_delete) = on_delete {
                            validate_on_action(
                                &on_delete,
                                local_col_name,
                                local_column.nullable,
                                local_column.default,
                            )?;
                        }

                        if let Some(on_update) = on_update {
                            validate_on_action(
                                &on_update,
                                local_col_name,
                                local_column.nullable,
                                local_column.default,
                            )?;
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
                            on_delete: on_delete.map(|od| od.into()).unwrap_or_default(),
                            on_update: on_update.map(|ou| ou.into()).unwrap_or_default(),
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
        self.tables.insert(name, table);

        Ok(())
    }
}

#[derive(Default)]
struct CreateTableInferrer {}

impl<D: Dialect> ColumnInferrer<D> for CreateTableInferrer {
    fn infer_unqualified_type(
        &self,
        _: &Simulator<D>,
        column: &str,
    ) -> Result<Option<SqlType>, Error> {
        Err(Error::InvalidDefault(column.to_string()))
    }

    fn infer_qualified_type(
        &self,
        _: &Simulator<D>,
        _: &str,
        column: &str,
    ) -> Result<SqlType, Error> {
        Err(Error::InvalidDefault(column.to_string()))
    }
}

fn validate_on_action(
    ref_act: &ReferentialAction,
    column_name: &str,
    nullable: bool,
    default: bool,
) -> Result<(), Error> {
    match ref_act {
        ReferentialAction::NoAction | ReferentialAction::Restrict | ReferentialAction::Cascade => {}
        ReferentialAction::SetNull => {
            if !nullable {
                return Err(Error::NullOnNotNullColumn(column_name.to_string()));
            }
        }
        ReferentialAction::SetDefault => {
            if !default {
                return Err(Error::DefaultOnNotDefaultColumn(column_name.to_string()));
            }
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::{ty::SqlType, *};

    #[test]
    fn create_table() {
        let mut sim = Simulator::new(GenericDialect {});
        sim.execute("create table abc (id int);").unwrap();
        assert_eq!(sim.tables.len(), 1);
        assert!(sim.tables.contains_key("abc"));
    }

    #[test]
    fn create_table_duplicate() {
        let mut sim = Simulator::new(GenericDialect {});
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
        let mut sim = Simulator::new(GenericDialect {});
        sim.execute("create table abc (id int);").unwrap();
        assert_eq!(sim.tables.len(), 1);
        assert!(sim.tables.contains_key("abc"));
        sim.execute("create table if not exists abc (id int);")
            .unwrap();
        assert_eq!(sim.tables.len(), 1);
    }

    #[test]
    fn create_table_columns() {
        let mut sim = Simulator::new(GenericDialect {});
        sim.execute("create table person (id uuid, name text, weight real);")
            .unwrap();
        assert_eq!(sim.tables.len(), 1);
        let table = sim.tables.get("person").unwrap();
        assert_eq!(table.columns.get("id").unwrap().get_ty(), &SqlType::Uuid);
        assert_eq!(table.columns.get("name").unwrap().get_ty(), &SqlType::Text);
        assert_eq!(
            table.columns.get("weight").unwrap().get_ty(),
            &SqlType::Float
        );
    }

    #[test]
    fn create_table_columns_duplicate() {
        let mut sim = Simulator::new(GenericDialect {});
        assert_eq!(
            sim.execute("create table person (id uuid, id int);"),
            Err(Error::ColumnAlreadyExists("id".to_string()))
        );
    }

    #[test]
    fn create_table_with_col_foreign_key() {
        let mut sim = Simulator::new(GenericDialect {});
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
        let mut sim = Simulator::new(GenericDialect {});
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
        let mut sim = Simulator::new(GenericDialect {});
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
        let mut sim = Simulator::new(GenericDialect {});
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
                expected: SqlType::Uuid,
                got: SqlType::Text
            })
        );
    }

    #[test]
    fn create_table_with_table_foreign_key() {
        let mut sim = Simulator::new(GenericDialect {});
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
        let mut sim = Simulator::new(GenericDialect {});
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
        let mut sim = Simulator::new(GenericDialect {});
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
        let mut sim = Simulator::new(GenericDialect {});
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
                expected: SqlType::Uuid,
                got: SqlType::Text
            })
        );
    }

    #[test]
    fn create_table_foreign_key_on_delete_null_on_not_null() {
        let mut sim = Simulator::new(GenericDialect {});
        sim.execute("create table person (id uuid primary key, name text unique, phone int);")
            .unwrap();

        assert_eq!(
            sim.execute(
                r#"
                create table order(
                    order_id uuid primary key,
                    person_id uuid not null,
                    foreign key (person_id) references person(id) on delete set null
                );
            "#,
            ),
            Err(Error::NullOnNotNullColumn("person_id".to_string()))
        );
    }

    #[test]
    fn create_table_foreign_key_on_delete_default_on_not_default() {
        let mut sim = Simulator::new(GenericDialect {});
        sim.execute("create table person (id uuid primary key, name text unique, phone int);")
            .unwrap();

        assert_eq!(
            sim.execute(
                r#"
                create table order(
                    order_id uuid primary key,
                    person_id uuid,
                    foreign key (person_id) references person(id) on delete set default
                );
            "#,
            ),
            Err(Error::DefaultOnNotDefaultColumn("person_id".to_string()))
        );
    }

    #[test]
    fn create_table_col_foreign_key_on_update_null_on_not_null() {
        let mut sim = Simulator::new(GenericDialect {});
        sim.execute("create table person (id uuid primary key, name text unique, phone int);")
            .unwrap();

        assert_eq!(
            sim.execute(
                r#"
                create table order(
                    order_id uuid primary key,
                    person_id uuid not null references person(id) on update set null
                );
            "#,
            ),
            Err(Error::NullOnNotNullColumn("person_id".to_string()))
        );
    }

    #[test]
    fn create_table_col_foreign_key_on_update_default_on_not_default() {
        let mut sim = Simulator::new(GenericDialect {});
        sim.execute("create table person (id uuid primary key, name text unique, phone int);")
            .unwrap();

        assert_eq!(
            sim.execute(
                r#"
                create table order(
                    order_id uuid primary key,
                    person_id uuid not null references person(id) on update set default
                );
            "#,
            ),
            Err(Error::DefaultOnNotDefaultColumn("person_id".to_string()))
        );
    }

    #[test]
    fn create_table_table_foreign_key_on_update_null_on_not_null() {
        let mut sim = Simulator::new(GenericDialect {});
        sim.execute("create table person (id uuid primary key, name text unique, phone int);")
            .unwrap();

        assert_eq!(
            sim.execute(
                r#"
                create table order(
                    order_id uuid primary key,
                    person_id uuid not null,
                    foreign key (person_id) references person(id) on update set null
                );
            "#,
            ),
            Err(Error::NullOnNotNullColumn("person_id".to_string()))
        );
    }

    #[test]
    fn create_table_table_foreign_key_on_update_default_on_not_default() {
        let mut sim = Simulator::new(GenericDialect {});
        sim.execute("create table person (id uuid primary key, name text unique, phone int);")
            .unwrap();

        assert_eq!(
            sim.execute(
                r#"
                create table order(
                    order_id uuid primary key,
                    person_id uuid,
                    foreign key (person_id) references person(id) on update set default
                );
            "#,
            ),
            Err(Error::DefaultOnNotDefaultColumn("person_id".to_string()))
        );
    }

    #[test]
    fn create_table_with_primary_key_col() {
        let mut sim = Simulator::new(GenericDialect {});
        sim.execute("create table person (id uuid primary key);")
            .unwrap();

        assert!(sim.get_table("person").unwrap().is_primary_key(&["id"]))
    }

    #[test]
    fn create_table_with_unique_col() {
        let mut sim = Simulator::new(GenericDialect {});
        sim.execute("create table person (id uuid primary key, name text unique);")
            .unwrap();

        assert!(sim.get_table("person").unwrap().is_unique(&["name"]))
    }

    #[test]
    fn create_table_with_primary_key_on_table() {
        let mut sim = Simulator::new(GenericDialect {});
        sim.execute("create table person (id uuid, primary key (id));")
            .unwrap();

        assert!(sim.get_table("person").unwrap().is_primary_key(&["id"]))
    }

    #[test]
    fn create_table_with_unique_on_table() {
        let mut sim = Simulator::new(GenericDialect {});
        sim.execute("create table person (id uuid primary key, name text, unique(name));")
            .unwrap();

        assert!(sim.get_table("person").unwrap().is_unique(&["name"]))
    }

    #[test]
    fn create_table_with_default_value_type_mismatch() {
        let mut sim = Simulator::new(GenericDialect {});
        assert_eq!(
            sim.execute(
                "create table person (id uuid primary key, name text default 123, unique(name));",
            ),
            Err(Error::TypeMismatch {
                expected: SqlType::Text,
                got: SqlType::SmallInt
            })
        )
    }

    #[test]
    fn create_table_with_default_value_column_name() {
        let mut sim = Simulator::new(GenericDialect {});
        assert_eq!(
            sim.execute(
                "create table person (id uuid primary key, name text, nickname text default name, unique(name));",
            ),
            Err(Error::InvalidDefault("name".to_string()))
        )
    }
}
