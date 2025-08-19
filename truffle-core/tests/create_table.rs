use truffle::{Error, Simulator, ty::SqlType};

#[test]
fn create_table() {
    let mut sim = Simulator::default();
    sim.execute("create table abc (id int);").unwrap();
    assert_eq!(sim.tables.len(), 1);
    assert!(sim.tables.contains_key("abc"));
}

#[test]
fn create_table_duplicate() {
    let mut sim = Simulator::default();
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
    let mut sim = Simulator::default();
    sim.execute("create table abc (id int);").unwrap();
    assert_eq!(sim.tables.len(), 1);
    assert!(sim.tables.contains_key("abc"));
    sim.execute("create table if not exists abc (id int);")
        .unwrap();
    assert_eq!(sim.tables.len(), 1);
}

#[test]
#[cfg(feature = "uuid")]
fn create_table_columns() {
    let mut sim = Simulator::default();
    sim.execute("create table person (id uuid, name text, weight real);")
        .unwrap();
    assert_eq!(sim.tables.len(), 1);
    let table = sim.tables.get("person").unwrap();
    assert_eq!(table.columns.get("id").unwrap().ty, SqlType::Uuid);
    assert_eq!(table.columns.get("name").unwrap().ty, SqlType::Text);
    assert_eq!(table.columns.get("weight").unwrap().ty, SqlType::Float);
}

#[test]
fn create_table_columns_duplicate() {
    let mut sim = Simulator::default();
    assert_eq!(
        sim.execute("create table person (id uuid, id int);"),
        Err(Error::ColumnAlreadyExists("id".to_string()))
    );
}

#[test]
fn create_table_with_col_foreign_key() {
    let mut sim = Simulator::default();
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
    let mut sim = Simulator::default();
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
    let mut sim = Simulator::default();
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
#[cfg(feature = "uuid")]
fn create_table_with_col_foreign_key_type_mismatch() {
    let mut sim = Simulator::default();
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
    let mut sim = Simulator::default();
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
    let mut sim = Simulator::default();
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
    let mut sim = Simulator::default();
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
#[cfg(feature = "uuid")]
fn create_table_with_table_foreign_key_type_mismatch() {
    let mut sim = Simulator::default();
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
    let mut sim = Simulator::default();
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
    let mut sim = Simulator::default();
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
    let mut sim = Simulator::default();
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
    let mut sim = Simulator::default();
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
    let mut sim = Simulator::default();
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
    let mut sim = Simulator::default();
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
    let mut sim = Simulator::default();
    sim.execute("create table person (id uuid primary key);")
        .unwrap();

    assert!(sim.get_table("person").unwrap().is_primary_key(&["id"]))
}

#[test]
fn create_table_with_unique_col() {
    let mut sim = Simulator::default();
    sim.execute("create table person (id uuid primary key, name text unique);")
        .unwrap();

    assert!(sim.get_table("person").unwrap().is_unique(&["name"]))
}

#[test]
fn create_table_with_primary_key_on_table() {
    let mut sim = Simulator::default();
    sim.execute("create table person (id uuid, primary key (id));")
        .unwrap();

    assert!(sim.get_table("person").unwrap().is_primary_key(&["id"]))
}

#[test]
fn create_table_with_unique_on_table() {
    let mut sim = Simulator::default();
    sim.execute("create table person (id uuid primary key, name text, unique(name));")
        .unwrap();

    assert!(sim.get_table("person").unwrap().is_unique(&["name"]))
}

#[test]
fn create_table_with_default_value_type_mismatch() {
    let mut sim = Simulator::default();
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
    let mut sim = Simulator::default();
    assert_eq!(
            sim.execute(
                "create table person (id uuid primary key, name text, nickname text default name, unique(name));",
            ),
            Err(Error::InvalidDefault("name".to_string()))
        )
}
