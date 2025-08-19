use truffle::{Error, Simulator};

#[test]
fn drop_table_success() {
    let mut sim = Simulator::default();
    sim.execute("create table person (id uuid, name text, weight real);")
        .unwrap();
    assert_eq!(sim.tables.len(), 1);
    sim.execute("drop table person;").unwrap();
    assert_eq!(sim.tables.len(), 0);
}

#[test]
fn drop_table_doesnt_exist() {
    let mut sim = Simulator::default();
    assert_eq!(
        sim.execute("drop table person;"),
        Err(Error::TableDoesntExist("person".to_string()))
    );
}

#[test]
fn drop_table_foreign_key_constaint() {
    let mut sim = Simulator::default();
    sim.execute("create table person (id int primary key, name text)")
        .unwrap();
    sim.execute("create table order (id int primary key, person_id int references person(id))")
        .unwrap();

    assert_eq!(
        sim.execute("drop table person"),
        Err(Error::ForeignKeyConstraint("person".to_string()))
    )
}
