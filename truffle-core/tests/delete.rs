use truffle::{Error, Simulator, ty::SqlType};

#[test]
fn delete_row_by_field() {
    let mut sim = Simulator::default();
    sim.execute("create table person (id int primary key, name text)")
        .unwrap();
    sim.execute("delete from person where id = 5").unwrap();

    let resolve = sim.execute("delete from person where id = ?").unwrap();

    assert_eq!(resolve.inputs.len(), 1);
    assert_eq!(resolve.get_input(0).unwrap().ty, SqlType::Integer);

    assert_eq!(resolve.outputs.len(), 0);
}

#[test]
fn delete_row_column_doesnt_exist() {
    let mut sim = Simulator::default();
    sim.execute("create table person (id int primary key, name text)")
        .unwrap();

    assert_eq!(
        sim.execute("delete from person where weight = ?"),
        Err(Error::ColumnDoesntExist("weight".to_string()))
    )
}

#[test]
fn delete_row_table_doesnt_exist() {
    let mut sim = Simulator::default();

    assert_eq!(
        sim.execute("delete from person where weight = ?"),
        Err(Error::TableDoesntExist("person".to_string()))
    )
}

#[test]
fn delete_row_join() {
    let mut sim = Simulator::default();
    sim.execute("create table person (id int primary key, name text)")
        .unwrap();
    sim.execute(
        "create table order (id int primary key, item text not null, address text not null)",
    )
    .unwrap();
    sim.execute("delete from person natural join order where address = ?")
        .unwrap();
}
