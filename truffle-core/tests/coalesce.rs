use truffle::{Error, Simulator, ty::SqlType};

#[test]
fn select_with_coalesce_function() {
    let mut sim = Simulator::default();
    sim.execute("create table item (id int primary key, name text not null default 'abc', age int default 0)").unwrap();

    let resolve = sim
        .execute("select COALESCE(age, $1) from item where id = $2")
        .unwrap();

    assert_eq!(resolve.inputs.len(), 2);
    assert_eq!(resolve.get_input(0).unwrap().ty, SqlType::Integer);
    assert_eq!(resolve.get_input(1).unwrap().ty, SqlType::Integer);
    assert_eq!(resolve.outputs.len(), 1);
    assert_eq!(
        resolve.outputs.iter().next().unwrap().1.ty,
        SqlType::Integer
    );
}

#[test]
fn select_with_coalesce_mixed_types() {
    let mut sim = Simulator::default();
    sim.execute("create table item (id int primary key, name text not null default 'abc', age int default 0)").unwrap();

    let resolve = sim
        .execute("select COALESCE(name, $1) from item where id = $2")
        .unwrap();

    assert_eq!(resolve.inputs.len(), 2);
    assert_eq!(resolve.get_input(0).unwrap().ty, SqlType::Text);
    assert_eq!(resolve.get_input(1).unwrap().ty, SqlType::Integer);
    assert_eq!(resolve.outputs.len(), 1);
    assert_eq!(resolve.outputs.iter().next().unwrap().1.ty, SqlType::Text);
}

#[test]
fn select_with_coalesce_column_doesnt_exist() {
    let mut sim = Simulator::default();
    sim.execute("create table item (id int primary key, name text not null default 'abc', age int default 0)").unwrap();

    assert_eq!(
        sim.execute("select COALESCE(cart, $1) from item where id = $2"),
        Err(Error::ColumnDoesntExist("cart".to_string()))
    );
}

#[test]
fn select_with_coalesce_multiple_args() {
    let mut sim = Simulator::default();
    sim.execute("create table item (id int primary key, name text not null default 'abc', age int default 0)").unwrap();

    let resolve = sim
        .execute("select COALESCE(age, id, $1) from item where id = $2")
        .unwrap();

    assert_eq!(resolve.inputs.len(), 2);
    assert_eq!(resolve.get_input(0).unwrap().ty, SqlType::Integer);
    assert_eq!(resolve.get_input(1).unwrap().ty, SqlType::Integer);
    assert_eq!(resolve.outputs.len(), 1);
    assert_eq!(
        resolve.outputs.iter().next().unwrap().1.ty,
        SqlType::Integer
    );
}

#[test]
fn select_with_coalesce_not_nullable_with_primitive() {
    let mut sim = Simulator::default();
    sim.execute("create table item (id int primary key, name text not null default 'abc', age int default 0)").unwrap();

    let resolve = sim
        .execute("select COALESCE(age, $1, 1) from item where id = $2")
        .unwrap();

    assert_eq!(resolve.inputs.len(), 2);

    let first_placeholder = resolve.get_input(0).unwrap();
    assert_eq!(first_placeholder.ty, SqlType::Integer);
    assert!(first_placeholder.nullable);

    assert_eq!(resolve.get_input(1).unwrap().ty, SqlType::Integer);

    assert_eq!(resolve.outputs.len(), 1);

    let output = resolve.outputs.get_index(0).unwrap().1;
    assert_eq!(output.ty, SqlType::Integer);
    assert!(!output.nullable);
}

#[test]
fn select_with_coalesce_not_nullable_with_column() {
    let mut sim = Simulator::default();
    sim.execute("create table item (id int primary key, name text not null default 'abc', age int default 0, birth_age int not null)").unwrap();

    let resolve = sim
        .execute("select COALESCE(age, birth_age) from item where id = $1")
        .unwrap();

    assert_eq!(resolve.inputs.len(), 1);
    assert_eq!(resolve.get_input(0).unwrap().ty, SqlType::Integer);

    assert_eq!(resolve.outputs.len(), 1);

    let output = resolve.outputs.get_index(0).unwrap().1;
    assert_eq!(output.ty, SqlType::Integer);
    assert!(!output.nullable);
}

#[test]
fn select_with_coalesce_placeholder_first() {
    let mut sim = Simulator::default();
    sim.execute("create table item (id int primary key, name text not null default 'abc', age int default 0, birth_age int not null)").unwrap();

    let resolve = sim
        .execute("select COALESCE($1, birth_age) from item where id = $2")
        .unwrap();

    assert_eq!(resolve.inputs.len(), 2);
    assert_eq!(resolve.get_input(0).unwrap().ty, SqlType::Integer);
    assert_eq!(resolve.get_input(1).unwrap().ty, SqlType::Integer);

    assert_eq!(resolve.outputs.len(), 1);

    let output = resolve.outputs.get_index(0).unwrap().1;
    assert_eq!(output.ty, SqlType::Integer);
    assert!(!output.nullable);
}
