use truffle::{Error, Simulator, ty::SqlType};

#[test]
fn select_with_min_function() {
    let mut sim = Simulator::default();
    sim.execute("create table item (id int primary key, name text not null default 'abc', age int default 0)").unwrap();

    let resolve = sim
        .execute("select min(id) from item where id = $1")
        .unwrap();

    assert_eq!(resolve.inputs.len(), 1);
    assert_eq!(resolve.get_input(0).unwrap().ty, SqlType::Integer);

    assert_eq!(resolve.outputs.len(), 1);
    assert_eq!(
        resolve.outputs.iter().next().unwrap().1.ty,
        SqlType::Integer
    );
}

#[test]
fn select_with_min_column_doesnt_exist() {
    let mut sim = Simulator::default();
    sim.execute("create table item (id int primary key, name text not null default 'abc', age int default 0)").unwrap();

    assert_eq!(
        sim.execute("select min(cart) from item where id = $1"),
        Err(Error::ColumnDoesntExist("cart".to_string()))
    );
}

#[test]
fn select_with_min_wildcard_function() {
    let mut sim = Simulator::default();
    sim.execute("create table item (id int primary key, name text not null default 'abc', age int default 0)").unwrap();

    assert!(matches!(
        sim.execute("select min(*) from item where id = $1"),
        Err(Error::FunctionCall(_))
    ));
}

#[test]
fn select_with_min_function_aliased() {
    let mut sim = Simulator::default();
    sim.execute("create table item (id int primary key, name text not null default 'abc', age int default 0)").unwrap();

    let resolve = sim.execute("select min(age) as min_age from item").unwrap();

    assert_eq!(resolve.inputs.len(), 0);

    assert_eq!(resolve.outputs.len(), 1);
    assert_eq!(
        resolve.get_output_with_name("min_age").unwrap().ty,
        SqlType::Integer
    );
}

#[test]
fn select_with_max_function() {
    let mut sim = Simulator::default();
    sim.execute("create table item (id int primary key, name text not null default 'abc', age int default 0)").unwrap();

    let resolve = sim
        .execute("select max(id) from item where id = $1")
        .unwrap();

    assert_eq!(resolve.inputs.len(), 1);
    assert_eq!(resolve.get_input(0).unwrap().ty, SqlType::Integer);

    assert_eq!(resolve.outputs.len(), 1);
    assert_eq!(
        resolve.outputs.iter().next().unwrap().1.ty,
        SqlType::Integer
    );
}

#[test]
fn select_with_max_column_doesnt_exist() {
    let mut sim = Simulator::default();
    sim.execute("create table item (id int primary key, name text not null default 'abc', age int default 0)").unwrap();

    assert_eq!(
        sim.execute("select max(cart) from item where id = $1"),
        Err(Error::ColumnDoesntExist("cart".to_string()))
    );
}

#[test]
fn select_with_max_wildcard_function() {
    let mut sim = Simulator::default();
    sim.execute("create table item (id int primary key, name text not null default 'abc', age int default 0)").unwrap();

    assert!(matches!(
        sim.execute("select max(*) from item where id = $1"),
        Err(Error::FunctionCall(_))
    ));
}

#[test]
fn select_with_max_function_aliased() {
    let mut sim = Simulator::default();
    sim.execute("create table item (id int primary key, name text not null default 'abc', age int default 0)").unwrap();

    let resolve = sim.execute("select max(age) as max_age from item").unwrap();

    assert_eq!(resolve.inputs.len(), 0);

    assert_eq!(resolve.outputs.len(), 1);
    assert_eq!(
        resolve.get_output_with_name("max_age").unwrap().ty,
        SqlType::Integer
    );
}
