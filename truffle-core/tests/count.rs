use truffle::{Error, Simulator, ty::SqlType};

#[test]
fn select_with_count_function() {
    let mut sim = Simulator::default();
    sim.execute("create table item (id int primary key, name text not null default 'abc', age int default 0)").unwrap();

    let resolve = sim
        .execute("select COUNT(id) from item where id = $1")
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
fn select_with_count_column_doesnt_exist() {
    let mut sim = Simulator::default();
    sim.execute("create table item (id int primary key, name text not null default 'abc', age int default 0)").unwrap();

    assert_eq!(
        sim.execute("select COUNT(cart) from item where id = $1"),
        Err(Error::ColumnDoesntExist("cart".to_string()))
    );
}

#[test]
fn select_with_count_wildcard_function() {
    let mut sim = Simulator::default();
    sim.execute("create table item (id int primary key, name text not null default 'abc', age int default 0)").unwrap();

    let resolve = sim
        .execute("select COUNT(*) from item where id = $1")
        .unwrap();

    assert_eq!(resolve.inputs.len(), 1);
    assert_eq!(resolve.get_input(0).unwrap().ty, SqlType::Integer);

    assert_eq!(resolve.outputs.len(), 1);
    assert_eq!(
        resolve.outputs.iter().next().unwrap().1.ty,
        SqlType::Integer
    );
}

// #[test]
// fn select_with_count_function_aliased() {
//     let mut sim = Simulator::default();
//     sim.execute("create table item (id int primary key, name text not null default 'abc', age int default 0)").unwrap();

//     let resolve = sim
//         .execute("select COUNT(id) as item_count from item where id = $1")
//         .unwrap();

//     assert_eq!(resolve.inputs.len(), 1);
//     assert_eq!(resolve.get_input(0).unwrap().ty, SqlType::Integer);

//     assert_eq!(resolve.outputs.len(), 1);
//     assert_eq!(
//         resolve.get_output_with_name("item_count").unwrap().ty,
//         SqlType::Integer
//     );
// }
