use truffle::{Error, Simulator, ty::SqlType};

#[test]
fn select_is_distinct() {
    let mut sim = Simulator::default();

    sim.execute("create table item (id int primary key, name text not null, age integer)")
        .unwrap();

    sim.execute("select * from item where age is distinct from ?")
        .unwrap();
}

#[test]
fn select_is_not_distinct() {
    let mut sim = Simulator::default();

    sim.execute("create table item (id int primary key, name text not null, age integer)")
        .unwrap();

    sim.execute("select * from item where age is not distinct from ?")
        .unwrap();
}

#[test]
fn select_is_distinct_type_mismatch() {
    let mut sim = Simulator::default();

    sim.execute("create table item (id int primary key, name text not null, age integer)")
        .unwrap();

    assert_eq!(
        sim.execute("select * from item where age is not distinct from 'value'"),
        Err(Error::TypeMismatch {
            expected: SqlType::Integer,
            got: SqlType::Text
        })
    );
}
