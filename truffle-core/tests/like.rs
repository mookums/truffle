use truffle::{Error, Simulator, ty::SqlType};

#[test]
fn select_with_like() {
    let mut sim = Simulator::default();
    sim.execute("create table item (id int primary key, name text not null)")
        .unwrap();

    sim.execute("select * from item where name like 'John%' ")
        .unwrap();
}

#[test]
fn select_with_like_wrong_type() {
    let mut sim = Simulator::default();
    sim.execute("create table item (id int primary key, name text not null, age integer not null)")
        .unwrap();

    assert_eq!(
        sim.execute("select * from item where age like 'John%' "),
        Err(Error::TypeMismatch {
            expected: SqlType::Text,
            got: SqlType::Integer
        })
    );
}

#[test]
fn select_with_ilike() {
    let mut sim = Simulator::default();
    sim.execute("create table item (id int primary key, name text not null)")
        .unwrap();

    sim.execute("select * from item where name ilike 'John%' ")
        .unwrap();
}

#[test]
fn select_with_ilike_wrong_type() {
    let mut sim = Simulator::default();
    sim.execute("create table item (id int primary key, name text not null, age integer not null)")
        .unwrap();

    assert_eq!(
        sim.execute("select * from item where age ilike 'John%' "),
        Err(Error::TypeMismatch {
            expected: SqlType::Text,
            got: SqlType::Integer
        })
    );
}
