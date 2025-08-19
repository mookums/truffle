use truffle::{Error, Simulator, ty::SqlType};

#[test]
fn select_where_between() {
    let mut sim = Simulator::default();
    sim.execute("create table person (id int primary key, name text not null)")
        .unwrap();
    sim.execute("select * from person where id between 0 and 999")
        .unwrap();
}

#[test]
fn select_where_between_type_mismatch() {
    let mut sim = Simulator::default();
    sim.execute("create table person (id int primary key, name text not null)")
        .unwrap();

    assert_eq!(
        sim.execute("select * from person where id between 'a' and 'f'"),
        Err(Error::TypeMismatch {
            expected: SqlType::Integer,
            got: SqlType::Text
        })
    );
}
