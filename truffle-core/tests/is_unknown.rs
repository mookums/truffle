use truffle::Simulator;

#[test]
fn update_is_unknown() {
    let mut sim = Simulator::default();

    sim.execute("create table item (id int primary key, name text not null, age integer)")
        .unwrap();

    sim.execute("update item set age = null where (age + 2) is unknown")
        .unwrap();
}

#[test]
fn select_is_not_unknown() {
    let mut sim = Simulator::default();

    sim.execute("create table item (id int primary key, name text not null, age integer)")
        .unwrap();

    sim.execute("select * from item where (age + 2) is not unknown")
        .unwrap();
}
