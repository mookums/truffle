use truffle::Simulator;

#[test]
fn select_is_null() {
    let mut sim = Simulator::default();

    sim.execute("create table item (id int primary key, name text not null, age integer)")
        .unwrap();

    sim.execute("update item set age = 10 where (age + 2) is null")
        .unwrap();
}

#[test]
fn update_is_not_null() {
    let mut sim = Simulator::default();

    sim.execute("create table item (id int primary key, name text not null, age integer)")
        .unwrap();

    sim.execute("select * from item where (age / 20) is not null")
        .unwrap();
}
