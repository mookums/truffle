use truffle::Simulator;

#[test]
fn select_substring() {
    let mut sim = Simulator::default();

    sim.execute("create table item (id int primary key, name text not null)")
        .unwrap();

    sim.execute("select 1 from item where substring(name, 1, 3) = 'abc'")
        .unwrap();
}
