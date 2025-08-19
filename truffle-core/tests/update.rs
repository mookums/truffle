use truffle::{Error, Simulator, ty::SqlType};

#[test]
fn update_basic_success() {
    let mut sim = Simulator::default();
    sim.execute("create table person (id int, name text)")
        .unwrap();

    let resolve = sim
        .execute("update person set name = 'other name' where id = ?")
        .unwrap();

    assert_eq!(resolve.inputs.len(), 1);
}

#[test]
fn update_set_type_mismatch() {
    let mut sim = Simulator::default();
    sim.execute("create table person (id int, name text)")
        .unwrap();

    assert_eq!(
        sim.execute("update person set name = 10 where id = ?"),
        Err(Error::TypeMismatch {
            expected: SqlType::Text,
            got: SqlType::SmallInt
        })
    );
}

#[test]
fn update_with_join() {
    let mut sim = Simulator::default();
    sim.execute("create table person (id int, name text, department_id int)")
        .unwrap();
    sim.execute("create table department (id int, budget int)")
        .unwrap();

    let resolve = sim
        .execute("update person set name = 'updated' from department where person.department_id = department.id and department.budget > ?")
        .unwrap();
    assert_eq!(resolve.inputs.len(), 1);
}

#[test]
fn update_nonexistent_table() {
    let mut sim = Simulator::default();
    assert_eq!(
        sim.execute("update nonexistent set name = 'value'"),
        Err(Error::TableDoesntExist("nonexistent".to_string()))
    );
}

#[test]
fn update_nonexistent_column() {
    let mut sim = Simulator::default();
    sim.execute("create table person (id int, name text)")
        .unwrap();

    assert_eq!(
        sim.execute("update person set nonexistent_column = 'value'"),
        Err(Error::ColumnDoesntExist("nonexistent_column".to_string()))
    );
}

#[test]
fn update_with_table_alias() {
    let mut sim = Simulator::default();
    sim.execute("create table person (id int, name text)")
        .unwrap();

    let resolve = sim
        .execute("update person as p set name = 'new name' where p.id = ?")
        .unwrap();
    assert_eq!(resolve.inputs.len(), 1);
}

#[test]
fn update_alias_conflicts_with_table_name() {
    let mut sim = Simulator::default();
    sim.execute("create table person (id int, name text)")
        .unwrap();
    sim.execute("create table employee (id int, salary int)")
        .unwrap();

    assert_eq!(
        sim.execute("update person as employee set name = 'value'"),
        Err(Error::AliasIsTableName("employee".to_string()))
    );
}

#[test]
fn update_multiple_assignments() {
    let mut sim = Simulator::default();
    sim.execute("create table person (id int, name text, age int)")
        .unwrap();

    let resolve = sim
        .execute("update person set name = ?, age = ? where id = ?")
        .unwrap();
    assert_eq!(resolve.inputs.len(), 3);
}

#[test]
fn update_with_expression_in_assignment() {
    let mut sim = Simulator::default();
    sim.execute("create table person (id int, age int)")
        .unwrap();

    let resolve = sim
        .execute("update person set age = age + ? where id = ?")
        .unwrap();
    assert_eq!(resolve.inputs.len(), 2);
}

#[test]
fn update_where_clause_type_mismatch() {
    let mut sim = Simulator::default();
    sim.execute("create table person (id int, name text)")
        .unwrap();

    assert_eq!(
        sim.execute("update person set name = 'value' where 'not_boolean'"),
        Err(Error::TypeMismatch {
            expected: SqlType::Boolean,
            got: SqlType::Text
        })
    );
}
