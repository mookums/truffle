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

#[test]
fn update_with_returning_wildcard() {
    let mut sim = Simulator::default();
    sim.execute("create table person (id int, name text, age int)")
        .unwrap();
    let resolve = sim
        .execute("update person set name = ?, age = ? where id = ? returning *")
        .unwrap();

    assert_eq!(resolve.inputs.len(), 3);
    assert_eq!(resolve.get_input(0).unwrap().ty, SqlType::Text);
    assert_eq!(resolve.get_input(1).unwrap().ty, SqlType::Integer);
    assert_eq!(resolve.get_input(2).unwrap().ty, SqlType::Integer);

    assert_eq!(resolve.outputs.len(), 3);
    assert_eq!(
        resolve.get_output_with_name("id").unwrap().ty,
        SqlType::Integer
    );
    assert_eq!(
        resolve.get_output_with_name("name").unwrap().ty,
        SqlType::Text
    );
    assert_eq!(
        resolve.get_output_with_name("age").unwrap().ty,
        SqlType::Integer
    );
}

#[test]
fn update_with_returning_single_column() {
    let mut sim = Simulator::default();
    sim.execute("create table person (id int, name text, age int)")
        .unwrap();
    let resolve = sim
        .execute("update person set name = ? where id = ? returning id")
        .unwrap();

    assert_eq!(resolve.inputs.len(), 2);
    assert_eq!(resolve.outputs.len(), 1);
    assert_eq!(
        resolve.get_output_with_name("id").unwrap().ty,
        SqlType::Integer
    );
}

#[test]
fn update_with_returning_multiple_columns() {
    let mut sim = Simulator::default();
    sim.execute("create table person (id int, name text, age int)")
        .unwrap();
    let resolve = sim
        .execute("update person set name = ?, age = ? returning id, name")
        .unwrap();

    assert_eq!(resolve.inputs.len(), 2);
    assert_eq!(resolve.outputs.len(), 2);
    assert_eq!(
        resolve.get_output_with_name("id").unwrap().ty,
        SqlType::Integer
    );
    assert_eq!(
        resolve.get_output_with_name("name").unwrap().ty,
        SqlType::Text
    );
}

#[test]
fn update_with_returning_qualified_wildcard() {
    let mut sim = Simulator::default();
    sim.execute("create table person (id int, name text, age int)")
        .unwrap();
    let resolve = sim
        .execute("update person set name = ? returning person.*")
        .unwrap();

    assert_eq!(resolve.inputs.len(), 1);
    assert_eq!(resolve.outputs.len(), 3);
    assert_eq!(
        resolve.get_output_with_name("id").unwrap().ty,
        SqlType::Integer
    );
    assert_eq!(
        resolve.get_output_with_name("name").unwrap().ty,
        SqlType::Text
    );
    assert_eq!(
        resolve.get_output_with_name("age").unwrap().ty,
        SqlType::Integer
    );
}

#[test]
fn update_with_returning_qualified_column() {
    let mut sim = Simulator::default();
    sim.execute("create table person (id int, name text, age int)")
        .unwrap();
    let resolve = sim
        .execute("update person set name = ? returning person.id, person.name")
        .unwrap();

    assert_eq!(resolve.inputs.len(), 1);
    assert_eq!(resolve.outputs.len(), 2);
    assert_eq!(
        resolve.get_output_with_name("id").unwrap().ty,
        SqlType::Integer
    );
    assert_eq!(
        resolve.get_output_with_name("name").unwrap().ty,
        SqlType::Text
    );
}

#[test]
fn update_with_returning_alias() {
    let mut sim = Simulator::default();
    sim.execute("create table person (id int, name text, age int)")
        .unwrap();
    let resolve = sim
        .execute("update person set name = ? returning id, name as full_name, age as years_old")
        .unwrap();

    assert_eq!(resolve.inputs.len(), 1);
    assert_eq!(resolve.outputs.len(), 3);
    assert_eq!(
        resolve.get_output_with_name("id").unwrap().ty,
        SqlType::Integer
    );
    assert_eq!(
        resolve.get_output_with_name("full_name").unwrap().ty,
        SqlType::Text
    );
    assert_eq!(
        resolve.get_output_with_name("years_old").unwrap().ty,
        SqlType::Integer
    );
}

#[test]
fn update_with_returning_table_alias() {
    let mut sim = Simulator::default();
    sim.execute("create table person (id int, name text, age int)")
        .unwrap();
    let resolve = sim
        .execute("update person as p set name = ? returning p.id, p.name")
        .unwrap();

    assert_eq!(resolve.inputs.len(), 1);
    assert_eq!(resolve.outputs.len(), 2);
    assert_eq!(
        resolve.get_output_with_name("id").unwrap().ty,
        SqlType::Integer
    );
    assert_eq!(
        resolve.get_output_with_name("name").unwrap().ty,
        SqlType::Text
    );
}

#[test]
fn update_with_returning_table_alias_wildcard() {
    let mut sim = Simulator::default();
    sim.execute("create table person (id int, name text, age int)")
        .unwrap();
    let resolve = sim
        .execute("update person as p set name = ? returning p.*")
        .unwrap();

    assert_eq!(resolve.inputs.len(), 1);
    assert_eq!(resolve.outputs.len(), 3);
    assert_eq!(
        resolve.get_output_with_name("id").unwrap().ty,
        SqlType::Integer
    );
    assert_eq!(
        resolve.get_output_with_name("name").unwrap().ty,
        SqlType::Text
    );
    assert_eq!(
        resolve.get_output_with_name("age").unwrap().ty,
        SqlType::Integer
    );
}

#[test]
fn update_with_returning_expression() {
    let mut sim = Simulator::default();
    sim.execute("create table person (id int, name text, age int)")
        .unwrap();
    let resolve = sim
        .execute("update person set age = age + ? returning id, age + 1 as next_age")
        .unwrap();

    assert_eq!(resolve.inputs.len(), 1);
    assert_eq!(resolve.outputs.len(), 2);
    assert_eq!(
        resolve.get_output_with_name("id").unwrap().ty,
        SqlType::Integer
    );
    assert_eq!(
        resolve.get_output_with_name("next_age").unwrap().ty,
        SqlType::Integer
    );
}

#[test]
fn update_with_returning_nonexistent_column() {
    let mut sim = Simulator::default();
    sim.execute("create table person (id int, name text)")
        .unwrap();
    assert_eq!(
        sim.execute("update person set name = ? returning nonexistent"),
        Err(Error::ColumnDoesntExist("nonexistent".to_string()))
    );
}

#[test]
fn update_with_returning_invalid_qualifier() {
    let mut sim = Simulator::default();
    sim.execute("create table person (id int, name text)")
        .unwrap();
    assert_eq!(
        sim.execute("update person set name = ? returning other_table.id"),
        Err(Error::QualifiedColumnDoesntExist {
            qualifier: "other_table".to_string(),
            column: "id".to_string()
        })
    );
}

#[test]
fn update_with_join_and_returning() {
    let mut sim = Simulator::default();
    sim.execute("create table person (id int, name text, department_id int)")
        .unwrap();
    sim.execute("create table department (id int, budget int)")
        .unwrap();
    let resolve = sim
        .execute("update person set name = ? from department where person.department_id = department.id returning person.id, person.name")
        .unwrap();

    assert_eq!(resolve.inputs.len(), 1);
    assert_eq!(resolve.outputs.len(), 2);
    assert_eq!(
        resolve.get_output_with_name("id").unwrap().ty,
        SqlType::Integer
    );
    assert_eq!(
        resolve.get_output_with_name("name").unwrap().ty,
        SqlType::Text
    );
}
