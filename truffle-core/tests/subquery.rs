use truffle::{Error, Simulator, ty::SqlType};

#[test]
fn select_with_scalar_subquery() {
    let mut sim = Simulator::default();
    sim.execute("create table department (id int primary key, name text not null)")
        .unwrap();
    sim.execute("create table employee (id int primary key, name text not null, dept_id int)")
        .unwrap();

    let resolve = sim
        .execute(
            "select name from employee where dept_id = (select id from department where name = $1)",
        )
        .unwrap();

    assert_eq!(resolve.inputs.len(), 1);
    assert_eq!(resolve.get_input(0).unwrap().ty, SqlType::Text);
    assert_eq!(resolve.outputs.len(), 1);
    assert_eq!(resolve.outputs.iter().next().unwrap().1.ty, SqlType::Text);
}

// #[test]
// fn select_with_subquery_in_select_clause() {
//     let mut sim = Simulator::default();
//     sim.execute("create table department (id int primary key, name text not null)")
//         .unwrap();
//     sim.execute("create table employee (id int primary key, name text not null, dept_id int)")
//         .unwrap();

//     let resolve = sim
//         .execute("select name, (select name from department where id = employee.dept_id) as dept_name from employee where id = $1")
//         .unwrap();

//     assert_eq!(resolve.inputs.len(), 1);
//     assert_eq!(resolve.get_input(0).unwrap().ty, SqlType::Integer);
//     assert_eq!(resolve.outputs.len(), 2);
//     assert_eq!(
//         resolve.get_output_with_name("name").unwrap().ty,
//         SqlType::Text
//     );
//     assert_eq!(
//         resolve.get_output_with_name("dept_name").unwrap().ty,
//         SqlType::Text
//     );
// }

// #[test]
// fn select_with_exists_subquery() {
//     let mut sim = Simulator::default();
//     sim.execute("create table department (id int primary key, name text not null)")
//         .unwrap();
//     sim.execute("create table employee (id int primary key, name text not null, dept_id int)")
//         .unwrap();

//     let resolve = sim
//         .execute("select name from department where exists (select 1 from employee where dept_id = department.id and name = $1)")
//         .unwrap();

//     assert_eq!(resolve.inputs.len(), 1);
//     assert_eq!(resolve.get_input(0).unwrap().ty, SqlType::Text);
//     assert_eq!(resolve.outputs.len(), 1);
//     assert_eq!(resolve.outputs.iter().next().unwrap().1.ty, SqlType::Text);
// }

// #[test]
// fn select_with_in_subquery() {
//     let mut sim = Simulator::default();
//     sim.execute("create table department (id int primary key, name text not null)")
//         .unwrap();
//     sim.execute("create table employee (id int primary key, name text not null, dept_id int)")
//         .unwrap();

//     let resolve = sim
//         .execute("select name from employee where dept_id in (select id from department where name = $1)")
//         .unwrap();

//     assert_eq!(resolve.inputs.len(), 1);
//     assert_eq!(resolve.get_input(0).unwrap().ty, SqlType::Text);
//     assert_eq!(resolve.outputs.len(), 1);
//     assert_eq!(resolve.outputs.iter().next().unwrap().1.ty, SqlType::Text);
// }

#[test]
fn select_with_subquery_column_doesnt_exist() {
    let mut sim = Simulator::default();
    sim.execute("create table department (id int primary key, name text not null)")
        .unwrap();
    sim.execute("create table employee (id int primary key, name text not null, dept_id int)")
        .unwrap();

    assert_eq!(
        sim.execute("select name from employee where dept_id = (select nonexistent from department where name = $1)"),
        Err(Error::ColumnDoesntExist("nonexistent".to_string()))
    );
}

#[test]
fn select_with_multiple_column_subquery_in_tuple() {
    let mut sim = Simulator::default();
    sim.execute("create table person (id int primary key, first_name text not null, last_name text not null)").unwrap();
    sim.execute("create table contact (person_id int, first_name text, last_name text)")
        .unwrap();

    let resolve = sim
        .execute("select id from person where (first_name, last_name) = (select first_name, last_name from contact where person_id = $1)")
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
// fn select_with_correlated_subquery() {
//     let mut sim = Simulator::default();
//     sim.execute(
//         "create table employee (id int primary key, name text not null, salary int, dept_id int)",
//     )
//     .unwrap();

//     let resolve = sim
//         .execute("select name from employee e1 where salary > (select avg(salary) from employee e2 where e2.dept_id = e1.dept_id)")
//         .unwrap();

//     assert_eq!(resolve.inputs.len(), 0);
//     assert_eq!(resolve.outputs.len(), 1);
//     assert_eq!(resolve.outputs.iter().next().unwrap().1.ty, SqlType::Text);
// }

#[test]
fn select_with_subquery_table_doesnt_exist() {
    let mut sim = Simulator::default();
    sim.execute("create table employee (id int primary key, name text not null, dept_id int)")
        .unwrap();

    assert_eq!(
        sim.execute("select name from employee where dept_id = (select id from nonexistent_table where name = $1)"),
        Err(Error::TableDoesntExist("nonexistent_table".to_string()))
    );
}
