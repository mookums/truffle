use truffle::{DialectKind, Error, Simulator, ty::SqlType};

#[test]
fn insert_table_by_column_index() {
    let mut sim = Simulator::default();
    sim.execute("create table person (id integer not null, name text, weight real);")
        .unwrap();
    sim.execute("insert into person values (10, 'John Doe', ?)")
        .unwrap();
}

#[test]
fn insert_table_by_column_name() {
    let mut sim = Simulator::default();
    sim.execute("create table person (id integer not null, name text, weight real);")
        .unwrap();
    sim.execute("insert into person (weight, name, id) values (221.9, 'John Doe', 10)")
        .unwrap();
}

#[test]
fn insert_column_count_mismatch() {
    let mut sim = Simulator::default();
    sim.execute("create table person (id integer not null, name text, weight real);")
        .unwrap();

    assert!(
        sim.execute(
            "insert into person (weight, name, id) values (221.9, 'John Doe', 10, 'abc', 'def')"
        )
        .is_err_and(|e| e
            == Error::ColumnCountMismatch {
                expected: 3,
                got: 5
            })
    );
}

#[test]
fn insert_column_type_mismatch() {
    let mut sim = Simulator::default();
    sim.execute("create table person (id integer not null, name text, weight real);")
        .unwrap();

    assert!(
        sim.execute("insert into person (id, name, weight) values ('id', 'John Doe', 12.1)")
            .is_err_and(|e| e
                == Error::TypeMismatch {
                    expected: SqlType::Integer,
                    got: SqlType::Text
                })
    );
}

#[test]
fn insert_column_doesnt_exist() {
    let mut sim = Simulator::default();
    sim.execute("create table person (id integer not null, name text, weight real);")
        .unwrap();

    assert!(
        sim.execute("insert into person (id, name, height) values (100, 'John Doe', 12.123);")
            .is_err_and(|e| e == Error::ColumnDoesntExist("height".to_string()))
    );
}

#[test]
fn insert_multiple_rows_success() {
    let mut sim = Simulator::default();
    sim.execute("create table person (id integer, name text);")
        .unwrap();
    sim.execute("insert into person values (1, 'John'), (2, 'Jane'), (3, 'Bob')")
        .unwrap();
}

#[test]
fn insert_multiple_rows_type_error() {
    let mut sim = Simulator::default();
    sim.execute("create table person (id integer, name text);")
        .unwrap();

    assert!(
        sim.execute("insert into person values (1, 'John'), ('bad_id', 'Jane'), (3, 'Bob')")
            .is_err_and(|e| e
                == Error::TypeMismatch {
                    expected: SqlType::Integer,
                    got: SqlType::Text
                })
    )
}

#[test]
fn insert_multiple_rows_count_mismatch() {
    let mut sim = Simulator::default();
    sim.execute("create table person (id integer, name text);")
        .unwrap();

    assert_eq!(
        sim.execute("insert into person values (1, 'John'), (2, 'Jane'), (3, 'Bob', 'abc')"),
        Err(Error::ColumnCountMismatch {
            expected: 2,
            got: 3
        })
    )
}

#[test]
fn insert_partial_columns_success() {
    let mut sim = Simulator::default();
    sim.execute("create table person (id integer, name text, weight real);")
        .unwrap();
    sim.execute("insert into person (id, name) values (1, 'John')")
        .unwrap();
}

#[test]
fn insert_missing_required_column() {
    let mut sim = Simulator::default();
    sim.execute(
        "create table person (id integer not null, name text not null, weight real not null);",
    )
    .unwrap();

    assert_eq!(
        sim.execute("insert into person (id, name) values (1, 'John')"),
        Err(Error::RequiredColumnMissing("weight".to_string()))
    );
}

#[test]
fn insert_resolved_inputs() {
    let mut sim = Simulator::default();
    sim.execute(
        "create table person (id integer not null, name text not null, weight integer default 10)",
    )
    .unwrap();

    let resolve = sim
        .execute("insert into person (id, name) values(?, ?)")
        .unwrap();

    assert_eq!(resolve.inputs.len(), 2);
    assert_eq!(resolve.get_input(0).unwrap().ty, SqlType::Integer);
    assert_eq!(resolve.get_input(1).unwrap().ty, SqlType::Text);
}

#[test]
fn insert_resolved_inputs_numbered() {
    let mut sim = Simulator::default();
    sim.execute(
        "create table person (id integer not null, name text not null, weight float default 10.2)",
    )
    .unwrap();

    let resolve = sim
        .execute("insert into person (id, name, weight) values($3, $1, $2)")
        .unwrap();

    assert_eq!(resolve.inputs.len(), 3);
    assert_eq!(resolve.get_input(0).unwrap().ty, SqlType::Text);
    assert_eq!(resolve.get_input(1).unwrap().ty, SqlType::Float);
    assert_eq!(resolve.get_input(2).unwrap().ty, SqlType::Integer);
}

#[test]
fn insert_resolved_inputs_numbered_repeating() {
    let mut sim = Simulator::default();
    sim.execute("create table person (id integer not null, name text not null, age integer not null, weight float default 10.2)").unwrap();

    let resolve = sim
        .execute("insert into person (id, name, weight, age) values($3, $1, $2, $2)")
        .unwrap();

    assert_eq!(resolve.inputs.len(), 3);
    assert_eq!(resolve.get_input(0).unwrap().ty, SqlType::Text);
    assert_eq!(resolve.get_input(1).unwrap().ty, SqlType::Float);
    assert_eq!(resolve.get_input(2).unwrap().ty, SqlType::Integer);
}

#[test]
fn insert_with_returning_wildcard() {
    let mut sim = Simulator::default();
    sim.execute(
        "create table person (id integer not null, name text not null, weight float default 10.2)",
    )
    .unwrap();

    let resolve = sim
        .execute("insert into person (id, name, weight) values($1, $2, $3) returning *")
        .unwrap();

    assert_eq!(resolve.inputs.len(), 3);
    assert_eq!(resolve.get_input(0).unwrap().ty, SqlType::Integer);
    assert_eq!(resolve.get_input(1).unwrap().ty, SqlType::Text);
    assert_eq!(resolve.get_input(2).unwrap().ty, SqlType::Float);

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
        resolve.get_output_with_name("weight").unwrap().ty,
        SqlType::Float
    );
}

#[test]
fn insert_with_returning_qualified_wildcard() {
    let mut sim = Simulator::default();
    sim.execute(
        "create table person (id integer not null, name text not null, weight float default 10.2)",
    )
    .unwrap();

    let resolve = sim
        .execute("insert into person (id, name, weight) values($1, $2, $3) returning person.*")
        .unwrap();

    assert_eq!(resolve.inputs.len(), 3);
    assert_eq!(resolve.get_input(0).unwrap().ty, SqlType::Integer);
    assert_eq!(resolve.get_input(1).unwrap().ty, SqlType::Text);
    assert_eq!(resolve.get_input(2).unwrap().ty, SqlType::Float);

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
        resolve.get_output_with_name("weight").unwrap().ty,
        SqlType::Float
    );
}

#[test]
fn insert_with_returning_single() {
    let mut sim = Simulator::default();
    sim.execute(
        "create table person (id integer not null, name text not null, weight float default 10.2)",
    )
    .unwrap();

    let resolve = sim
        .execute("insert into person (id, name, weight) values($1, $2, $3) returning id")
        .unwrap();

    assert_eq!(resolve.inputs.len(), 3);
    assert_eq!(resolve.get_input(0).unwrap().ty, SqlType::Integer);
    assert_eq!(resolve.get_input(1).unwrap().ty, SqlType::Text);
    assert_eq!(resolve.get_input(2).unwrap().ty, SqlType::Float);

    assert_eq!(resolve.outputs.len(), 1);
    assert_eq!(
        resolve.get_output_with_name("id").unwrap().ty,
        SqlType::Integer
    );
}

#[test]
fn insert_with_returning_qualified_single() {
    let mut sim = Simulator::default();
    sim.execute(
        "create table person (id integer not null, name text not null, weight float default 10.2)",
    )
    .unwrap();

    let resolve = sim
        .execute("insert into person (id, name, weight) values($1, $2, $3) returning person.id")
        .unwrap();

    assert_eq!(resolve.inputs.len(), 3);
    assert_eq!(resolve.get_input(0).unwrap().ty, SqlType::Integer);
    assert_eq!(resolve.get_input(1).unwrap().ty, SqlType::Text);
    assert_eq!(resolve.get_input(2).unwrap().ty, SqlType::Float);

    assert_eq!(resolve.outputs.len(), 1);
    assert_eq!(
        resolve.get_output_with_name("id").unwrap().ty,
        SqlType::Integer
    );
}

#[test]
fn insert_with_returning_aliased_wildcard_postgres() {
    let mut sim = Simulator::with_dialect(DialectKind::Postgres);
    sim.execute(
        "create table person (id integer not null, name text not null, weight float default 10.2)",
    )
    .unwrap();

    let resolve = sim
        .execute("insert into person as p (id, name, weight) values($1, $2, $3) returning p.*")
        .unwrap();

    assert_eq!(resolve.inputs.len(), 3);
    assert_eq!(resolve.get_input(0).unwrap().ty, SqlType::Integer);
    assert_eq!(resolve.get_input(1).unwrap().ty, SqlType::Text);
    assert_eq!(resolve.get_input(2).unwrap().ty, SqlType::Float);

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
        resolve.get_output_with_name("weight").unwrap().ty,
        SqlType::Float
    );
}

#[test]
fn insert_with_returning_aliased_fields_postgres() {
    let mut sim = Simulator::with_dialect(DialectKind::Postgres);
    sim.execute(
        "create table person (id integer not null, name text not null, weight float default 10.2)",
    )
    .unwrap();

    let resolve = sim
            .execute("insert into person as p (id, name, weight) values($1, $2, $3) returning p.id, p.weight")
            .unwrap();

    assert_eq!(resolve.inputs.len(), 3);
    assert_eq!(resolve.get_input(0).unwrap().ty, SqlType::Integer);
    assert_eq!(resolve.get_input(1).unwrap().ty, SqlType::Text);
    assert_eq!(resolve.get_input(2).unwrap().ty, SqlType::Float);

    assert_eq!(resolve.outputs.len(), 2);
    assert_eq!(
        resolve.get_output_with_name("id").unwrap().ty,
        SqlType::Integer
    );
    assert_eq!(
        resolve.get_output_with_name("weight").unwrap().ty,
        SqlType::Float
    );
}

#[test]
fn insert_with_returning_alias() {
    let mut sim = Simulator::with_dialect(DialectKind::Postgres);
    sim.execute(
        "create table person (id integer not null, name text not null, weight float default 10.2)",
    )
    .unwrap();

    let resolve = sim
            .execute("insert into person as p (id, name, weight) values($1, $2, $3) returning p.id, p.weight as how_heavy")
            .unwrap();

    assert_eq!(resolve.inputs.len(), 3);
    assert_eq!(resolve.get_input(0).unwrap().ty, SqlType::Integer);
    assert_eq!(resolve.get_input(1).unwrap().ty, SqlType::Text);
    assert_eq!(resolve.get_input(2).unwrap().ty, SqlType::Float);

    assert_eq!(resolve.outputs.len(), 2);
    assert_eq!(
        resolve.get_output_with_name("id").unwrap().ty,
        SqlType::Integer
    );
    assert_eq!(
        resolve.get_output_with_name("how_heavy").unwrap().ty,
        SqlType::Float
    );
}
