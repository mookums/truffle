use sqlparser::ast::{
    Expr, Insert, SelectItem, SelectItemQualifiedWildcardKind, SetExpr, TableObject,
};

use crate::{
    Error, Simulator,
    expr::{ColumnInferrer, InferType},
    object_name_to_strings,
    resolve::{ResolveOutputKey, ResolvedQuery},
    ty::SqlType,
};

impl Simulator {
    pub(crate) fn insert(&self, ins: Insert) -> Result<ResolvedQuery, Error> {
        let TableObject::TableName(table_object_name) = ins.table else {
            todo!();
        };

        // Only POSTGRES uses this.
        let alias = ins.table_alias.map(|i| i.value);
        let table_name = &object_name_to_strings(&table_object_name)[0];

        let table = self
            .get_table(table_name)
            .ok_or_else(|| Error::TableDoesntExist(table_name.clone()))?;

        let mut provided_columns = vec![];
        for column in ins.columns {
            let column_name = column.value;
            if !table.has_column(&column_name) {
                return Err(Error::ColumnDoesntExist(column_name));
            }

            provided_columns.push(column_name);
        }

        // This stores the return information for this query.
        let mut resolved = ResolvedQuery::default();
        let inferrer = InsertInferrer::default();

        let source = ins.source.unwrap();
        match *source.body {
            SetExpr::Values(values) => {
                for row in values.rows {
                    // Ensure we have the correct number of columns.
                    if provided_columns.is_empty() {
                        if table.columns.len() != row.len() {
                            return Err(Error::ColumnCountMismatch {
                                expected: table.columns.len(),
                                got: row.len(),
                            });
                        }
                    } else if provided_columns.len() != row.len() {
                        return Err(Error::ColumnCountMismatch {
                            expected: provided_columns.len(),
                            got: row.len(),
                        });
                    }

                    for (i, (column_name, column)) in table.columns.iter().enumerate() {
                        if provided_columns.is_empty() {
                            // Implicit (Table Index) Columns.
                            let expr = &row[i];

                            _ = self.infer_expr_type(
                                expr,
                                InferType::Required(column.ty.clone()),
                                &inferrer,
                                &mut resolved,
                            )?;
                        } else if let Some(index) =
                            provided_columns.iter().position(|pc| pc == column_name)
                        {
                            // If the column was named explicitly...
                            let expr = &row[index];

                            _ = self.infer_expr_type(
                                expr,
                                InferType::Required(column.ty.clone()),
                                &inferrer,
                                &mut resolved,
                            )?;
                        } else if !(column.nullable || column.default) {
                            // If the column was not named explicitly, we check it.
                            return Err(Error::RequiredColumnMissing(column_name.to_string()));
                        }
                    }
                }
            }
            _ => todo!("Unexpected body for INSERT"),
        }

        if let Some(returning) = ins.returning {
            for item in returning {
                match item {
                    SelectItem::UnnamedExpr(expr) => match expr {
                        Expr::Identifier(ident) => {
                            let value = ident.value.clone();

                            if let Some(column) = table.get_column(&value) {
                                resolved.insert_output(
                                    ResolveOutputKey {
                                        qualifier: Some(table_name.clone()),
                                        name: value,
                                    },
                                    column.clone(),
                                );
                            } else {
                                return Err(Error::ColumnDoesntExist(value.to_string()));
                            }
                        }
                        Expr::CompoundIdentifier(idents) => {
                            let qualifier = &idents.first().unwrap().value;
                            let column_name = &idents.get(1).unwrap().value;

                            if qualifier == table_name
                                || alias.as_ref().is_some_and(|a| a == qualifier)
                            {
                                let column = table.get_column(column_name).ok_or_else(|| {
                                    Error::ColumnDoesntExist(column_name.to_string())
                                })?;

                                resolved.insert_output(
                                    ResolveOutputKey {
                                        qualifier: Some(qualifier.clone()),
                                        name: column_name.clone(),
                                    },
                                    column.clone(),
                                );
                            } else {
                                return Err(Error::QualifierDoesntExist(qualifier.to_string()));
                            }
                        }
                        _ => {
                            return Err(Error::Unsupported(format!(
                                "Unsupported Select Expr: {expr:?}"
                            )));
                        }
                    },
                    SelectItem::ExprWithAlias { expr, alias } => {
                        return Err(Error::Unsupported(format!(
                            "Unsupported Select Expr with Alias: expr={expr}, alias={alias}"
                        )));
                    }
                    SelectItem::QualifiedWildcard(kind, _) => match kind {
                        SelectItemQualifiedWildcardKind::ObjectName(name) => {
                            let qualifier = &object_name_to_strings(&name)[0];

                            if qualifier == table_name
                                || alias.as_ref().is_some_and(|a| a == qualifier)
                            {
                                for column in table.columns.iter() {
                                    resolved.insert_output(
                                        ResolveOutputKey {
                                            qualifier: Some(qualifier.clone()),
                                            name: column.0.to_string(),
                                        },
                                        column.1.clone(),
                                    );
                                }
                            } else {
                                return Err(Error::QualifierDoesntExist(qualifier.to_string()));
                            }
                        }
                        SelectItemQualifiedWildcardKind::Expr(_) => {
                            return Err(Error::Unsupported(
                                "Expression as qualifier for wildcard in SELECT".to_string(),
                            ));
                        }
                    },

                    SelectItem::Wildcard(_) => {
                        for column in table.columns.iter() {
                            resolved.insert_output(
                                ResolveOutputKey {
                                    qualifier: Some(table_name.clone()),
                                    name: column.0.to_string(),
                                },
                                column.1.clone(),
                            );
                        }
                    }
                }
            }
        }

        Ok(resolved)
    }
}

#[derive(Default)]
struct InsertInferrer {}

impl ColumnInferrer for InsertInferrer {
    fn infer_unqualified_type(&self, _: &Simulator, _: &str) -> Result<Option<SqlType>, Error> {
        Err(Error::Unsupported(
            "Can't infer values in INSERT".to_string(),
        ))
    }

    fn infer_qualified_type(&self, _: &Simulator, _: &str, _: &str) -> Result<SqlType, Error> {
        Err(Error::Unsupported(
            "Can't infer values in INSERT".to_string(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use crate::*;

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
        sim.execute("create table person (id integer not null, name text not null, weight integer default 10)").unwrap();

        let resolve = sim
            .execute("insert into person (id, name) values(?, ?)")
            .unwrap();

        assert_eq!(resolve.inputs.len(), 2);
        assert_eq!(resolve.get_input(0).unwrap(), &SqlType::Integer);
        assert_eq!(resolve.get_input(1).unwrap(), &SqlType::Text);
    }

    #[test]
    fn insert_resolved_inputs_numbered() {
        let mut sim = Simulator::default();
        sim.execute("create table person (id integer not null, name text not null, weight float default 10.2)").unwrap();

        let resolve = sim
            .execute("insert into person (id, name, weight) values($3, $1, $2)")
            .unwrap();

        assert_eq!(resolve.inputs.len(), 3);
        assert_eq!(resolve.get_input(0).unwrap(), &SqlType::Text);
        assert_eq!(resolve.get_input(1).unwrap(), &SqlType::Float);
        assert_eq!(resolve.get_input(2).unwrap(), &SqlType::Integer);
    }

    #[test]
    fn insert_resolved_inputs_numbered_repeating() {
        let mut sim = Simulator::default();
        sim.execute("create table person (id integer not null, name text not null, age integer not null, weight float default 10.2)").unwrap();

        let resolve = sim
            .execute("insert into person (id, name, weight, age) values($3, $1, $2, $2)")
            .unwrap();

        assert_eq!(resolve.inputs.len(), 3);
        assert_eq!(resolve.get_input(0).unwrap(), &SqlType::Text);
        assert_eq!(resolve.get_input(1).unwrap(), &SqlType::Float);
        assert_eq!(resolve.get_input(2).unwrap(), &SqlType::Integer);
    }

    #[test]
    fn insert_with_returning_wildcard() {
        let mut sim = Simulator::default();
        sim.execute("create table person (id integer not null, name text not null, weight float default 10.2)").unwrap();

        let resolve = sim
            .execute("insert into person (id, name, weight) values($1, $2, $3) returning *")
            .unwrap();

        assert_eq!(resolve.inputs.len(), 3);
        assert_eq!(resolve.get_input(0).unwrap(), &SqlType::Integer);
        assert_eq!(resolve.get_input(1).unwrap(), &SqlType::Text);
        assert_eq!(resolve.get_input(2).unwrap(), &SqlType::Float);

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
        sim.execute("create table person (id integer not null, name text not null, weight float default 10.2)").unwrap();

        let resolve = sim
            .execute("insert into person (id, name, weight) values($1, $2, $3) returning person.*")
            .unwrap();

        assert_eq!(resolve.inputs.len(), 3);
        assert_eq!(resolve.get_input(0).unwrap(), &SqlType::Integer);
        assert_eq!(resolve.get_input(1).unwrap(), &SqlType::Text);
        assert_eq!(resolve.get_input(2).unwrap(), &SqlType::Float);

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
        sim.execute("create table person (id integer not null, name text not null, weight float default 10.2)").unwrap();

        let resolve = sim
            .execute("insert into person (id, name, weight) values($1, $2, $3) returning id")
            .unwrap();

        assert_eq!(resolve.inputs.len(), 3);
        assert_eq!(resolve.get_input(0).unwrap(), &SqlType::Integer);
        assert_eq!(resolve.get_input(1).unwrap(), &SqlType::Text);
        assert_eq!(resolve.get_input(2).unwrap(), &SqlType::Float);

        assert_eq!(resolve.outputs.len(), 1);
        assert_eq!(
            resolve.get_output_with_name("id").unwrap().ty,
            SqlType::Integer
        );
    }

    #[test]
    fn insert_with_returning_qualified_single() {
        let mut sim = Simulator::default();
        sim.execute("create table person (id integer not null, name text not null, weight float default 10.2)").unwrap();

        let resolve = sim
            .execute("insert into person (id, name, weight) values($1, $2, $3) returning person.id")
            .unwrap();

        assert_eq!(resolve.inputs.len(), 3);
        assert_eq!(resolve.get_input(0).unwrap(), &SqlType::Integer);
        assert_eq!(resolve.get_input(1).unwrap(), &SqlType::Text);
        assert_eq!(resolve.get_input(2).unwrap(), &SqlType::Float);

        assert_eq!(resolve.outputs.len(), 1);
        assert_eq!(
            resolve.get_output_with_name("id").unwrap().ty,
            SqlType::Integer
        );
    }

    #[test]
    fn insert_with_returning_aliased_wildcard_postgres() {
        let mut sim = Simulator::with_dialect(DialectKind::Postgres);
        sim.execute("create table person (id integer not null, name text not null, weight float default 10.2)").unwrap();

        let resolve = sim
            .execute("insert into person as p (id, name, weight) values($1, $2, $3) returning p.*")
            .unwrap();

        assert_eq!(resolve.inputs.len(), 3);
        assert_eq!(resolve.get_input(0).unwrap(), &SqlType::Integer);
        assert_eq!(resolve.get_input(1).unwrap(), &SqlType::Text);
        assert_eq!(resolve.get_input(2).unwrap(), &SqlType::Float);

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
        sim.execute("create table person (id integer not null, name text not null, weight float default 10.2)").unwrap();

        let resolve = sim
            .execute("insert into person as p (id, name, weight) values($1, $2, $3) returning p.id, p.weight")
            .unwrap();

        assert_eq!(resolve.inputs.len(), 3);
        assert_eq!(resolve.get_input(0).unwrap(), &SqlType::Integer);
        assert_eq!(resolve.get_input(1).unwrap(), &SqlType::Text);
        assert_eq!(resolve.get_input(2).unwrap(), &SqlType::Float);

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
}
