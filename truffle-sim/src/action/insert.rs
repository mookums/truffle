use sqlparser::ast::{Insert, SetExpr, TableObject};

use crate::{Error, Simulator, expr::ColumnInferrer, object_name_to_strings};

struct InsertInferrer {}

impl ColumnInferrer for InsertInferrer {
    fn infer_unqualified_type(
        &self,
        _: &Simulator,
        _: &str,
    ) -> Result<Option<crate::ty::SqlType>, Error> {
        Err(Error::Unsupported(
            "Can't infer values in INSERT".to_string(),
        ))
    }

    fn infer_qualified_type(
        &self,
        _: &Simulator,
        _: &str,
        _: &str,
    ) -> Result<crate::ty::SqlType, Error> {
        Err(Error::Unsupported(
            "Can't infer values in INSERT".to_string(),
        ))
    }
}

impl Simulator {
    pub(crate) fn insert(&self, ins: Insert) -> Result<(), Error> {
        let TableObject::TableName(table_object_name) = ins.table else {
            todo!();
        };

        let table_name = object_name_to_strings(&table_object_name)
            .first()
            .unwrap()
            .clone();

        let table = self
            .get_table(&table_name)
            .ok_or(Error::TableDoesntExist(table_name))?;

        let mut provided_columns = vec![];
        for column in ins.columns {
            let column_name = column.value;
            if !table.has_column(&column_name) {
                return Err(Error::ColumnDoesntExist(column_name));
            }

            provided_columns.push(column_name);
        }

        let inferrer = InsertInferrer {};

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
                            let ty =
                                self.infer_expr_type(expr, Some(column.ty.clone()), &inferrer)?;

                            if column.ty != ty {
                                return Err(Error::TypeMismatch {
                                    expected: column.ty.clone(),
                                    got: ty,
                                });
                            }
                        } else if let Some(index) =
                            provided_columns.iter().position(|pc| pc == column_name)
                        {
                            // If the column was named explicitly...
                            let expr = &row[index];

                            let ty =
                                self.infer_expr_type(expr, Some(column.ty.clone()), &inferrer)?;

                            if column.ty != ty {
                                return Err(Error::TypeMismatch {
                                    expected: column.ty.clone(),
                                    got: ty,
                                });
                            }
                        } else if !(column.nullable || column.default) {
                            // If the column was not named explicitly, we check it.
                            return Err(Error::RequiredColumnMissing(column_name.to_string()));
                        }
                    }
                }
            }
            _ => todo!(),
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::*;

    #[test]
    fn insert_table_by_column_index() {
        let mut sim = Simulator::new(Box::new(GenericDialect {}));
        sim.execute("create table person (id integer not null, name text, weight real);")
            .unwrap();
        sim.execute("insert into person values (10, 'John Doe', ?)")
            .unwrap();
    }

    #[test]
    fn insert_table_by_column_name() {
        let mut sim = Simulator::new(Box::new(GenericDialect {}));
        sim.execute("create table person (id integer not null, name text, weight real);")
            .unwrap();
        sim.execute("insert into person (weight, name, id) values (221.9, 'John Doe', 10)")
            .unwrap();
    }

    #[test]
    fn insert_column_count_mismatch() {
        let mut sim = Simulator::new(Box::new(GenericDialect {}));
        sim.execute("create table person (id integer not null, name text, weight real);")
            .unwrap();
        assert_eq!(
            sim.execute(
                "insert into person (weight, name, id) values (221.9, 'John Doe', 10, 'abc', 'def')"
            ),
            Err(Error::ColumnCountMismatch {
                expected: 3,
                got: 5
            })
        );
    }

    #[test]
    fn insert_column_type_mismatch() {
        let mut sim = Simulator::new(Box::new(GenericDialect {}));
        sim.execute("create table person (id integer not null, name text, weight real);")
            .unwrap();
        assert_eq!(
            sim.execute("insert into person (id, name, weight) values ('id', 'John Doe', 12.1)"),
            Err(Error::TypeMismatch {
                expected: SqlType::Integer,
                got: SqlType::Text
            })
        );
    }

    #[test]
    fn insert_column_doesnt_exist() {
        let mut sim = Simulator::new(Box::new(GenericDialect {}));
        sim.execute("create table person (id integer not null, name text, weight real);")
            .unwrap();
        assert_eq!(
            sim.execute("insert into person (id, name, height) values (100, 'John Doe', 12.123);"),
            Err(Error::ColumnDoesntExist("height".to_string()))
        );
    }

    #[test]
    fn insert_multiple_rows_success() {
        let mut sim = Simulator::new(Box::new(GenericDialect {}));
        sim.execute("create table person (id integer, name text);")
            .unwrap();
        sim.execute("insert into person values (1, 'John'), (2, 'Jane'), (3, 'Bob')")
            .unwrap();
    }

    #[test]
    fn insert_multiple_rows_type_error() {
        let mut sim = Simulator::new(Box::new(GenericDialect {}));
        sim.execute("create table person (id integer, name text);")
            .unwrap();

        assert_eq!(
            sim.execute("insert into person values (1, 'John'), ('bad_id', 'Jane'), (3, 'Bob')"),
            Err(Error::TypeMismatch {
                expected: SqlType::Integer,
                got: SqlType::Text
            })
        )
    }

    #[test]
    fn insert_multiple_rows_count_mismatch() {
        let mut sim = Simulator::new(Box::new(GenericDialect {}));
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
        let mut sim = Simulator::new(Box::new(GenericDialect {}));
        sim.execute("create table person (id integer, name text, weight real);")
            .unwrap();
        sim.execute("insert into person (id, name) values (1, 'John')")
            .unwrap();
    }

    #[test]
    fn insert_missing_required_column() {
        let mut sim = Simulator::new(Box::new(GenericDialect {}));
        sim.execute(
            "create table person (id integer not null, name text not null, weight real not null);",
        )
        .unwrap();

        assert_eq!(
            sim.execute("insert into person (id, name) values (1, 'John')"),
            Err(Error::RequiredColumnMissing("weight".to_string()))
        );
    }
}
