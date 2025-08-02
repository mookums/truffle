use sqlparser::ast::{Delete, FromTable, TableFactor};

use crate::{
    Error, Simulator, expr::InferType, object_name_to_strings, resolve::ResolvedQuery, ty::SqlType,
};

use super::join::JoinInferrer;

impl Simulator {
    pub(crate) fn delete(&self, delete: Delete) -> Result<(), Error> {
        // TODO: Support multi table deletes (for MySQL)
        let mut contexts = vec![];
        let mut resolved = ResolvedQuery::default();

        match delete.from {
            FromTable::WithFromKeyword(tables_with_joins) => {
                for from in tables_with_joins {
                    // TODO: Remove this duplication.
                    let TableFactor::Table { name, alias, .. } = &from.relation else {
                        return Err(Error::Unsupported(
                            "Unsupported DELETE relation".to_string(),
                        ));
                    };
                    let from_table_name = object_name_to_strings(name).first().unwrap().clone();
                    let from_table_alias = alias.as_ref().map(|a| a.name.value.clone());

                    let from_table = self
                        .get_table(&from_table_name)
                        .ok_or_else(|| Error::TableDoesntExist(from_table_name.clone()))?;

                    if let Some(alias) = &from_table_alias {
                        if self.has_table(alias) {
                            return Err(Error::AliasIsTableName(alias.to_string()));
                        }
                    }

                    let join_table = self.infer_joins(
                        from_table,
                        &from_table_name,
                        from_table_alias.as_ref(),
                        &from.joins,
                        &mut resolved,
                    )?;

                    contexts.push(join_table);
                }
            }
            FromTable::WithoutKeyword(_) => {
                return Err(Error::Unsupported(
                    "DELETE FROM without FROM keyword".to_string(),
                ));
            }
        }

        let inferrer = JoinInferrer {
            join_contexts: &contexts,
        };

        if let Some(selection) = delete.selection {
            let ty = self.infer_expr_type(
                &selection,
                InferType::Required(SqlType::Boolean),
                &inferrer,
                &mut resolved,
            )?;

            if ty != SqlType::Boolean {
                return Err(Error::TypeMismatch {
                    expected: SqlType::Boolean,
                    got: ty,
                });
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::*;

    #[test]
    fn delete_row_by_field() {
        let mut sim = Simulator::new(GenericDialect {});
        sim.execute("create table person (id int primary key, name text)")
            .unwrap();
        sim.execute("delete from person where id = ?").unwrap();
        sim.execute("delete from person where id = 5").unwrap();
    }

    #[test]
    fn delete_row_column_doesnt_exist() {
        let mut sim = Simulator::new(GenericDialect {});
        sim.execute("create table person (id int primary key, name text)")
            .unwrap();

        assert_eq!(
            sim.execute("delete from person where weight = ?"),
            Err(Error::ColumnDoesntExist("weight".to_string()))
        )
    }

    #[test]
    fn delete_row_table_doesnt_exist() {
        let mut sim = Simulator::new(GenericDialect {});

        assert_eq!(
            sim.execute("delete from person where weight = ?"),
            Err(Error::TableDoesntExist("person".to_string()))
        )
    }

    #[test]
    fn delete_row_join() {
        let mut sim = Simulator::new(GenericDialect {});
        sim.execute("create table person (id int primary key, name text)")
            .unwrap();
        sim.execute(
            "create table order (id int primary key, item text not null, address text not null)",
        )
        .unwrap();
        sim.execute("delete from person natural join order where address = ?")
            .unwrap();
    }
}
