use sqlparser::ast::{
    Assignment, AssignmentTarget, Expr, SelectItem, SqliteOnConflict, TableFactor, TableWithJoins,
    UpdateTableFromKind,
};

use crate::{
    Error, Simulator,
    action::join::JoinInferrer,
    expr::{ColumnInferrer, InferContext},
    object_name_to_strings,
    resolve::ResolvedQuery,
    ty::SqlType,
};

impl Simulator {
    pub(crate) fn update(
        &self,
        table: TableWithJoins,
        assignments: Vec<Assignment>,
        from: Option<UpdateTableFromKind>,
        selection: Option<Expr>,
        _: Option<Vec<SelectItem>>,
        _: Option<SqliteOnConflict>,
    ) -> Result<ResolvedQuery, Error> {
        let mut resolved = ResolvedQuery::default();

        let TableFactor::Table { name, alias, .. } = &table.relation else {
            return Err(Error::Unsupported(
                "Unsupported SELECT relation".to_string(),
            ));
        };

        let table_name = &object_name_to_strings(name)[0];
        let table_alias = alias.as_ref().map(|a| &a.name.value);

        // Ensure the table exists.
        let update_table = self
            .get_table(table_name)
            .ok_or_else(|| Error::TableDoesntExist(table_name.clone()))?;

        // Ensure that the alias isn't a table name.
        if let Some(alias) = table_alias {
            if self.has_table(alias) {
                return Err(Error::AliasIsTableName(alias.to_string()));
            }
        }

        let mut contexts = Vec::new();

        let join_ctx = self.infer_joins(
            update_table,
            table_name,
            table_alias,
            &table.joins,
            &mut resolved,
        )?;

        contexts.push(join_ctx);

        if let Some(from) = from {
            match from {
                UpdateTableFromKind::BeforeSet(_) => todo!(),
                UpdateTableFromKind::AfterSet(items) => {
                    for item in items {
                        let TableFactor::Table { name, alias, .. } = &item.relation else {
                            return Err(Error::Unsupported(
                                "Unsupported table relation".to_string(),
                            ));
                        };

                        let join_table_name = &object_name_to_strings(name)[0];
                        let join_table_alias = alias.as_ref().map(|a| &a.name.value);

                        let join_table = self
                            .get_table(join_table_name)
                            .ok_or_else(|| Error::TableDoesntExist(join_table_name.clone()))?;

                        // Ensure that the alias isn't a table name.
                        if let Some(alias) = table_alias {
                            if self.has_table(alias) {
                                return Err(Error::AliasIsTableName(alias.to_string()));
                            }
                        }

                        let ctx = self.infer_joins(
                            join_table,
                            join_table_name,
                            join_table_alias,
                            &table.joins,
                            &mut resolved,
                        )?;

                        contexts.push(ctx);
                    }
                }
            }
        }

        let inferrer = JoinInferrer {
            join_contexts: &contexts,
        };

        for assignment in assignments {
            match assignment.target {
                AssignmentTarget::ColumnName(object_name) => {
                    let name = &object_name_to_strings(&object_name)[0];
                    let update_column = inferrer
                        .infer_unqualified_column(self, name)?
                        .ok_or_else(|| Error::ColumnDoesntExist(name.to_string()))?;

                    self.infer_expr_column(
                        &assignment.value,
                        InferContext::with_type(update_column.ty),
                        &inferrer,
                        &mut resolved,
                    )?;
                }
                AssignmentTarget::Tuple(object_names) => {
                    let names: Vec<_> = object_names
                        .into_iter()
                        .map(|on| object_name_to_strings(&on)[0].clone())
                        .collect();

                    todo!()
                }
            }
        }

        // TODO: Support Returning
        // Specficially for Postgres, MySQL and SQL Server

        if let Some(selection) = selection {
            self.infer_expr_column(
                &selection,
                InferContext::with_type(SqlType::Boolean),
                &inferrer,
                &mut resolved,
            )?;
        }

        Ok(resolved)
    }
}

#[cfg(test)]
mod tests {
    use crate::*;

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
}
