use sqlparser::ast::{Delete, FromTable, TableFactor};

use crate::{
    Error, Simulator,
    expr::{InferConstraints, InferContext},
    object_name_to_strings,
    resolve::ResolvedQuery,
    ty::SqlType,
};

use super::join::JoinInferrer;

impl Simulator {
    pub(crate) fn delete(&self, delete: Delete) -> Result<ResolvedQuery, Error> {
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
                    let from_table_name = &object_name_to_strings(name)[0];
                    let from_table_alias = alias.as_ref().map(|a| &a.name.value);

                    let from_table = self
                        .get_table(from_table_name)
                        .ok_or_else(|| Error::TableDoesntExist(from_table_name.clone()))?;

                    if let Some(alias) = &from_table_alias
                        && self.has_table(alias)
                    {
                        return Err(Error::AliasIsTableName(alias.to_string()));
                    }

                    let join_table = self.infer_joins(
                        from_table,
                        from_table_name,
                        from_table_alias,
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
            let infer = self.infer_expr_column(
                &selection,
                InferContext {
                    constraints: InferConstraints {
                        ty: Some(SqlType::Boolean),
                        ..Default::default()
                    },
                    ..Default::default()
                },
                &inferrer,
                &mut resolved,
            )?;

            if infer.column.ty != SqlType::Boolean {
                return Err(Error::TypeMismatch {
                    expected: SqlType::Boolean,
                    got: infer.column.ty,
                });
            }
        }

        Ok(resolved)
    }
}
