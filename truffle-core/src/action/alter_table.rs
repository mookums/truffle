use crate::{Error, Simulator, column::Column, object_name_to_strings, ty::SqlType};
use sqlparser::ast::{AlterTableOperation, ColumnOption, ObjectName};
use tracing::debug;

use crate::resolve::ResolvedQuery;

impl Simulator {
    pub(crate) fn alter_table(
        &mut self,
        name: ObjectName,
        operations: Vec<AlterTableOperation>,
    ) -> Result<ResolvedQuery, Error> {
        let table_name = &object_name_to_strings(&name)[0];

        let table = self
            .tables
            .get_mut(table_name)
            .ok_or_else(|| Error::TableDoesntExist(table_name.to_string()))?;

        for operation in operations {
            match operation {
                AlterTableOperation::AddColumn { column_def, .. } => {
                    let column_name = &column_def.name.value;

                    if table.columns.contains_key(column_name) {
                        return Err(Error::ColumnAlreadyExists(column_name.to_string()));
                    }

                    let ty: SqlType = column_def.data_type.into();
                    let mut nullable = true;
                    let mut default = false;

                    for option in column_def.options {
                        match option.option {
                            ColumnOption::Null => nullable = true,
                            ColumnOption::NotNull => nullable = false,
                            ColumnOption::Default(_) => default = true,
                            _ => {
                                return Err(Error::Unsupported(format!(
                                    "Unsupported option in ALTER TABLE ADD COLUMN: {option:#?}"
                                )));
                            }
                        }
                    }

                    table.columns.insert(
                        column_name.to_string(),
                        Column {
                            ty,
                            nullable,
                            default,
                        },
                    );
                }

                AlterTableOperation::DropColumn {
                    column_name,
                    if_exists,
                    ..
                } => {
                    let name = &column_name.value;
                    if !table.columns.contains_key(name) {
                        if if_exists {
                            continue;
                        }
                        return Err(Error::ColumnDoesntExist(name.to_string()));
                    }

                    table.columns.shift_remove(name);
                }

                AlterTableOperation::RenameColumn {
                    old_column_name,
                    new_column_name,
                } => {
                    let old = &old_column_name.value;
                    let new = &new_column_name.value;

                    if !table.columns.contains_key(old) {
                        return Err(Error::ColumnDoesntExist(old.to_string()));
                    }
                    if table.columns.contains_key(new) {
                        return Err(Error::ColumnAlreadyExists(new.to_string()));
                    }

                    let index = table.columns.get_index_of(old).unwrap();
                    table.columns.replace_index(index, new.to_string()).unwrap();
                }

                _ => {
                    return Err(Error::Unsupported(format!(
                        "Unsupported ALTER TABLE operation: {operation:#?}"
                    )));
                }
            }
        }

        debug!(name = %table_name, "Altered Table");
        Ok(ResolvedQuery::default())
    }
}
