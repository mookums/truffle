use crate::ty::SqlType;

#[derive(Debug, Clone)]
pub struct Column {
    pub ty: SqlType,
    pub nullable: bool,
    pub default: bool,
}

impl Column {
    pub fn get_ty(&self) -> &SqlType {
        &self.ty
    }
}
