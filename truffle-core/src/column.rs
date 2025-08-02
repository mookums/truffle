use crate::ty::SqlType;

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

#[cfg_attr(feature = "serde", derive(Deserialize, Serialize))]
#[derive(Debug, Clone, PartialEq, Eq)]
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
