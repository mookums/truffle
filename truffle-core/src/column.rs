use std::fmt::Display;

use crate::ty::SqlType;

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

#[cfg_attr(feature = "serde", derive(Deserialize, Serialize))]
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
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

impl Display for Column {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.ty)?;

        if self.nullable {
            write!(f, " null")?;
        } else {
            write!(f, " not null")?;
        }

        if self.default {
            write!(f, " default")?;
        }

        Ok(())
    }
}
