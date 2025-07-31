pub mod select;

use sqlparser::{
    ast::{Query, SetExpr},
    dialect::Dialect,
};
use tracing::warn;

use crate::{Error, Simulator};

impl<D: Dialect> Simulator<D> {
    pub(crate) fn query(&self, query: Box<Query>) -> Result<(), Error> {
        if let SetExpr::Select(select) = *query.body {
            self.select(&select)?;
        } else {
            warn!(query_type = %query.body, "Unsupported Query");
        }

        Ok(())
    }
}
