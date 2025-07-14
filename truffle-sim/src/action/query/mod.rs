pub mod select;

use sqlparser::ast::{Query, SetExpr};
use tracing::warn;

use crate::{Error, Simulator};

impl Simulator {
    pub(crate) fn query(&self, query: Box<Query>) -> Result<(), Error> {
        if let SetExpr::Select(select) = *query.body {
            self.select(&select)?;
        } else {
            warn!(query_type = %query.body, "Unsupported Query");
        }

        Ok(())
    }
}
