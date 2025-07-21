pub mod select;

use select::handle_select_query;
use sqlparser::ast::{Query, SetExpr};
use tracing::warn;

use crate::{Error, Simulator};

pub fn handle_query(sim: &mut Simulator, query: Box<Query>) -> Result<(), Error> {
    if let SetExpr::Select(select) = *query.body {
        handle_select_query(sim, &select)?;
    } else {
        warn!(query_type = %query.body, "Unsupported Query");
    }

    Ok(())
}
