use std::sync::Arc;

use super::{dataframe, StatusFlags};

pub enum QueryResponse {
    Ok(StatusFlags),
    ResultSet(StatusFlags, Arc<dataframe::DataFrame>),
}
