use super::{dataframe, StatusFlags};

pub enum QueryResponse {
    Ok(StatusFlags),
    ResultSet(StatusFlags, Box<dataframe::DataFrame>),
}
