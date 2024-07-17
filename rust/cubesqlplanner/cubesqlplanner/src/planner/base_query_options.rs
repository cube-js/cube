use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
pub struct BaseQueryOptions {
    measures: Option<Vec<String>>,
    dimensions: Option<Vec<String>>,
}
