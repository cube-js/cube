use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct QueryContextTimeDimension {
    pub dimension: String,
    pub granularity: Option<String>,
    pub date_range: Option<Vec<String>>,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct QueryContextFilter {
    pub member: String,
    pub operator: String,
    pub values: Option<Vec<Option<String>>>,
}

/// The query a measure `case.when[].context` condition is evaluated against, in the shape
/// the JS `QueryContext` class reads: full `cube.member` paths, the filter leaves that
/// constrain the whole query (`or` groups are left out), a date range on its time dimension.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Default)]
#[serde(rename_all = "camelCase")]
pub struct QueryContextData {
    pub dimensions: Vec<String>,
    pub time_dimensions: Vec<QueryContextTimeDimension>,
    pub measures: Vec<String>,
    pub filters: Vec<QueryContextFilter>,
}
