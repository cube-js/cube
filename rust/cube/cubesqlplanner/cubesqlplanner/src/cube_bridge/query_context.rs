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

/// The query a measure `case.when[].context` condition is evaluated against,
/// in the shape the JS `QueryContext` class reads: full `cube.member` paths,
/// filters as the leaves of the filter tree, a time dimension's date range on
/// the time dimension itself.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Default)]
#[serde(rename_all = "camelCase")]
pub struct QueryContextData {
    pub dimensions: Vec<String>,
    pub time_dimensions: Vec<QueryContextTimeDimension>,
    pub measures: Vec<String>,
    pub filters: Vec<QueryContextFilter>,
}
