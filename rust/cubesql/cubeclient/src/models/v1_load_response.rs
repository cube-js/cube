#[derive(Clone, Debug, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct V1LoadResponse {
    #[serde(rename = "pivotQuery", skip_serializing_if = "Option::is_none")]
    pub pivot_query: Option<serde_json::Value>,
    #[serde(rename = "slowQuery", skip_serializing_if = "Option::is_none")]
    pub slow_query: Option<bool>,
    #[serde(rename = "queryType", skip_serializing_if = "Option::is_none")]
    pub query_type: Option<String>,
    #[serde(rename = "results")]
    pub results: Vec<super::V1LoadResult>,
}

impl V1LoadResponse {
    #[must_use]
    pub fn new(results: Vec<super::V1LoadResult>) -> Self {
        Self {
            pivot_query: None,
            slow_query: None,
            query_type: None,
            results,
        }
    }
}
