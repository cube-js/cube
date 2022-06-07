#[derive(Clone, Debug, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct V1LoadRequest {
    #[serde(rename = "queryType", skip_serializing_if = "Option::is_none")]
    pub query_type: Option<String>,
    #[serde(rename = "query", skip_serializing_if = "Option::is_none")]
    pub query: Option<super::V1LoadRequestQuery>,
}

impl V1LoadRequest {
    #[must_use]
    pub fn new() -> Self {
        Self {
            query_type: None,
            query: None,
        }
    }
}
