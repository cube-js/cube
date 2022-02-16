#[derive(Clone, Debug, PartialEq, Default, Serialize, Deserialize)]
pub struct V1LoadRequest {
    #[serde(rename = "queryType", skip_serializing_if = "Option::is_none")]
    pub query_type: Option<String>,
    #[serde(rename = "query", skip_serializing_if = "Option::is_none")]
    pub query: Option<crate::models::V1LoadRequestQuery>,
}

impl V1LoadRequest {
    pub fn new() -> V1LoadRequest {
        V1LoadRequest {
            query_type: None,
            query: None,
        }
    }
}
