#[derive(Clone, Debug, PartialEq, Default, Serialize, Deserialize)]
pub struct V1LoadRequestQueryFilterLogicalOr {
    #[serde(rename = "or", skip_serializing_if = "Option::is_none")]
    pub or: Option<Vec<serde_json::Value>>,
}

impl V1LoadRequestQueryFilterLogicalOr {
    pub fn new() -> V1LoadRequestQueryFilterLogicalOr {
        V1LoadRequestQueryFilterLogicalOr { or: None }
    }
}
