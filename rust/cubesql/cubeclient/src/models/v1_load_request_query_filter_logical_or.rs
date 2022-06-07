#[derive(Clone, Debug, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct V1LoadRequestQueryFilterLogicalOr {
    #[serde(rename = "or", skip_serializing_if = "Option::is_none")]
    pub or: Option<Vec<serde_json::Value>>,
}

impl V1LoadRequestQueryFilterLogicalOr {
    #[must_use]
    pub fn new() -> Self {
        Self { or: None }
    }
}
