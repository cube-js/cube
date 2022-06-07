#[derive(Clone, Debug, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct V1LoadRequestQueryFilterLogicalAnd {
    #[serde(rename = "and", skip_serializing_if = "Option::is_none")]
    pub and: Option<Vec<serde_json::Value>>,
}

impl V1LoadRequestQueryFilterLogicalAnd {
    #[must_use]
    pub fn new() -> Self {
        Self { and: None }
    }
}
