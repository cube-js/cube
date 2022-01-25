/// V1LoadRequestQueryFilterLogicalOr : An array with one or more filters or other logical operators.

#[derive(Clone, Debug, PartialEq, Default, Serialize, Deserialize)]
pub struct V1LoadRequestQueryFilterLogicalOr {
    #[serde(rename = "or", skip_serializing_if = "Option::is_none")]
    pub or: Option<Vec<serde_json::Value>>,
}

impl V1LoadRequestQueryFilterLogicalOr {
    /// An array with one or more filters or other logical operators.
    pub fn new() -> V1LoadRequestQueryFilterLogicalOr {
        V1LoadRequestQueryFilterLogicalOr { or: None }
    }
}
