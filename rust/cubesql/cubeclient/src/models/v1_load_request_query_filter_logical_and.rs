/// V1LoadRequestQueryFilterLogicalAnd : An array with one or more filters or other logical operators.

#[derive(Clone, Debug, PartialEq, Default, Serialize, Deserialize)]
pub struct V1LoadRequestQueryFilterLogicalAnd {
    #[serde(rename = "and", skip_serializing_if = "Option::is_none")]
    pub and: Option<Vec<serde_json::Value>>,
}

impl V1LoadRequestQueryFilterLogicalAnd {
    /// An array with one or more filters or other logical operators.
    pub fn new() -> V1LoadRequestQueryFilterLogicalAnd {
        V1LoadRequestQueryFilterLogicalAnd { and: None }
    }
}
