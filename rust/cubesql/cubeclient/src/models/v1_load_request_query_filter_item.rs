#[derive(Clone, Debug, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct V1LoadRequestQueryFilterItem {
    #[serde(rename = "member", skip_serializing_if = "Option::is_none")]
    pub member: Option<String>,
    #[serde(rename = "operator", skip_serializing_if = "Option::is_none")]
    pub operator: Option<String>,
    #[serde(rename = "values", skip_serializing_if = "Option::is_none")]
    pub values: Option<Vec<String>>,
    #[serde(rename = "or", skip_serializing_if = "Option::is_none")]
    pub or: Option<Vec<serde_json::Value>>,
    #[serde(rename = "and", skip_serializing_if = "Option::is_none")]
    pub and: Option<Vec<serde_json::Value>>,
}

impl V1LoadRequestQueryFilterItem {
    #[must_use]
    pub fn new() -> Self {
        Self {
            member: None,
            operator: None,
            values: None,
            or: None,
            and: None,
        }
    }
}
