#[derive(Clone, Debug, PartialEq, Default, Serialize, Deserialize)]
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
    pub fn new() -> V1LoadRequestQueryFilterItem {
        V1LoadRequestQueryFilterItem {
            member: None,
            operator: None,
            values: None,
            or: None,
            and: None,
        }
    }
}
