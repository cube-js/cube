#[derive(Clone, Debug, PartialEq, Default, Serialize, Deserialize)]
pub struct V1LoadRequestQueryFilterBase {
    #[serde(rename = "member", skip_serializing_if = "Option::is_none")]
    pub member: Option<String>,
    #[serde(rename = "operator", skip_serializing_if = "Option::is_none")]
    pub operator: Option<String>,
    #[serde(rename = "values", skip_serializing_if = "Option::is_none")]
    pub values: Option<Vec<String>>,
}

impl V1LoadRequestQueryFilterBase {
    pub fn new() -> V1LoadRequestQueryFilterBase {
        V1LoadRequestQueryFilterBase {
            member: None,
            operator: None,
            values: None,
        }
    }
}
