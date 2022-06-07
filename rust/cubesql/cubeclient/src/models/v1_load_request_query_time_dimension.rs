#[derive(Clone, Debug, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct V1LoadRequestQueryTimeDimension {
    #[serde(rename = "dimension")]
    pub dimension: String,
    #[serde(rename = "granularity", skip_serializing_if = "Option::is_none")]
    pub granularity: Option<String>,
    #[serde(rename = "dateRange", skip_serializing_if = "Option::is_none")]
    pub date_range: Option<serde_json::Value>,
}

impl V1LoadRequestQueryTimeDimension {
    #[must_use]
    pub fn new(dimension: String) -> Self {
        Self {
            dimension,
            granularity: None,
            date_range: None,
        }
    }
}
