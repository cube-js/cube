#[derive(Clone, Debug, PartialEq, Default, Serialize, Deserialize)]
pub struct V1LoadRequestQueryTimeDimension {
    #[serde(rename = "dimension")]
    pub dimension: String,
    #[serde(rename = "granularity", skip_serializing_if = "Option::is_none")]
    pub granularity: Option<String>,
    #[serde(rename = "dateRange", skip_serializing_if = "Option::is_none")]
    pub date_range: Option<serde_json::Value>,
}

impl V1LoadRequestQueryTimeDimension {
    pub fn new(dimension: String) -> V1LoadRequestQueryTimeDimension {
        V1LoadRequestQueryTimeDimension {
            dimension,
            granularity: None,
            date_range: None,
        }
    }
}
