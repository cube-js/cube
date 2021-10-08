#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct V1LoadRequestQueryTimeDimension {
    #[serde(rename = "dimension")]
    pub dimension: String,
    #[serde(rename = "granularity")]
    pub granularity: String,
    #[serde(rename = "dateRange", skip_serializing_if = "Option::is_none")]
    pub date_range: Option<serde_json::Value>,
}

impl V1LoadRequestQueryTimeDimension {
    pub fn new(dimension: String, granularity: String) -> V1LoadRequestQueryTimeDimension {
        V1LoadRequestQueryTimeDimension {
            dimension,
            granularity,
            date_range: None,
        }
    }
}
