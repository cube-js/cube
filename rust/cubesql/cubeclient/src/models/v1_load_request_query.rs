use serde_json::Value;

#[derive(Clone, Debug, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct V1LoadRequestQuery {
    #[serde(rename = "measures", skip_serializing_if = "Option::is_none")]
    pub measures: Option<Vec<String>>,
    #[serde(rename = "dimensions", skip_serializing_if = "Option::is_none")]
    pub dimensions: Option<Vec<String>>,
    #[serde(rename = "segments", skip_serializing_if = "Option::is_none")]
    pub segments: Option<Vec<String>>,
    #[serde(rename = "timeDimensions", skip_serializing_if = "Option::is_none")]
    pub time_dimensions: Option<Vec<super::V1LoadRequestQueryTimeDimension>>,
    #[serde(rename = "order", skip_serializing_if = "Option::is_none")]
    pub order: Option<Value>,
    #[serde(rename = "limit", skip_serializing_if = "Option::is_none")]
    pub limit: Option<i32>,
    #[serde(rename = "offset", skip_serializing_if = "Option::is_none")]
    pub offset: Option<i32>,
    #[serde(rename = "filters", skip_serializing_if = "Option::is_none")]
    pub filters: Option<Vec<super::V1LoadRequestQueryFilterItem>>,
}

impl V1LoadRequestQuery {
    #[must_use]
    pub fn new() -> Self {
        Self {
            measures: None,
            dimensions: None,
            segments: None,
            time_dimensions: None,
            order: None,
            limit: None,
            offset: None,
            filters: None,
        }
    }
}
