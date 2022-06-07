#[derive(Clone, Debug, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct V1LoadResult {
    #[serde(rename = "dataSource", skip_serializing_if = "Option::is_none")]
    pub data_source: Option<String>,
    #[serde(rename = "annotation")]
    pub annotation: Box<super::V1LoadResultAnnotation>,
    #[serde(rename = "data")]
    pub data: Vec<serde_json::Value>,
    #[serde(rename = "refreshKeyValues", skip_serializing_if = "Option::is_none")]
    pub refresh_key_values: Option<Vec<serde_json::Value>>,
}

impl V1LoadResult {
    #[must_use]
    pub fn new(annotation: super::V1LoadResultAnnotation, data: Vec<serde_json::Value>) -> Self {
        Self {
            data_source: None,
            annotation: Box::new(annotation),
            data,
            refresh_key_values: None,
        }
    }
}
