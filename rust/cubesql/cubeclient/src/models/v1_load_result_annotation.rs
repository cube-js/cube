#[derive(Clone, Debug, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct V1LoadResultAnnotation {
    #[serde(rename = "measures")]
    pub measures: serde_json::Value,
    #[serde(rename = "dimensions")]
    pub dimensions: serde_json::Value,
    #[serde(rename = "segments")]
    pub segments: serde_json::Value,
    #[serde(rename = "timeDimensions")]
    pub time_dimensions: serde_json::Value,
}

impl V1LoadResultAnnotation {
    #[must_use]
    pub fn new(
        measures: serde_json::Value,
        dimensions: serde_json::Value,
        segments: serde_json::Value,
        time_dimensions: serde_json::Value,
    ) -> Self {
        Self {
            measures,
            dimensions,
            segments,
            time_dimensions,
        }
    }
}
