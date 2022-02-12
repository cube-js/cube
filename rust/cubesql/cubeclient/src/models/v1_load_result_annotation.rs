#[derive(Clone, Debug, PartialEq, Default, Serialize, Deserialize)]
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
    pub fn new(
        measures: serde_json::Value,
        dimensions: serde_json::Value,
        segments: serde_json::Value,
        time_dimensions: serde_json::Value,
    ) -> V1LoadResultAnnotation {
        V1LoadResultAnnotation {
            measures,
            dimensions,
            segments,
            time_dimensions,
        }
    }
}
