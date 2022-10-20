#[derive(Clone, Debug, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct V1CubeMeta {
    #[serde(rename = "name")]
    pub name: String,
    #[serde(rename = "title", skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    #[serde(rename = "measures")]
    pub measures: Vec<super::V1CubeMetaMeasure>,
    #[serde(rename = "dimensions")]
    pub dimensions: Vec<super::V1CubeMetaDimension>,
    #[serde(rename = "segments")]
    pub segments: Vec<super::V1CubeMetaSegment>,
}

impl V1CubeMeta {
    #[must_use]
    pub fn new(
        name: String,
        measures: Vec<super::V1CubeMetaMeasure>,
        dimensions: Vec<super::V1CubeMetaDimension>,
        segments: Vec<super::V1CubeMetaSegment>,
    ) -> Self {
        Self {
            name,
            title: None,
            measures,
            dimensions,
            segments,
        }
    }
}
