#[derive(Clone, Debug, PartialEq, Default, Serialize, Deserialize)]
pub struct V1CubeMeta {
    #[serde(rename = "name")]
    pub name: String,
    #[serde(rename = "title", skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    #[serde(rename = "measures")]
    pub measures: Vec<crate::models::V1CubeMetaMeasure>,
    #[serde(rename = "dimensions")]
    pub dimensions: Vec<crate::models::V1CubeMetaDimension>,
    #[serde(rename = "segments")]
    pub segments: Vec<crate::models::V1CubeMetaSegment>,
}

impl V1CubeMeta {
    pub fn new(
        name: String,
        measures: Vec<crate::models::V1CubeMetaMeasure>,
        dimensions: Vec<crate::models::V1CubeMetaDimension>,
        segments: Vec<crate::models::V1CubeMetaSegment>,
    ) -> V1CubeMeta {
        V1CubeMeta {
            name,
            title: None,
            measures,
            dimensions,
            segments,
        }
    }
}
