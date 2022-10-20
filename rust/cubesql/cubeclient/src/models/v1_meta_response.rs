#[derive(Clone, Debug, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct V1MetaResponse {
    #[serde(rename = "cubes", skip_serializing_if = "Option::is_none")]
    pub cubes: Option<Vec<super::V1CubeMeta>>,
}

impl V1MetaResponse {
    #[must_use]
    pub fn new() -> Self {
        Self { cubes: None }
    }
}
