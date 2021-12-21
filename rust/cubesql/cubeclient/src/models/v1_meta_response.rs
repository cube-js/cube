#[derive(Clone, Debug, PartialEq, Default, Serialize, Deserialize)]
pub struct V1MetaResponse {
    #[serde(rename = "cubes", skip_serializing_if = "Option::is_none")]
    pub cubes: Option<Vec<crate::models::V1CubeMeta>>,
}

impl V1MetaResponse {
    pub fn new() -> V1MetaResponse {
        V1MetaResponse { cubes: None }
    }
}
