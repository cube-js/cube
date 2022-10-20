#[derive(Clone, Debug, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct V1CubeMetaDimension {
    #[serde(rename = "name")]
    pub name: String,
    #[serde(rename = "type")]
    pub ty: String,
}

impl V1CubeMetaDimension {
    #[must_use]
    pub fn new(name: String, ty: String) -> Self {
        Self { name, ty }
    }
}
