#[derive(Clone, Debug, PartialEq, Default, Serialize, Deserialize)]
pub struct V1CubeMetaDimension {
    #[serde(rename = "name")]
    pub name: String,
    #[serde(rename = "type")]
    pub _type: String,
}

impl V1CubeMetaDimension {
    pub fn new(name: String, _type: String) -> V1CubeMetaDimension {
        V1CubeMetaDimension { name, _type }
    }
}
