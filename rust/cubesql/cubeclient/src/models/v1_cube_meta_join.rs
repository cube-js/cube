#[derive(Clone, Debug, PartialEq, Default, Serialize, Deserialize)]
pub struct V1CubeMetaJoin {
    #[serde(rename = "name")]
    pub name: String,
    #[serde(rename = "relationship")]
    pub relationship: String,
}

impl V1CubeMetaJoin {
    pub fn new(name: String, relationship: String) -> V1CubeMetaJoin {
        V1CubeMetaJoin { name, relationship }
    }
}
