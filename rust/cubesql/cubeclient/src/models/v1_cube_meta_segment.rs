#[derive(Clone, Debug, PartialEq, Default, Serialize, Deserialize)]
pub struct V1CubeMetaSegment {
    #[serde(rename = "name")]
    pub name: String,
    #[serde(rename = "title")]
    pub title: String,
    #[serde(rename = "shortTitle")]
    pub short_title: String,
}

impl V1CubeMetaSegment {
    pub fn new(name: String, title: String, short_title: String) -> V1CubeMetaSegment {
        V1CubeMetaSegment {
            name,
            title,
            short_title,
        }
    }
}
