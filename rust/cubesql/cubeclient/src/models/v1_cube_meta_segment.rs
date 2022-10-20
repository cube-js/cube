#[derive(Clone, Debug, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct V1CubeMetaSegment {
    #[serde(rename = "name")]
    pub name: String,
    #[serde(rename = "title")]
    pub title: String,
    #[serde(rename = "shortTitle")]
    pub short_title: String,
}

impl V1CubeMetaSegment {
    #[must_use]
    pub fn new(name: String, title: String, short_title: String) -> Self {
        Self {
            name,
            title,
            short_title,
        }
    }
}
