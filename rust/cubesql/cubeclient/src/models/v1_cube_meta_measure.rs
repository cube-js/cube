#[derive(Clone, Debug, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct V1CubeMetaMeasure {
    #[serde(rename = "name")]
    pub name: String,
    #[serde(rename = "title", skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    #[serde(rename = "type")]
    pub ty: String,
    #[serde(rename = "aggType", skip_serializing_if = "Option::is_none")]
    pub agg_type: Option<String>,
}

impl V1CubeMetaMeasure {
    #[must_use]
    pub fn new(name: String, ty: String) -> Self {
        Self {
            name,
            title: None,
            ty,
            agg_type: None,
        }
    }
}
