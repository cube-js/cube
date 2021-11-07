#[derive(Clone, Debug, PartialEq, Default, Serialize, Deserialize)]
pub struct V1CubeMetaMeasure {
    #[serde(rename = "name")]
    pub name: String,
    #[serde(rename = "title", skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    #[serde(rename = "type")]
    pub _type: String,
    #[serde(rename = "aggType", skip_serializing_if = "Option::is_none")]
    pub agg_type: Option<String>,
}

impl V1CubeMetaMeasure {
    pub fn new(name: String, _type: String) -> V1CubeMetaMeasure {
        V1CubeMetaMeasure {
            name,
            title: None,
            _type,
            agg_type: None,
        }
    }
}
