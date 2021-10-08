#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct V1LoadResponse400 {
    #[serde(rename = "error")]
    pub error: String,
}

impl V1LoadResponse400 {
    pub fn new(error: String) -> V1LoadResponse400 {
        V1LoadResponse400 { error }
    }
}
