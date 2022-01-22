#[derive(Clone, Debug, PartialEq, Default, Serialize, Deserialize)]
pub struct V1Error {
    #[serde(rename = "error")]
    pub error: String,
}

impl V1Error {
    pub fn new(error: String) -> V1Error {
        V1Error { error }
    }
}
