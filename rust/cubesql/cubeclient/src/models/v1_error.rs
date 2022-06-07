#[derive(Clone, Debug, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct V1Error {
    #[serde(rename = "error")]
    pub error: String,
}

impl V1Error {
    #[must_use]
    pub fn new(error: String) -> Self {
        Self { error }
    }
}
