#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct V1LoadContinueWait {
    pub error: String,
}

impl V1LoadContinueWait {
    #[must_use]
    pub fn new(error: String) -> Self {
        Self { error }
    }
}
