#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct V1LoadContinueWait {
    pub error: String,
}

impl V1LoadContinueWait {
    pub fn new(error: String) -> V1LoadContinueWait {
        V1LoadContinueWait { error }
    }
}
