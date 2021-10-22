#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct V1LoadConinueWait {
    pub error: String,
}

impl V1LoadConinueWait {
    pub fn new(error: String) -> V1LoadConinueWait {
        V1LoadConinueWait { error }
    }
}
