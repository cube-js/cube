use std::fmt::{Display, Formatter};

pub type Result<T> = std::result::Result<T, HllError>;
#[derive(Debug)]
pub struct HllError {
    pub message: String,
}

impl Display for HllError {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl HllError {
    pub fn new<Str: ToString>(message: Str) -> HllError {
        return HllError {
            message: message.to_string(),
        };
    }
}

impl From<std::io::Error> for HllError {
    fn from(err: std::io::Error) -> Self {
        return HllError::new(err);
    }
}

impl From<serde_json::Error> for HllError {
    fn from(err: serde_json::Error) -> Self {
        return HllError::new(err);
    }
}
