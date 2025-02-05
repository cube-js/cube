use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub enum JoinHintItem {
    Single(String),
    Vector(Vec<String>),
}
