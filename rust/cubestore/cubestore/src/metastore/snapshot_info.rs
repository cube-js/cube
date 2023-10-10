use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct SnapshotInfo {
    pub id: u128,
    pub current: bool,
}
