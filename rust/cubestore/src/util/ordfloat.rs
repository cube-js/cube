use serde::{Deserialize, Serialize};
use smallvec::alloc::fmt::Formatter;
use std::cmp::Ordering;
use std::fmt;
use std::hash::{Hash, Hasher};

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
#[repr(transparent)]
pub struct OrdF64(pub f64);

impl PartialEq for OrdF64 {
    fn eq(&self, other: &Self) -> bool {
        return self.cmp(other) == Ordering::Equal;
    }
}
impl Eq for OrdF64 {}

impl PartialOrd for OrdF64 {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        return Some(self.cmp(other));
    }
}

impl Ord for OrdF64 {
    fn cmp(&self, other: &Self) -> Ordering {
        return self.0.total_cmp(&other.0);
    }
}

impl fmt::Display for OrdF64 {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), fmt::Error> {
        self.0.fmt(f)
    }
}

impl From<f64> for OrdF64 {
    fn from(v: f64) -> Self {
        return Self(v);
    }
}

impl Hash for OrdF64 {
    fn hash<H: Hasher>(&self, state: &mut H) {
        format!("{}", self.0).hash(state);
    }
}
