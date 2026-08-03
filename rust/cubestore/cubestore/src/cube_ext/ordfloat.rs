use serde_derive::{Deserialize, Serialize};
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
        return total_cmp_64(self.0, other.0);
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

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
#[repr(transparent)]
pub struct OrdF32(pub f32);

impl PartialEq for OrdF32 {
    fn eq(&self, other: &Self) -> bool {
        return self.cmp(other) == Ordering::Equal;
    }
}
impl Eq for OrdF32 {}

impl PartialOrd for OrdF32 {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        return Some(self.cmp(other));
    }
}

impl Ord for OrdF32 {
    fn cmp(&self, other: &Self) -> Ordering {
        return total_cmp_32(self.0, other.0);
    }
}

impl fmt::Display for OrdF32 {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), fmt::Error> {
        self.0.fmt(f)
    }
}

impl From<f32> for OrdF32 {
    fn from(v: f32) -> Self {
        return Self(v);
    }
}

impl Hash for OrdF32 {
    fn hash<H: Hasher>(&self, state: &mut H) {
        format!("{}", self.0).hash(state);
    }
}

// implements comparison using IEEE 754 total ordering for f32
// Original implementation from https://doc.rust-lang.org/std/primitive.f64.html#method.total_cmp
// TODO to change to use std when it becomes stable
pub fn total_cmp_32(l: f32, r: f32) -> std::cmp::Ordering {
    let mut left = l.to_bits() as i32;
    let mut right = r.to_bits() as i32;

    left ^= (((left >> 31) as u32) >> 1) as i32;
    right ^= (((right >> 31) as u32) >> 1) as i32;

    left.cmp(&right)
}

// implements comparison using IEEE 754 total ordering for f64
// Original implementation from https://doc.rust-lang.org/std/primitive.f64.html#method.total_cmp
// TODO to change to use std when it becomes stable
pub fn total_cmp_64(l: f64, r: f64) -> std::cmp::Ordering {
    let mut left = l.to_bits() as i64;
    let mut right = r.to_bits() as i64;

    left ^= (((left >> 63) as u64) >> 1) as i64;
    right ^= (((right >> 63) as u64) >> 1) as i64;

    left.cmp(&right)
}
