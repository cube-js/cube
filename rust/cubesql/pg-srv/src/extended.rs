//! Implementation for Extended Query

#[cfg(feature = "with-chrono")]
use crate::{DateValue, TimestampValue};

#[derive(Debug, PartialEq)]
pub enum BindValue {
    String(String),
    Int64(i64),
    Float64(f64),
    Bool(bool),
    #[cfg(feature = "with-chrono")]
    Timestamp(TimestampValue),
    #[cfg(feature = "with-chrono")]
    Date(DateValue),
    Null,
}
