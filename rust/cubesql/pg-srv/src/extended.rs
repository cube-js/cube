//! Implementation for Extended Query

#[derive(Debug, PartialEq)]
pub enum BindValue {
    String(String),
    Int64(i64),
    Float64(f64),
    Bool(bool),
    Null,
}
