#[derive(Debug)]
pub enum BindValue {
    String(String),
    Int64(i64),
    #[allow(unused)]
    UInt64(u64),
    Float64(f64),
    Bool(bool),
    Null,
}
