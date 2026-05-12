#[derive(Clone, Debug)]
pub enum ComparisonKind {
    Gt,
    Gte,
    Lt,
    Lte,
}

/// `Comparison` filter operation: compares the member against a
/// single value with `>`, `>=`, `<`, or `<=`.
#[derive(Clone, Debug)]
pub struct ComparisonOp {
    pub(crate) kind: ComparisonKind,
    pub(crate) value: String,
    pub(crate) member_type: Option<String>,
}

impl ComparisonOp {
    pub fn new(kind: ComparisonKind, value: String, member_type: Option<String>) -> Self {
        Self {
            kind,
            value,
            member_type,
        }
    }
}
