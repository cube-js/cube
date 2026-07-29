use crate::cube_bridge::base_query_options::FilterValue;

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
    pub(crate) value: FilterValue,
    pub(crate) member_type: Option<String>,
}

impl ComparisonOp {
    pub fn new(kind: ComparisonKind, value: FilterValue, member_type: Option<String>) -> Self {
        Self {
            kind,
            value,
            member_type,
        }
    }
}
