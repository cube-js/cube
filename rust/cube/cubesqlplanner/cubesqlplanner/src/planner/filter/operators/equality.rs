/// `Equality` filter operation: tests the member for equality, or
/// inequality when `negated`, against a single value.
#[derive(Clone, Debug)]
pub struct EqualityOp {
    pub(crate) negated: bool,
    pub(crate) value: String,
    pub(crate) member_type: Option<String>,
}

impl EqualityOp {
    pub fn new(negated: bool, value: String, member_type: Option<String>) -> Self {
        Self {
            negated,
            value,
            member_type,
        }
    }
}
