use crate::cube_bridge::base_query_options::FilterValue;

/// `InList` filter operation: tests whether the member belongs to,
/// or — when `negated` — does not belong to, a list of values.
#[derive(Clone, Debug)]
pub struct InListOp {
    pub(crate) negated: bool,
    pub(crate) values: Vec<FilterValue>,
    pub(crate) member_type: Option<String>,
}

impl InListOp {
    pub fn new(negated: bool, values: Vec<FilterValue>, member_type: Option<String>) -> Self {
        Self {
            negated,
            values,
            member_type,
        }
    }
}
