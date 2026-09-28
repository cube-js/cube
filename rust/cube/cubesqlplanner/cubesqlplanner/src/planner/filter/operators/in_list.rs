/// `InList` filter operation: tests whether the member belongs to,
/// or — when `negated` — does not belong to, a list of values.
#[derive(Clone, Debug)]
pub struct InListOp {
    pub(crate) negated: bool,
    pub(crate) values: Vec<Option<String>>,
    pub(crate) member_type: Option<String>,
}

impl InListOp {
    pub fn new(negated: bool, values: Vec<Option<String>>, member_type: Option<String>) -> Self {
        Self {
            negated,
            values,
            member_type,
        }
    }
}
