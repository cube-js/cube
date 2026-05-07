#[derive(Clone, Debug)]
pub struct LikeOp {
    pub(crate) negated: bool,
    pub(crate) start_wild: bool,
    pub(crate) end_wild: bool,
    pub(crate) values: Vec<String>,
    pub(crate) has_null: bool,
    pub(crate) member_type: Option<String>,
}

impl LikeOp {
    pub fn new(
        negated: bool,
        start_wild: bool,
        end_wild: bool,
        values: Vec<String>,
        has_null: bool,
        member_type: Option<String>,
    ) -> Self {
        Self {
            negated,
            start_wild,
            end_wild,
            values,
            has_null,
            member_type,
        }
    }
}
