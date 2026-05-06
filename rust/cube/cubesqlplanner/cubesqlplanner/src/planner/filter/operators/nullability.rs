#[derive(Clone, Debug)]
pub struct NullabilityOp {
    pub(crate) negated: bool,
}

impl NullabilityOp {
    pub fn new(negated: bool) -> Self {
        Self { negated }
    }
}
