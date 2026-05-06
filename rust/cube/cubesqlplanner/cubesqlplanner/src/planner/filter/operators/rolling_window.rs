#[derive(Clone, Debug)]
pub struct RegularRollingWindowOp {
    pub(crate) trailing: Option<String>,
    pub(crate) leading: Option<String>,
}

impl RegularRollingWindowOp {
    pub fn new(trailing: Option<String>, leading: Option<String>) -> Self {
        Self { trailing, leading }
    }
}
