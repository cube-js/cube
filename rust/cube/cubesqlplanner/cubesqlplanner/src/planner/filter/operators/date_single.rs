#[derive(Clone, Debug)]
pub enum DateSingleKind {
    Before,
    BeforeOrOn,
    After,
    AfterOrOn,
}

#[derive(Clone, Debug)]
pub struct DateSingleOp {
    pub(crate) kind: DateSingleKind,
    pub(crate) value: String,
}

impl DateSingleOp {
    pub fn new(kind: DateSingleKind, value: String) -> Self {
        Self { kind, value }
    }
}
