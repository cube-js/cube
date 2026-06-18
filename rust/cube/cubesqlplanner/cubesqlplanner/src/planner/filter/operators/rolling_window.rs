/// `RegularRollingWindow` filter operation: trailing and leading
/// interval bounds of a rolling window relative to each time-series
/// point.
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
