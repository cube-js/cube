use crate::planner::Granularity;

#[derive(Clone)]
pub struct ToDateRollingWindowOp {
    pub(crate) granularity: Granularity,
}

impl std::fmt::Debug for ToDateRollingWindowOp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ToDateRollingWindowOp")
            .field("granularity", &"<Granularity>")
            .finish()
    }
}

impl ToDateRollingWindowOp {
    pub fn new(granularity: Granularity) -> Self {
        Self { granularity }
    }
}
