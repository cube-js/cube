use crate::planner::Granularity;
use crate::planner::SeriesSpan;

/// `ToDateRollingWindow` filter operation: bounds a "since the start
/// of <granularity>" window — e.g. month-to-date, year-to-date.
///
/// `window_range` holds the span the window reads — the start of the period the
/// series opens in, to the series' own end — when that is known at plan time. A
/// literal span is one an engine can eliminate partitions by; without it both
/// bounds are read back off the series with a scalar sub-select, which is opaque
/// to pruning.
#[derive(Clone)]
pub struct ToDateRollingWindowOp {
    pub(crate) granularity: Granularity,
    pub(crate) window_range: Option<SeriesSpan>,
}

impl std::fmt::Debug for ToDateRollingWindowOp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ToDateRollingWindowOp")
            .field("granularity", &"<Granularity>")
            .field("window_range", &self.window_range)
            .finish()
    }
}

impl ToDateRollingWindowOp {
    pub fn new(granularity: Granularity, window_range: Option<SeriesSpan>) -> Self {
        Self {
            granularity,
            window_range,
        }
    }
}
