/// `RegularRollingWindow` filter operation: trailing and leading
/// interval bounds of a rolling window relative to each time-series
/// point.
///
/// `scan_range` holds the span the base scan reads — the series' own bounds
/// with the frame already folded in — when that is known at plan time. A
/// literal span is one an engine can eliminate partitions by; without it the
/// bounds are read back off the series with a scalar sub-select and the frame
/// applied to them in SQL, which is opaque to pruning. An `unbounded` side
/// keeps the unshifted bound and is dropped when the filter renders.
#[derive(Clone, Debug)]
pub struct RegularRollingWindowOp {
    pub(crate) trailing: Option<String>,
    pub(crate) leading: Option<String>,
    pub(crate) scan_range: Option<(String, String)>,
}

impl RegularRollingWindowOp {
    pub fn new(
        trailing: Option<String>,
        leading: Option<String>,
        scan_range: Option<(String, String)>,
    ) -> Self {
        Self {
            trailing,
            leading,
            scan_range,
        }
    }
}

/// `RollingWindowOffset` filter operation: a rolling window over a single date
/// range `[from, to]` (no time series / granularity). The window is anchored by
/// `offset` ('start' → `from`, 'end' → `to`); the trailing side is the lower
/// bound, the leading side the upper bound. An `unbounded` side drops its bound,
/// a finite interval shifts it (trailing subtracts, leading adds).
#[derive(Clone, Debug)]
pub struct RollingWindowOffsetOp {
    pub(crate) from: Option<String>,
    pub(crate) to: Option<String>,
    pub(crate) trailing: Option<String>,
    pub(crate) leading: Option<String>,
    pub(crate) offset: String,
}

impl RollingWindowOffsetOp {
    pub fn new(
        from: Option<String>,
        to: Option<String>,
        trailing: Option<String>,
        leading: Option<String>,
        offset: String,
    ) -> Self {
        Self {
            from,
            to,
            trailing,
            leading,
            offset,
        }
    }
}
