use crate::planner::time_dimension::QueryDateTimeHelper;
use cubenativeutils::CubeError;

#[derive(Clone, Debug)]
pub enum DateRangeKind {
    InRange,
    NotInRange,
}

/// `DateRange` filter operation: tests the member's date against an
/// inclusive `[from, to]` range; `kind` chooses in-range vs not.
#[derive(Clone, Debug)]
pub struct DateRangeOp {
    pub(crate) kind: DateRangeKind,
    pub(crate) from: String,
    pub(crate) to: String,
}

impl DateRangeOp {
    pub fn new(kind: DateRangeKind, from: String, to: String) -> Self {
        Self { kind, from, to }
    }

    /// Formats `from` / `to` with the given fractional-seconds
    /// precision, ready to be embedded into rendered SQL.
    pub fn formatted_date_range(&self, precision: u32) -> Result<(String, String), CubeError> {
        let from = QueryDateTimeHelper::format_from_date(&self.from, precision)?;
        let to = QueryDateTimeHelper::format_to_date(&self.to, precision)?;
        Ok((from, to))
    }
}
