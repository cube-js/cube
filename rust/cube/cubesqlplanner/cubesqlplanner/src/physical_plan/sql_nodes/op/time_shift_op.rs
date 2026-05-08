use crate::planner::planners::multi_stage::TimeShiftState;
use crate::planner::MemberSymbol;
use cubenativeutils::CubeError;

use super::{OpCtx, OpExec};

/// Shifts a time-typed dimension by a configured interval (e.g.
/// "previous month") so multi-stage queries can compare aligned periods.
/// Dimensions without a configured shift fall through unchanged.
#[derive(Clone, Debug)]
pub struct TimeShiftOp {
    shifts: TimeShiftState,
}

impl TimeShiftOp {
    pub fn new(shifts: TimeShiftState) -> Self {
        Self { shifts }
    }
}

impl OpExec for TimeShiftOp {
    fn exec(&self, ctx: &mut OpCtx<'_>) -> Result<String, CubeError> {
        let MemberSymbol::Dimension(ev) = ctx.sym.as_ref() else {
            return ctx.render_tail();
        };
        if ev.is_reference() || !ev.is_time() {
            return ctx.render_tail();
        }
        let Some(shift) = self.shifts.dimensions_shifts.get(&ev.full_name()) else {
            return ctx.render_tail();
        };
        let Some(interval) = &shift.interval else {
            return Err(CubeError::internal(format!(
                "TimeShift op: dimension '{}' has a shift entry but no interval",
                ev.full_name()
            )));
        };
        let interval_sql = interval.to_sql();
        let inner_visitor = ctx.visitor.with_arg_needs_paren_safe(false);
        let input = ctx.with_visitor(inner_visitor).render_tail()?;
        let shifted = ctx.templates.add_timestamp_interval(input, interval_sql)?;
        Ok(format!("({})", shifted))
    }
}
