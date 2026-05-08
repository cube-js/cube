use crate::planner::symbols::CalendarDimensionTimeShift;
use crate::planner::MemberSymbol;
use cubenativeutils::CubeError;
use std::collections::HashMap;
use std::rc::Rc;

use super::{OpCtx, OpExec};

/// Applies a calendar-cube shift to a dimension: either substitutes a
/// preconfigured SQL expression for it, or moves it by an interval in the
/// inverse direction so calendar joins line up with the requested period.
#[derive(Clone)]
pub struct CalendarTimeShiftOp {
    shifts: Rc<HashMap<String, CalendarDimensionTimeShift>>,
}

impl CalendarTimeShiftOp {
    pub fn new(shifts: HashMap<String, CalendarDimensionTimeShift>) -> Self {
        Self {
            shifts: Rc::new(shifts),
        }
    }
}

impl OpExec for CalendarTimeShiftOp {
    fn exec(&self, ctx: &mut OpCtx<'_>) -> Result<String, CubeError> {
        let MemberSymbol::Dimension(ev) = ctx.sym.as_ref() else {
            return ctx.render_tail();
        };
        if ev.is_reference() {
            return ctx.render_tail();
        }
        let Some(shift) = self.shifts.get(&ev.full_name()) else {
            return ctx.render_tail();
        };
        if let Some(sql) = &shift.sql {
            sql.eval(
                &ctx.visitor,
                ctx.legacy_node_processor.clone(),
                ctx.query_tools.clone(),
                ctx.templates,
            )
        } else if let Some(interval) = &shift.interval {
            let interval_sql = interval.inverse().to_sql();
            let inner_visitor = ctx.visitor.with_arg_needs_paren_safe(false);
            let input = ctx.with_visitor(inner_visitor).render_tail()?;
            let shifted = ctx.templates.add_timestamp_interval(input, interval_sql)?;
            Ok(format!("({})", shifted))
        } else {
            ctx.render_tail()
        }
    }
}
