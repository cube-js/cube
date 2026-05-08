use crate::planner::MemberSymbol;
use cubenativeutils::CubeError;
use std::collections::HashSet;
use std::rc::Rc;

use super::{OpCtx, OpExec};

/// Lifts time values into the user's timezone and applies any requested
/// granularity (`week`/`month`/calendar-cube SQL/...). Covers both
/// `TimeDimension` symbols and raw time-typed `Dimension` symbols (the
/// latter only when raw timezone conversion is enabled).
#[derive(Clone)]
pub struct TimeDimensionOp {
    dimensions_with_ignored_timezone: Rc<HashSet<String>>,
}

impl TimeDimensionOp {
    pub fn new(dimensions_with_ignored_timezone: HashSet<String>) -> Self {
        Self {
            dimensions_with_ignored_timezone: Rc::new(dimensions_with_ignored_timezone),
        }
    }
}

impl OpExec for TimeDimensionOp {
    fn exec(&self, ctx: &mut OpCtx<'_>) -> Result<String, CubeError> {
        match ctx.sym.as_ref() {
            MemberSymbol::TimeDimension(ev) => {
                let Some(granularity_obj) = ev.granularity_obj() else {
                    return ctx.render_tail();
                };
                // Short-circuit to calendar SQL — the rest of the pipeline is
                // not used. Outer visitor stays as-is: the calendar SQL is the
                // expression itself, no further wrapping here.
                if let Some(calendar_sql) = granularity_obj.calendar_sql() {
                    return calendar_sql.eval(
                        &ctx.visitor,
                        ctx.legacy_node_processor.clone(),
                        ctx.query_tools.clone(),
                        ctx.templates,
                    );
                }
                let granularity_obj = granularity_obj.clone();
                let skip_convert_tz = self
                    .dimensions_with_ignored_timezone
                    .contains(&ev.full_name());
                // Wraps in `convert_tz(…)` and a granularity function — safe,
                // reset paren-safe for the child render.
                let inner_visitor = ctx.visitor.with_arg_needs_paren_safe(false);
                let input_sql = ctx.with_visitor(inner_visitor).render_tail()?;
                let converted_tz = if skip_convert_tz {
                    input_sql
                } else {
                    ctx.templates.convert_tz(input_sql)?
                };
                granularity_obj.apply_to_input_sql(ctx.templates, converted_tz)
            }
            MemberSymbol::Dimension(ev) => {
                let wraps_convert_tz = !ctx.visitor.ignore_tz_convert()
                    && ctx.query_tools.convert_tz_for_raw_time_dimension()
                    && ev.dimension_type() == "time";
                if !wraps_convert_tz {
                    return ctx.render_tail();
                }
                let inner_visitor = ctx.visitor.with_arg_needs_paren_safe(false);
                let input_sql = ctx.with_visitor(inner_visitor).render_tail()?;
                ctx.templates.convert_tz(input_sql)
            }
            _ => ctx.render_tail(),
        }
    }
}
