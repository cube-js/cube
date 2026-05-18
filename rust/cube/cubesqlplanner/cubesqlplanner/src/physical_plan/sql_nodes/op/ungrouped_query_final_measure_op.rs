use crate::planner::symbols::{AggregationType, MeasureKind};
use crate::planner::MemberSymbol;
use cubenativeutils::CubeError;

use super::{OpCtx, OpExec};

/// Renders a measure for an ungrouped query at the row level: count-likes
/// turn into `CASE WHEN <expr> IS NOT NULL THEN 1 END` so a downstream
/// aggregator can sum them, other measures pass through unchanged.
#[derive(Clone, Debug)]
pub struct UngroupedQueryFinalMeasureOp;

impl OpExec for UngroupedQueryFinalMeasureOp {
    fn exec(&self, ctx: &mut OpCtx<'_>) -> Result<String, CubeError> {
        let MemberSymbol::Measure(ev) = ctx.sym.as_ref() else {
            return Err(CubeError::internal(
                "UngroupedQueryFinalMeasure op called for non-measure symbol".to_string(),
            ));
        };
        let is_count_like = match ev.kind() {
            MeasureKind::Count(_) => true,
            MeasureKind::Aggregated(a) => matches!(
                a.agg_type(),
                AggregationType::CountDistinct | AggregationType::CountDistinctApprox
            ),
            _ => false,
        };
        // Count-likes wrap the child in `CASE WHEN … IS NOT NULL THEN 1 END`
        // (safe), other kinds pass through and must propagate the flag.
        let child_visitor = if is_count_like {
            ctx.visitor.with_arg_needs_paren_safe(false)
        } else {
            ctx.visitor.clone()
        };
        let input = ctx.with_visitor(child_visitor).render_tail()?;
        Ok(if input == "*" {
            "1".to_string()
        } else if is_count_like {
            // TODO: route through templates.
            format!("CASE WHEN ({}) IS NOT NULL THEN 1 END", input)
        } else {
            input
        })
    }
}
