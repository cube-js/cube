use crate::planner::symbols::{AggregationType, MeasureKind};
use crate::planner::MemberSymbol;
use cubenativeutils::CubeError;

use super::{Op, OpCtx, OpExec};

/// Aggregates a cumulative measure over its rolling window: SUM-able kinds
/// (`count`, `sum`, `running_total`, `min`, `max`, HLL approx) are wrapped
/// directly over `input_pipeline`; non-cumulative or unsupported kinds fall
/// back to `default_pipeline` for the regular aggregation path. Discards
/// the tail — each branch is a self-contained pipeline.
#[derive(Clone, Debug)]
pub struct RollingWindowOp {
    input_pipeline: Vec<Op>,
    default_pipeline: Vec<Op>,
}

impl RollingWindowOp {
    pub fn new(input_pipeline: Vec<Op>, default_pipeline: Vec<Op>) -> Self {
        Self {
            input_pipeline,
            default_pipeline,
        }
    }

    pub(super) fn nested_pipelines(&self) -> [&[Op]; 2] {
        [&self.input_pipeline, &self.default_pipeline]
    }
}

impl OpExec for RollingWindowOp {
    fn is_terminal(&self) -> bool {
        true
    }

    fn exec(&self, ctx: &mut OpCtx<'_>) -> Result<String, CubeError> {
        let MemberSymbol::Measure(m) = ctx.sym.as_ref() else {
            return Err(CubeError::internal(
                "RollingWindow op called for non-measure symbol".to_string(),
            ));
        };
        if !m.is_cumulative() {
            return ctx.render_pipeline(&self.default_pipeline);
        }
        let kind = m.kind().clone();
        let render_input = |c: &OpCtx<'_>| -> Result<String, CubeError> {
            let inner_visitor = c.visitor.with_arg_needs_paren_safe(false);
            c.with_visitor(inner_visitor)
                .render_pipeline(&self.input_pipeline)
        };
        match kind {
            MeasureKind::Count(_) => Ok(format!("sum({})", render_input(ctx)?)),
            MeasureKind::Aggregated(a) => match a.agg_type() {
                AggregationType::CountDistinctApprox => {
                    ctx.templates.hll_cardinality_merge(render_input(ctx)?)
                }
                AggregationType::Sum | AggregationType::RunningTotal => {
                    Ok(format!("sum({})", render_input(ctx)?))
                }
                AggregationType::Min | AggregationType::Max => {
                    Ok(format!("{}({})", a.agg_type().as_str(), render_input(ctx)?))
                }
                AggregationType::Avg
                | AggregationType::CountDistinct
                | AggregationType::NumberAgg => ctx.render_pipeline(&self.default_pipeline),
            },
            _ => ctx.render_pipeline(&self.default_pipeline),
        }
    }
}
