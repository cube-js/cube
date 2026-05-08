use crate::planner::MemberSymbol;
use cubenativeutils::CubeError;

use super::{Op, OpCtx, OpExec};

/// Renders a multi-stage non-calculated measure as a windowed aggregate
/// over its `input_pipeline` (`agg(agg(input)) OVER (PARTITION BY …)`).
/// Other measures take the `else_pipeline` branch.
#[derive(Clone)]
pub struct MultiStageWindowOp {
    input_pipeline: Vec<Op>,
    else_pipeline: Vec<Op>,
    partition: Vec<String>,
}

impl MultiStageWindowOp {
    pub fn new(input_pipeline: Vec<Op>, else_pipeline: Vec<Op>, partition: Vec<String>) -> Self {
        Self {
            input_pipeline,
            else_pipeline,
            partition,
        }
    }
}

impl OpExec for MultiStageWindowOp {
    fn exec(&self, ctx: &mut OpCtx<'_>) -> Result<String, CubeError> {
        let MemberSymbol::Measure(m) = ctx.sym.as_ref() else {
            return Err(CubeError::internal(
                "MultiStageWindow op called for non-measure symbol".to_string(),
            ));
        };
        if !(m.is_multi_stage() && !m.is_calculated()) {
            return ctx.render_pipeline(&self.else_pipeline);
        }
        let measure_type = m.measure_type();
        let inner_visitor = ctx.visitor.with_arg_needs_paren_safe(false);
        let input_sql = ctx
            .with_visitor(inner_visitor)
            .render_pipeline(&self.input_pipeline)?;
        let partition_by = if self.partition.is_empty() {
            String::new()
        } else {
            format!("PARTITION BY {} ", self.partition.join(", "))
        };
        Ok(format!(
            "{measure_type}({measure_type}({input_sql})) OVER ({partition_by})"
        ))
    }
}
