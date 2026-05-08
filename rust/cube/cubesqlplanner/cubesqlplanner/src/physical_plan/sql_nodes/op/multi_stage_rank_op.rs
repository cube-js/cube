use crate::planner::symbols::MeasureKind;
use crate::planner::MemberSymbol;
use cubenativeutils::CubeError;

use super::{OpCtx, OpExec};

/// Renders a multi-stage rank measure as a SQL `RANK() OVER (PARTITION BY …
/// ORDER BY …)`. Other measures fall through to the rest of the pipeline.
#[derive(Clone, Debug)]
pub struct MultiStageRankOp {
    partition: Vec<String>,
}

impl MultiStageRankOp {
    pub fn new(partition: Vec<String>) -> Self {
        Self { partition }
    }
}

impl OpExec for MultiStageRankOp {
    fn exec(&self, ctx: &mut OpCtx<'_>) -> Result<String, CubeError> {
        let MemberSymbol::Measure(m) = ctx.sym.as_ref() else {
            return Err(CubeError::internal(
                "MultiStageRank op called for non-measure symbol".to_string(),
            ));
        };
        if !(m.is_multi_stage() && matches!(m.kind(), MeasureKind::Rank)) {
            return ctx.render_tail();
        }
        let inner_visitor = ctx.visitor.with_arg_needs_paren_safe(false);
        let order_by = if !m.measure_order_by().is_empty() {
            let sql = m
                .measure_order_by()
                .iter()
                .map(|item| -> Result<String, CubeError> {
                    let sql = item.sql_call().eval(
                        &inner_visitor,
                        ctx.node_processor.clone(),
                        ctx.query_tools.clone(),
                        ctx.templates,
                    )?;
                    Ok(format!("{} {}", sql, item.direction()))
                })
                .collect::<Result<Vec<_>, _>>()?
                .join(", ");
            format!("ORDER BY {sql}")
        } else {
            String::new()
        };
        let partition_by = if self.partition.is_empty() {
            String::new()
        } else {
            format!("PARTITION BY {} ", self.partition.join(", "))
        };
        Ok(format!("rank() OVER ({partition_by}{order_by})"))
    }
}
