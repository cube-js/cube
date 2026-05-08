use crate::planner::MemberSymbol;
use cubenativeutils::CubeError;

use super::{OpCtx, OpExec};

/// Applies a measure's per-row filter rules so only matching rows
/// contribute to its aggregation.
#[derive(Clone)]
pub struct MeasureFilterOp;

impl OpExec for MeasureFilterOp {
    fn exec(&self, ctx: &mut OpCtx<'_>) -> Result<String, CubeError> {
        match ctx.sym.as_ref() {
            MemberSymbol::Measure(ev) => {
                let measure_filters = ev.measure_filters();
                if measure_filters.is_empty() {
                    return ctx.render_tail();
                }
                let inner_visitor = ctx.visitor.with_arg_needs_paren_safe(false);
                let input = ctx.with_visitor(inner_visitor.clone()).render_tail()?;
                let filters = measure_filters
                    .iter()
                    .map(|filter| -> Result<String, CubeError> {
                        Ok(format!(
                            "({})",
                            filter.eval(
                                &inner_visitor,
                                ctx.node_processor.clone(),
                                ctx.query_tools.clone(),
                                ctx.templates,
                            )?
                        ))
                    })
                    .collect::<Result<Vec<_>, _>>()?
                    .join(" AND ");
                let result = if input.as_str() == "*" {
                    "1".to_string()
                } else {
                    input
                };
                Ok(format!("CASE WHEN {} THEN {} END", filters, result))
            }
            _ => Err(CubeError::internal(
                "MeasureFilter op called for non-measure symbol".to_string(),
            )),
        }
    }
}
