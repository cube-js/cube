use crate::planner::MemberSymbol;
use cubenativeutils::CubeError;

use super::{OpCtx, OpExec};

/// Renders a measure for an ungrouped query at the measure level — the
/// expression is left unaggregated, with the lone exception of `count(*)`
/// which becomes `1` per row.
#[derive(Clone)]
pub struct UngroupedMeasureOp;

impl OpExec for UngroupedMeasureOp {
    fn exec(&self, ctx: &mut OpCtx<'_>) -> Result<String, CubeError> {
        let MemberSymbol::Measure(_) = ctx.sym.as_ref() else {
            return Err(CubeError::internal(
                "UngroupedMeasure op called for non-measure symbol".to_string(),
            ));
        };
        let input = ctx.render_tail()?;
        Ok(if input == "*" { "1".to_string() } else { input })
    }
}
