use crate::planner::MemberSymbol;
use cubenativeutils::CubeError;

use super::{Op, OpCtx, OpExec};

/// Top-level dispatch over the symbol kind: dimensions, time dimensions,
/// measures and everything else each follow their own rendering pipeline.
/// Discards the tail — each branch is a self-contained pipeline.
#[derive(Clone)]
pub struct DispatchByKindOp {
    dimension: Vec<Op>,
    time_dimension: Vec<Op>,
    measure: Vec<Op>,
    default: Vec<Op>,
}

impl DispatchByKindOp {
    pub fn new(
        dimension: Vec<Op>,
        time_dimension: Vec<Op>,
        measure: Vec<Op>,
        default: Vec<Op>,
    ) -> Self {
        Self {
            dimension,
            time_dimension,
            measure,
            default,
        }
    }
}

impl OpExec for DispatchByKindOp {
    fn exec(&self, ctx: &mut OpCtx<'_>) -> Result<String, CubeError> {
        let pipeline = match ctx.sym.as_ref() {
            MemberSymbol::Dimension(_) => &self.dimension,
            MemberSymbol::TimeDimension(_) => &self.time_dimension,
            MemberSymbol::Measure(_) => &self.measure,
            _ => &self.default,
        };
        ctx.render_pipeline(pipeline)
    }
}
