use crate::physical_plan::sql_nodes::SqlNode;
use cubenativeutils::CubeError;
use std::rc::Rc;

use super::{EvaluateSymbolOp, LegacySqlNodeOp, OpCtx, OpExec, ParenthesizeOp};

/// All op variants that participate in pipeline rendering.
///
/// Adding a new op = new variant here + new dispatch arm in [`OpExec for Op`]
/// + (preferably) a constructor on `impl Op`. The compiler enforces
/// exhaustiveness on the dispatch — there is no central match with logic to
/// keep in sync; per-variant logic lives in its own struct's `OpExec` impl.
///
/// `LegacySqlNode` is a migration-only escape hatch that wraps an
/// `Rc<dyn SqlNode>`; it goes away once every legacy node has been migrated.
pub enum Op {
    EvaluateSymbol(EvaluateSymbolOp),
    Parenthesize(ParenthesizeOp),
    LegacySqlNode(LegacySqlNodeOp),
}

impl Op {
    pub fn evaluate_symbol() -> Self {
        Self::EvaluateSymbol(EvaluateSymbolOp)
    }

    pub fn parenthesize() -> Self {
        Self::Parenthesize(ParenthesizeOp)
    }

    pub fn legacy(node: Rc<dyn SqlNode>) -> Self {
        Self::LegacySqlNode(LegacySqlNodeOp::new(node))
    }
}

impl OpExec for Op {
    fn exec(&self, ctx: &mut OpCtx<'_>) -> Result<String, CubeError> {
        match self {
            Op::EvaluateSymbol(o) => o.exec(ctx),
            Op::Parenthesize(o) => o.exec(ctx),
            Op::LegacySqlNode(o) => o.exec(ctx),
        }
    }
}
