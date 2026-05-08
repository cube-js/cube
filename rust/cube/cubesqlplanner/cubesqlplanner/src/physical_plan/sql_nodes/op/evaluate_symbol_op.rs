use crate::physical_plan::symbols::{MemberSqlContext, ToSql};
use cubenativeutils::CubeError;

use super::{OpCtx, OpExec};

/// Terminal op: defers to the symbol's own `ToSql` evaluator, threading the
/// current visitor and `legacy_node_processor` through `MemberSqlContext`.
///
/// Mirrors the legacy `EvaluateSqlNode`.
pub struct EvaluateSymbolOp;

impl OpExec for EvaluateSymbolOp {
    fn exec(&self, ctx: &mut OpCtx<'_>) -> Result<String, CubeError> {
        let path = ctx.sym.compiled_path();
        let member_ctx = MemberSqlContext {
            visitor: &ctx.visitor,
            node_processor: &ctx.legacy_node_processor,
            query_tools: &ctx.query_tools,
            templates: ctx.templates,
            name: path.name(),
            full_name: path.full_name(),
        };
        ctx.sym.as_ref().to_sql(&member_ctx)
    }
}
