use crate::physical_plan::symbols::{MemberSqlContext, ToSql};
use cubenativeutils::CubeError;

use super::{OpCtx, OpExec};

/// Produces the base SQL of a member from its own definition (column ref
/// or `sql:` expression). Terminal step of any rendering pipeline.
#[derive(Clone)]
pub struct EvaluateSymbolOp;

impl OpExec for EvaluateSymbolOp {
    fn is_terminal(&self) -> bool {
        true
    }

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
