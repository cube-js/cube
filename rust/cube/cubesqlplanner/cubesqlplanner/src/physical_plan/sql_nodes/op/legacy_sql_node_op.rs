use crate::physical_plan::sql_nodes::SqlNode;
use cubenativeutils::CubeError;
use std::rc::Rc;

use super::{OpCtx, OpExec};

/// Migration escape hatch: wraps a legacy `Rc<dyn SqlNode>` so it can sit
/// inside an Op pipeline. The op simply forwards `exec` to the wrapped node's
/// `to_sql`, threading the visitor and the current `legacy_node_processor`
/// through unchanged.
///
/// Acts as the boundary between an already-migrated outer pipeline and a
/// not-yet-migrated inner subtree. Once every legacy node has its Op
/// counterpart, this variant can be removed.
pub struct LegacySqlNodeOp {
    pub inner: Rc<dyn SqlNode>,
}

impl LegacySqlNodeOp {
    pub fn new(inner: Rc<dyn SqlNode>) -> Self {
        Self { inner }
    }
}

impl OpExec for LegacySqlNodeOp {
    fn exec(&self, ctx: &mut OpCtx<'_>) -> Result<String, CubeError> {
        self.inner.to_sql(
            &ctx.visitor,
            &ctx.sym,
            ctx.query_tools.clone(),
            ctx.legacy_node_processor.clone(),
            ctx.templates,
        )
    }
}
