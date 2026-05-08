use crate::physical_plan::sql_nodes::SqlNode;
use cubenativeutils::CubeError;
use std::rc::Rc;

use super::{OpCtx, OpExec};

/// Migration-only bridge: lets an Op pipeline contain a not-yet-migrated
/// `SqlNode` subtree. Goes away once every node has its Op counterpart.
#[derive(Clone)]
pub struct LegacySqlNodeOp {
    inner: Rc<dyn SqlNode>,
}

impl LegacySqlNodeOp {
    pub fn new(inner: Rc<dyn SqlNode>) -> Self {
        Self { inner }
    }
}

impl OpExec for LegacySqlNodeOp {
    fn is_terminal(&self) -> bool {
        true
    }

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
