use crate::physical_plan::sql_nodes::SqlNode;
use crate::physical_plan::SqlEvaluatorVisitor;
use crate::planner::query_tools::QueryTools;
use crate::planner::sql_templates::PlanSqlTemplates;
use crate::planner::MemberSymbol;
use cubenativeutils::CubeError;
use std::any::Any;
use std::cell::OnceCell;
use std::rc::Rc;

use super::{Op, OpCtx, OpExec};

/// Migration-only bridge: lets an Op pipeline plug into the existing
/// `SqlNode` tree wherever the rest of the planner expects one. Goes away
/// once consumers switch from `Rc<dyn SqlNode>` to a `Plan`.
pub struct OpPipelineSqlNode {
    ops: Vec<Op>,
    /// Caches the structural validation result so a pipeline reused across
    /// many renders pays the `validate_pipeline` cost only once.
    validated: OnceCell<()>,
}

impl OpPipelineSqlNode {
    pub fn new(ops: Vec<Op>) -> Rc<Self> {
        Rc::new(Self {
            ops,
            validated: OnceCell::new(),
        })
    }

    pub fn ops(&self) -> &[Op] {
        &self.ops
    }
}

impl SqlNode for OpPipelineSqlNode {
    fn to_sql(
        &self,
        visitor: &SqlEvaluatorVisitor,
        node: &Rc<MemberSymbol>,
        query_tools: Rc<QueryTools>,
        node_processor: Rc<dyn SqlNode>,
        templates: &PlanSqlTemplates,
    ) -> Result<String, CubeError> {
        if self.validated.get().is_none() {
            Op::validate_pipeline(&self.ops)?;
            let _ = self.validated.set(());
        }
        let (op, tail) = self.ops.split_first().ok_or_else(|| {
            CubeError::internal("OpPipelineSqlNode invoked with empty pipeline".to_string())
        })?;
        let mut ctx = OpCtx {
            visitor: visitor.clone(),
            query_tools,
            templates,
            sym: node.clone(),
            tail,
            legacy_node_processor: node_processor,
        };
        op.exec(&mut ctx)
    }

    fn as_any(self: Rc<Self>) -> Rc<dyn Any> {
        self.clone()
    }

    fn childs(&self) -> Vec<Rc<dyn SqlNode>> {
        vec![]
    }
}
