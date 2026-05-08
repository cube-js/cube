use crate::physical_plan::sql_nodes::SqlNode;
use crate::physical_plan::SqlEvaluatorVisitor;
use crate::planner::query_tools::QueryTools;
use crate::planner::sql_templates::PlanSqlTemplates;
use crate::planner::MemberSymbol;
use cubenativeutils::CubeError;
use std::any::Any;
use std::rc::Rc;

use super::{Op, OpCtx, OpExec};

/// Adapter exposing a `Vec<Op>` pipeline as a `SqlNode`. Lets a pipeline be
/// dropped into the legacy `Rc<dyn SqlNode>` tree built by `SqlNodesFactory`,
/// so migration can proceed one node at a time.
pub struct OpPipelineSqlNode {
    ops: Vec<Op>,
}

impl OpPipelineSqlNode {
    pub fn new(ops: Vec<Op>) -> Rc<Self> {
        Rc::new(Self { ops })
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
