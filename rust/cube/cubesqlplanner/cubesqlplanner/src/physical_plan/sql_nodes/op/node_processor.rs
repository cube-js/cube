use crate::physical_plan::SqlEvaluatorVisitor;
use crate::planner::query_tools::QueryTools;
use crate::planner::sql_templates::PlanSqlTemplates;
use crate::planner::MemberSymbol;
use cubenativeutils::CubeError;
use std::cell::OnceCell;
use std::rc::Rc;

use super::{Op, OpCtx, OpExec};

/// `Rc`-shareable carrier of a validated `Vec<Op>`. Renders the SQL of a
/// member symbol by running the pipeline once via [`Self::to_sql`].
pub struct NodeProcessor {
    ops: Vec<Op>,
    /// Caches the structural validation result so a pipeline reused across
    /// many renders pays the `validate_pipeline` cost only once.
    validated: OnceCell<()>,
}

impl NodeProcessor {
    pub fn new(ops: Vec<Op>) -> Rc<Self> {
        Rc::new(Self {
            ops,
            validated: OnceCell::new(),
        })
    }

    pub fn to_sql(
        &self,
        visitor: &SqlEvaluatorVisitor,
        node: &Rc<MemberSymbol>,
        query_tools: Rc<QueryTools>,
        node_processor: Rc<NodeProcessor>,
        templates: &PlanSqlTemplates,
    ) -> Result<String, CubeError> {
        if self.validated.get().is_none() {
            Op::validate_pipeline(&self.ops)?;
            let _ = self.validated.set(());
        }
        let (op, tail) = self.ops.split_first().ok_or_else(|| {
            CubeError::internal("NodeProcessor invoked with empty pipeline".to_string())
        })?;
        let mut ctx = OpCtx {
            visitor: visitor.clone(),
            query_tools,
            templates,
            sym: node.clone(),
            tail,
            node_processor,
        };
        op.exec(&mut ctx)
    }
}
