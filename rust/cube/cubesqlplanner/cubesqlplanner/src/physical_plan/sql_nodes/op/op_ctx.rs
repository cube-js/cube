use crate::physical_plan::SqlEvaluatorVisitor;
use crate::planner::query_tools::QueryTools;
use crate::planner::sql_templates::PlanSqlTemplates;
use crate::planner::MemberSymbol;
use cubenativeutils::CubeError;
use std::rc::Rc;

use super::{NodeProcessor, Op, OpExec};

/// State of one render in flight: the symbol being rendered, the visitor
/// and shared resources, the unprocessed `tail` of the current pipeline,
/// and the top-level `node_processor` to re-enter through for sub-arg
/// evaluation.
pub struct OpCtx<'a> {
    pub visitor: SqlEvaluatorVisitor,
    pub query_tools: Rc<QueryTools>,
    pub templates: &'a PlanSqlTemplates,
    pub sym: Rc<MemberSymbol>,
    pub tail: &'a [Op],
    pub node_processor: Rc<NodeProcessor>,
}

impl<'a> OpCtx<'a> {
    /// Continue with the next op in the current pipeline. Errors if `tail` is
    /// empty (i.e. the pipeline ended without a terminal op).
    pub fn render_tail(&self) -> Result<String, CubeError> {
        let (op, rest) = self.tail.split_first().ok_or_else(|| {
            CubeError::internal(
                "OpCtx::render_tail called on empty tail — pipeline missing terminal op"
                    .to_string(),
            )
        })?;
        let mut sub = OpCtx {
            visitor: self.visitor.clone(),
            query_tools: self.query_tools.clone(),
            templates: self.templates,
            sym: self.sym.clone(),
            tail: rest,
            node_processor: self.node_processor.clone(),
        };
        op.exec(&mut sub)
    }

    /// Run a separate sub-pipeline against the current symbol/visitor. The
    /// slice may live for any lifetime shorter than the outer ctx's; the
    /// templates reference is reborrowed to match.
    pub fn render_pipeline<'b>(&self, ops: &'b [Op]) -> Result<String, CubeError>
    where
        'a: 'b,
    {
        let (op, rest) = ops.split_first().ok_or_else(|| {
            CubeError::internal("OpCtx::render_pipeline called with empty ops slice".to_string())
        })?;
        let mut sub = OpCtx::<'b> {
            visitor: self.visitor.clone(),
            query_tools: self.query_tools.clone(),
            templates: self.templates,
            sym: self.sym.clone(),
            tail: rest,
            node_processor: self.node_processor.clone(),
        };
        op.exec(&mut sub)
    }

    /// Materialize the remaining pipeline as a standalone `NodeProcessor`,
    /// suitable as a re-entry point for plumbing that must not recurse
    /// through the current op (e.g. a filter rendered without the masking
    /// wrapper).
    ///
    /// Cost: `O(tail_len)` Op clones plus one `Rc<NodeProcessor>` allocation
    /// per call. Cheap enough for cold paths; avoid on hot ones.
    pub fn tail_as_node_processor(&self) -> Rc<NodeProcessor> {
        NodeProcessor::new(self.tail.to_vec())
    }

    /// Fresh ctx pointing at the same tail/symbol but with a swapped visitor
    /// (e.g. a child render that needs different `arg_needs_paren_safe` or
    /// `ignore_tz_convert` flags).
    pub fn with_visitor(&self, visitor: SqlEvaluatorVisitor) -> OpCtx<'a> {
        OpCtx {
            visitor,
            query_tools: self.query_tools.clone(),
            templates: self.templates,
            sym: self.sym.clone(),
            tail: self.tail,
            node_processor: self.node_processor.clone(),
        }
    }
}
