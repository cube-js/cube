use crate::physical_plan::sql_nodes::SqlNode;
use crate::physical_plan::SqlEvaluatorVisitor;
use crate::planner::query_tools::QueryTools;
use crate::planner::sql_templates::PlanSqlTemplates;
use crate::planner::MemberSymbol;
use cubenativeutils::CubeError;
use std::rc::Rc;

use super::{Op, OpExec, OpPipelineSqlNode};

/// Per-render context passed to an Op handler. Holds the visitor and shared
/// resources, plus the slice of the pipeline yet to be processed (`tail`).
///
/// Op handlers continue the chain via [`render_tail`], dive into a side
/// pipeline (e.g. RollingWindow's `input_pipeline`) via [`render_pipeline`],
/// and may temporarily override the visitor via [`with_visitor`].
///
/// `legacy_node_processor` is the bridge to the existing `Rc<dyn SqlNode>`
/// world: the leaf `EvaluateSymbol` op forwards it to `MemberSqlContext`,
/// and during migration any sub-render that still depends on legacy plumbing
/// goes through it.
pub struct OpCtx<'a> {
    pub visitor: SqlEvaluatorVisitor,
    pub query_tools: Rc<QueryTools>,
    pub templates: &'a PlanSqlTemplates,
    pub sym: Rc<MemberSymbol>,
    pub tail: &'a [Op],
    pub legacy_node_processor: Rc<dyn SqlNode>,
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
            legacy_node_processor: self.legacy_node_processor.clone(),
        };
        op.exec(&mut sub)
    }

    /// Run a separate pipeline (e.g. RollingWindow's `input_pipeline` or a
    /// branch of a kind dispatch). The slice may live for any lifetime
    /// shorter than the outer ctx's; the templates reference is reborrowed
    /// to match.
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
            legacy_node_processor: self.legacy_node_processor.clone(),
        };
        op.exec(&mut sub)
    }

    /// Materialize the remaining pipeline as a `SqlNode`. Used by ops that
    /// need to hand the rest of the chain to legacy plumbing — e.g. as a
    /// `node_processor` for a filter expression that must avoid recursing
    /// back through the current op.
    ///
    /// Cost: `O(tail_len)` Rc clones plus one `Rc<OpPipelineSqlNode>`
    /// allocation per call — heavier than the legacy `Rc::clone` of an
    /// already-built node. Fine for cold paths (mask filter rendering); call
    /// sparingly on hot paths.
    pub fn tail_as_sql_node(&self) -> Rc<dyn SqlNode> {
        OpPipelineSqlNode::new(self.tail.to_vec())
    }

    /// Build a fresh ctx pointing at the same tail/symbol but with a different
    /// visitor — used by ops that need to flip `arg_needs_paren_safe` etc.
    pub fn with_visitor(&self, visitor: SqlEvaluatorVisitor) -> OpCtx<'a> {
        OpCtx {
            visitor,
            query_tools: self.query_tools.clone(),
            templates: self.templates,
            sym: self.sym.clone(),
            tail: self.tail,
            legacy_node_processor: self.legacy_node_processor.clone(),
        }
    }
}
