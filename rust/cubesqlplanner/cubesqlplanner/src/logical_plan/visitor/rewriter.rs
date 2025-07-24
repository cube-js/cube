use crate::logical_plan::{LogicalNode, PlanNode};
use cubenativeutils::CubeError;
use std::rc::Rc;

pub trait LogicalNodeRewriter {
    fn process_node(&mut self, node: &PlanNode) -> Result<Option<PlanNode>, CubeError>;
}

pub struct LogicalPlanRewriter {}

impl LogicalPlanRewriter {
    pub fn new() -> Self {
        Self {}
    }

    pub fn rewrite_top_down<T: LogicalNodeRewriter, N: LogicalNode>(
        &self,
        node_visitor: &mut T,
        node: Rc<N>,
    ) -> Result<Rc<N>, CubeError> {
        let res = if let Some(rewrited) =
            self.rewrite_top_down_impl(node_visitor, node.as_plan_node())?
        {
            rewrited.into_logical_node()?
        } else {
            node
        };
        Ok(res)
    }

    fn rewrite_top_down_impl<T: LogicalNodeRewriter>(
        &self,
        node_visitor: &mut T,
        node: PlanNode,
    ) -> Result<Option<PlanNode>, CubeError> {
        if let Some(rewrited) = node_visitor.process_node(&node)? {
            return Ok(Some(rewrited));
        }
        let mut has_changes = false;
        let mut inputs = node.inputs();
        for input in inputs.iter_mut() {
            if let Some(rewrited) = self.rewrite_top_down_impl(node_visitor, input.clone())? {
                *input = rewrited;
                has_changes = true;
            }
        }
        let res = if has_changes {
            Some(node.with_inputs(inputs)?)
        } else {
            None
        };

        Ok(res)
    }
}
