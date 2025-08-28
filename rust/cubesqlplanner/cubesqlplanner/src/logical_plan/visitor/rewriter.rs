use crate::logical_plan::{LogicalNode, PlanNode};
use cubenativeutils::CubeError;
use std::rc::Rc;

pub struct NodeRewriteResult {
    rewritten: Option<PlanNode>,
    stop: bool,
}

impl NodeRewriteResult {
    pub fn rewritten(rewritten_node: PlanNode) -> Self {
        Self {
            rewritten: Some(rewritten_node),
            stop: true,
        }
    }

    pub fn stop() -> Self {
        Self {
            rewritten: None,
            stop: true,
        }
    }

    pub fn pass() -> Self {
        Self {
            rewritten: None,
            stop: false,
        }
    }
}

pub trait LogicalNodeRewriter {
    fn process_node(&mut self, node: &PlanNode) -> Result<NodeRewriteResult, CubeError>;
}

pub struct LogicalPlanRewriter {}

impl LogicalPlanRewriter {
    pub fn new() -> Self {
        Self {}
    }

    pub fn rewrite_top_down<T: LogicalNodeRewriter, N: LogicalNode>(
        &self,
        node: Rc<N>,
        node_visitor: &mut T,
    ) -> Result<Rc<N>, CubeError> {
        let res = if let Some(rewrited) =
            self.rewrite_top_down_impl(node.as_plan_node(), node_visitor)?
        {
            rewrited.into_logical_node()?
        } else {
            node
        };
        Ok(res)
    }

    pub fn rewrite_top_down_with<F, N: LogicalNode>(
        &self,
        node: Rc<N>,
        f: F,
    ) -> Result<Rc<N>, CubeError>
    where
        F: FnMut(&PlanNode) -> Result<NodeRewriteResult, CubeError>,
    {
        struct FnWrapper<F>(F);

        impl<F> LogicalNodeRewriter for FnWrapper<F>
        where
            F: FnMut(&PlanNode) -> Result<NodeRewriteResult, CubeError>,
        {
            fn process_node(&mut self, node: &PlanNode) -> Result<NodeRewriteResult, CubeError> {
                (self.0)(node)
            }
        }

        let mut wrapper = FnWrapper(f);
        self.rewrite_top_down(node, &mut wrapper)
    }

    fn rewrite_top_down_impl<T: LogicalNodeRewriter>(
        &self,
        node: PlanNode,
        node_visitor: &mut T,
    ) -> Result<Option<PlanNode>, CubeError> {
        let NodeRewriteResult { stop, rewritten } = node_visitor.process_node(&node)?;
        if let Some(rewritten) = rewritten {
            return Ok(Some(rewritten));
        }
        if stop {
            return Ok(None);
        }
        let mut has_changes = false;
        let mut inputs = node.inputs();
        for input in inputs.iter_mut() {
            if let Some(rewrited) = self.rewrite_top_down_impl(input.clone(), node_visitor)? {
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
