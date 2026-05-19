use crate::logical_plan::{LogicalNode, PlanNode};
use cubenativeutils::CubeError;
use std::rc::Rc;

pub trait LogicalNodeVisitor {
    fn process_node(&mut self, node: &PlanNode) -> Result<(), CubeError>;
}

pub struct LogicalPlanVisitor {}

impl LogicalPlanVisitor {
    pub fn new() -> Self {
        Self {}
    }

    pub fn visit<T: LogicalNodeVisitor, N: LogicalNode>(
        &self,
        node_visitor: &mut T,
        node: &Rc<N>,
    ) -> Result<(), CubeError> {
        self.visit_impl(node_visitor, &node.as_plan_node())
    }

    pub fn visit_with<F, N: LogicalNode>(&self, node: &Rc<N>, f: F) -> Result<(), CubeError>
    where
        F: FnMut(&PlanNode) -> Result<(), CubeError>,
    {
        struct FnWrapper<F>(F);

        impl<F> LogicalNodeVisitor for FnWrapper<F>
        where
            F: FnMut(&PlanNode) -> Result<(), CubeError>,
        {
            fn process_node(&mut self, node: &PlanNode) -> Result<(), CubeError> {
                (self.0)(node)
            }
        }

        let mut wrapper = FnWrapper(f);
        self.visit(&mut wrapper, node)
    }

    /// Visit a subtree rooted at a `PlanNode` directly. Used by callers
    /// outside the `LogicalNode` trait (e.g. `LogicalPlan` which doesn't
    /// fit the trait because it lives above the PlanNode hierarchy).
    pub fn visit_plan_node<T: LogicalNodeVisitor>(
        &self,
        node_visitor: &mut T,
        node: &PlanNode,
    ) -> Result<(), CubeError> {
        self.visit_impl(node_visitor, node)
    }

    fn visit_impl<T: LogicalNodeVisitor>(
        &self,
        node_visitor: &mut T,
        node: &PlanNode,
    ) -> Result<(), CubeError> {
        node_visitor.process_node(&node)?;
        for input in node.inputs() {
            self.visit_impl(node_visitor, &input)?;
        }

        Ok(())
    }
}
