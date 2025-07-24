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
