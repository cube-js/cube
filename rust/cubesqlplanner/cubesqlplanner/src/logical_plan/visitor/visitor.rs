use cubenativeutils::CubeError;

use crate::logical_plan::{LogicalNode, PlanNode};
pub trait LogicalNodeVisitor {
    fn process_node(&mut self, node: &PlanNode) -> Result<(), CubeError>;
}

pub struct LogicalPlanVisitor<'a, T: LogicalNodeVisitor> {
    node_visitor: &'a mut T,
}

impl<'a, T: LogicalNodeVisitor> LogicalPlanVisitor<'a, T> {
    pub fn new(node_visitor: &'a mut T) -> Self {
        Self { node_visitor }
    }

    pub fn visit<N: LogicalNode>(&self, node: &Rc<N>) -> Result<(), CubeError> {
        self.visit_impl(node.as_plan_node())
    }

    fn visit_impl(&self, node: &PlanNode) -> Result<(), CubeError> {
        self.node_visitor.process_node(&node)?;
    }
}

