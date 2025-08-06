use crate::physical_plan_builder::PhysicalPlanBuilder;

use super::context::PushDownBuilderContext;
use cubenativeutils::CubeError;

pub(super) trait LogicalNodeProcessor<'a, LogicalNode> {
    type PhysycalNode;
    fn new(builder: &'a PhysicalPlanBuilder) -> Self;
    fn process(
        &self,
        logical_plan: &LogicalNode,
        context: &PushDownBuilderContext,
    ) -> Result<Self::PhysycalNode, CubeError>;
}

pub(super) trait ProcessableNode: Sized {
    type ProcessorType<'a>: LogicalNodeProcessor<'a, Self>;
}
