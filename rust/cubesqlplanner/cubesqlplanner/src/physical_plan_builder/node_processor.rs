use super::context::*;
use cubenativeutils::CubeError;
use std::rc::Rc;

pub(super) trait LogicalNodeProcessor {
    type LogicalNode;
    type PhysycalNode;
    fn process(
        logical_plan: &Rc<Self::LogicalNode>,
        context: &PushDownBuilderContext,
    ) -> Result<(Self::PhysycalNode, PullUpBuilderContext), CubeError>;
}
