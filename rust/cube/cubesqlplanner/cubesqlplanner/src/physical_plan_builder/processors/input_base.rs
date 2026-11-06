use super::super::{LogicalNodeProcessor, ProcessableNode, PushDownBuilderContext};
use crate::logical_plan::InputBase;
use crate::physical_plan::From;
use crate::physical_plan_builder::PhysicalPlanBuilder;
use cubenativeutils::CubeError;
use std::rc::Rc;

pub struct InputBaseProcessor<'a> {
    builder: &'a PhysicalPlanBuilder,
}

impl<'a> LogicalNodeProcessor<'a, InputBase> for InputBaseProcessor<'a> {
    type PhysycalNode = Rc<From>;
    fn new(builder: &'a PhysicalPlanBuilder) -> Self {
        Self { builder }
    }

    fn process(
        &self,
        query_source: &InputBase,
        context: &PushDownBuilderContext,
    ) -> Result<Self::PhysycalNode, CubeError> {
        match query_source {
            InputBase::QueryInputs(item) => self.builder.process_node(item.as_ref(), context),
            InputBase::Merge(item) => self.builder.process_node(item.as_ref(), context),
            InputBase::PreAggregation(item) => self.builder.process_node(item.as_ref(), context),
        }
    }
}

impl ProcessableNode for InputBase {
    type ProcessorType<'a> = InputBaseProcessor<'a>;
}
