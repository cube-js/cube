use super::super::{LogicalNodeProcessor, ProcessableNode, PushDownBuilderContext};
use crate::logical_plan::CalculationSource;
use crate::physical_plan::From;
use crate::physical_plan_builder::PhysicalPlanBuilder;
use cubenativeutils::CubeError;
use std::rc::Rc;

pub struct CalculationSourceProcessor<'a> {
    builder: &'a PhysicalPlanBuilder,
}

impl<'a> LogicalNodeProcessor<'a, CalculationSource> for CalculationSourceProcessor<'a> {
    type PhysycalNode = Rc<From>;
    fn new(builder: &'a PhysicalPlanBuilder) -> Self {
        Self { builder }
    }

    fn process(
        &self,
        source: &CalculationSource,
        context: &PushDownBuilderContext,
    ) -> Result<Self::PhysycalNode, CubeError> {
        match source {
            CalculationSource::Merge(item) => self.builder.process_node(item.as_ref(), context),
            CalculationSource::GridLookup(item) => {
                self.builder.process_node(item.as_ref(), context)
            }
        }
    }
}

impl ProcessableNode for CalculationSource {
    type ProcessorType<'a> = CalculationSourceProcessor<'a>;
}
