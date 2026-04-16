use super::super::{LogicalNodeProcessor, ProcessableNode, PushDownBuilderContext};
use crate::logical_plan::ResolveMultipliedMeasures;
use crate::physical_plan_builder::PhysicalPlanBuilder;
use crate::plan::SingleSource;
use cubenativeutils::CubeError;

pub struct ResolveMultipliedMeasuresProcessor<'a> {
    builder: &'a PhysicalPlanBuilder,
}

impl<'a> LogicalNodeProcessor<'a, ResolveMultipliedMeasures>
    for ResolveMultipliedMeasuresProcessor<'a>
{
    type PhysycalNode = Vec<SingleSource>;
    fn new(builder: &'a PhysicalPlanBuilder) -> Self {
        Self { builder }
    }

    fn process(
        &self,
        _resolve_multiplied_measures: &ResolveMultipliedMeasures,
        _context: &PushDownBuilderContext,
    ) -> Result<Self::PhysycalNode, CubeError> {
        // Multiplied measures are now CTEs, this processor is only kept for PreAggregation path
        Ok(vec![])
    }
}

impl ProcessableNode for ResolveMultipliedMeasures {
    type ProcessorType<'a> = ResolveMultipliedMeasuresProcessor<'a>;
}
