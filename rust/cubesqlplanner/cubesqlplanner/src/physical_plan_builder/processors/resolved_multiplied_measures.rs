use super::super::{LogicalNodeProcessor, ProcessableNode, PushDownBuilderContext};
use crate::logical_plan::ResolvedMultipliedMeasures;
use crate::physical_plan_builder::PhysicalPlanBuilder;
use crate::plan::SingleSource;
use cubenativeutils::CubeError;

pub struct ResolvedMultipliedMeasuresProcessor<'a> {
    builder: &'a PhysicalPlanBuilder,
}

impl<'a> LogicalNodeProcessor<'a, ResolvedMultipliedMeasures>
    for ResolvedMultipliedMeasuresProcessor<'a>
{
    type PhysycalNode = Vec<SingleSource>;
    fn new(builder: &'a PhysicalPlanBuilder) -> Self {
        Self { builder }
    }

    fn process(
        &self,
        resolved_multiplied_measures: &ResolvedMultipliedMeasures,
        context: &PushDownBuilderContext,
    ) -> Result<Self::PhysycalNode, CubeError> {
        match resolved_multiplied_measures {
            ResolvedMultipliedMeasures::ResolveMultipliedMeasures(resolve_multiplied_measures) => {
                self.builder
                    .process_node(resolve_multiplied_measures.as_ref(), context)
            }
            ResolvedMultipliedMeasures::PreAggregation(_pre_aggregation_query) => {
                todo!()
                /* let pre_aggregation_query =
                    self.build_simple_query(pre_aggregation_query, context)?;
                let source =
                    SingleSource::Subquery(Rc::new(QueryPlan::Select(pre_aggregation_query)));
                Ok(vec![source]) */
            }
        }
    }
}

impl ProcessableNode for ResolvedMultipliedMeasures {
    type ProcessorType<'a> = ResolvedMultipliedMeasuresProcessor<'a>;
}
