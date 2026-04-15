use super::super::{LogicalNodeProcessor, ProcessableNode, PushDownBuilderContext};
use crate::logical_plan::ResolveMultipliedMeasures;
use crate::physical_plan_builder::PhysicalPlanBuilder;
use crate::plan::{QueryPlan, SingleSource};
use cubenativeutils::CubeError;
use std::rc::Rc;

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
        resolve_multiplied_measures: &ResolveMultipliedMeasures,
        context: &PushDownBuilderContext,
    ) -> Result<Self::PhysycalNode, CubeError> {
        let mut joins = Vec::new();
        for multiplied_measure_subquery in resolve_multiplied_measures
            .aggregate_multiplied_subqueries
            .iter()
        {
            let select = self
                .builder
                .process_node(multiplied_measure_subquery.as_ref(), context)?;
            let source = SingleSource::Subquery(Rc::new(QueryPlan::Select(select)));
            joins.push(source);
        }
        Ok(joins)
    }
}

impl ProcessableNode for ResolveMultipliedMeasures {
    type ProcessorType<'a> = ResolveMultipliedMeasuresProcessor<'a>;
}
