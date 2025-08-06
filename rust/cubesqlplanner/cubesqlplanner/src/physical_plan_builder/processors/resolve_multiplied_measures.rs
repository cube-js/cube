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
        for (i, regular_measure_subquery) in resolve_multiplied_measures
            .regular_measure_subqueries
            .iter()
            .enumerate()
        {
            let mut regular_measure_context = context.clone();
            regular_measure_context.alias_prefix = if i == 0 {
                Some(format!("main"))
            } else {
                Some(format!("main_{}", i))
            };
            let select = self
                .builder
                .process_node(regular_measure_subquery.as_ref(), &regular_measure_context)?;
            let source = SingleSource::Subquery(Rc::new(QueryPlan::Select(select)));
            joins.push(source);
        }
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
