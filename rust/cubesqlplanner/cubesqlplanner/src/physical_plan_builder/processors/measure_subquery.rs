use super::super::{LogicalNodeProcessor, ProcessableNode, PushDownBuilderContext};
use crate::logical_plan::MeasureSubquery;
use crate::physical_plan_builder::PhysicalPlanBuilder;
use crate::plan::{Select, SelectBuilder};
use crate::planner::sql_evaluator::ReferencesBuilder;
use cubenativeutils::CubeError;
use std::collections::HashMap;
use std::rc::Rc;

pub struct MeasureSubqueryProcessor<'a> {
    builder: &'a PhysicalPlanBuilder,
}

impl<'a> LogicalNodeProcessor<'a, MeasureSubquery> for MeasureSubqueryProcessor<'a> {
    type PhysycalNode = Rc<Select>;
    fn new(builder: &'a PhysicalPlanBuilder) -> Self {
        Self { builder }
    }

    fn process(
        &self,
        measure_subquery: &MeasureSubquery,
        context: &PushDownBuilderContext,
    ) -> Result<Self::PhysycalNode, CubeError> {
        let query_tools = self.builder.query_tools();
        let mut render_references = HashMap::new();
        let from = self
            .builder
            .process_node(measure_subquery.source.as_ref(), context)?;

        let mut context_factory = context.make_sql_nodes_factory()?;
        let references_builder = ReferencesBuilder::new(from.clone());
        let mut select_builder = SelectBuilder::new(from);

        context_factory.set_rendered_as_multiplied_measures(
            measure_subquery
                .schema
                .measures
                .iter()
                .map(|m| m.full_name())
                .collect(),
        );
        self.builder.resolve_subquery_dimensions_references(
            &measure_subquery.source.dimension_subqueries,
            &references_builder,
            &mut render_references,
        )?;
        for dim in measure_subquery.schema.dimensions.iter() {
            select_builder.add_projection_member(dim, None);
        }
        for meas in measure_subquery.schema.measures.iter() {
            select_builder.add_projection_member(meas, None);
        }

        context_factory.set_ungrouped_measure(true);
        context_factory.set_render_references(render_references);

        let select = Rc::new(select_builder.build(query_tools.clone(), context_factory));
        Ok(select)
    }
}

impl ProcessableNode for MeasureSubquery {
    type ProcessorType<'a> = MeasureSubqueryProcessor<'a>;
}
