use super::super::{LogicalNodeProcessor, ProcessableNode, PushDownBuilderContext};
use crate::logical_plan::transforms as logical_transforms;
use crate::logical_plan::MeasureSubquery;
use crate::physical_plan::{ReferenceSubstitutions, ReferencesBuilder, Select, SelectBuilder};
use crate::physical_plan_builder::PhysicalPlanBuilder;
use crate::planner::symbols::transforms;
use crate::planner::MeasureRenderModifier;
use cubenativeutils::CubeError;
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
        let from = self
            .builder
            .process_node(measure_subquery.source.as_ref(), context)?;

        let mut context_factory = context.make_sql_nodes_factory()?;
        let references_builder = ReferencesBuilder::new(from.clone());

        let mut substitutions = ReferenceSubstitutions::new();
        self.builder.collect_subquery_dimensions_substitutions(
            &measure_subquery.source.dimension_subqueries(),
            &references_builder,
            &mut substitutions,
            &mut context_factory,
        )?;
        let schema = logical_transforms::substitute_symbols_in_schema(
            &measure_subquery.schema,
            &substitutions,
        )?;

        let mut select_builder = SelectBuilder::new(from);
        for dim in schema.dimensions.iter() {
            select_builder.add_projection_member(dim, None);
        }
        // The subquery emits raw row-level measure values; the enclosing
        // aggregate select applies the actual aggregation.
        for meas in schema.measures.iter() {
            let meas =
                transforms::measures_render_modifier(meas, &MeasureRenderModifier::RawValue)?;
            select_builder.add_projection_member(&meas, None);
        }

        let select = Rc::new(select_builder.build(query_tools.clone(), context_factory));
        Ok(select)
    }
}

impl ProcessableNode for MeasureSubquery {
    type ProcessorType<'a> = MeasureSubqueryProcessor<'a>;
}
