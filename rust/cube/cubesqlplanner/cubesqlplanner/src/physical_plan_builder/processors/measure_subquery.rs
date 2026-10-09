use super::super::PushDownBuilderContext;
use crate::logical_plan::MeasureSubquery;
use crate::physical_plan::ReferencesBuilder;
use crate::physical_plan::{Select, SelectBuilder};
use crate::physical_plan_builder::PhysicalPlanBuilder;
use crate::planner::filter::Filter;
use crate::planner::symbols::transforms;
use crate::planner::MeasureRenderModifier;
use cubenativeutils::CubeError;
use std::rc::Rc;

/// Deliberately not a `ProcessableNode`: a measure subquery is only meaningful
/// inside the aggregate that owns it, which is the only thing that knows the
/// filters its sources must resolve their bindings against. Going through the
/// generic `process_node` would lose them silently, so there is no way in.
pub struct MeasureSubqueryProcessor<'a> {
    builder: &'a PhysicalPlanBuilder,
}

impl<'a> MeasureSubqueryProcessor<'a> {
    pub fn new(builder: &'a PhysicalPlanBuilder) -> Self {
        Self { builder }
    }

    /// `filter_params_filters` are the enclosing keys subquery's filters. This
    /// select applies no WHERE of its own - the keys subquery already
    /// restricts the rows - but its sources' `FILTER_PARAMS` and `FILTER_GROUP`
    /// bindings resolve against them, so both copies of the fact source render
    /// the same predicate.
    pub fn process(
        &self,
        measure_subquery: &MeasureSubquery,
        context: &PushDownBuilderContext,
        filter_params_filters: Option<Filter>,
    ) -> Result<Rc<Select>, CubeError> {
        let query_tools = self.builder.query_tools();
        let from = self
            .builder
            .process_node(measure_subquery.source.as_ref(), context)?;

        let mut context_factory = context.make_sql_nodes_factory()?;
        let references_builder = ReferencesBuilder::new(from.clone());
        let mut select_builder = SelectBuilder::new(from);

        self.builder.resolve_subquery_dimensions_references(
            &measure_subquery.source.dimension_subqueries(),
            &references_builder,
            &mut context_factory,
        )?;
        for dim in measure_subquery.schema.dimensions.iter() {
            select_builder.add_projection_member(dim, None);
        }
        // The subquery emits raw row-level measure values; the enclosing
        // aggregate select applies the actual aggregation.
        for meas in measure_subquery.schema.measures.iter() {
            let meas =
                transforms::measures_render_modifier(meas, &MeasureRenderModifier::RawValue)?;
            select_builder.add_projection_member(&meas, None);
        }

        select_builder.set_filter_params_filters(filter_params_filters);

        let select = Rc::new(select_builder.build(query_tools.clone(), context_factory));
        Ok(select)
    }
}
