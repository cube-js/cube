use super::super::context::PushDownBuilderContext;
use super::super::{LogicalNodeProcessor, ProcessableNode};
use crate::logical_plan::MultiStageGetDateRange;
use crate::physical_plan_builder::PhysicalPlanBuilder;
use crate::plan::{QueryPlan, SelectBuilder};
use crate::planner::sql_evaluator::ReferencesBuilder;
use cubenativeutils::CubeError;
use std::collections::HashMap;
use std::rc::Rc;

pub struct MultiStageGetDateRangeProcessor<'a> {
    builder: &'a PhysicalPlanBuilder,
}

impl<'a> LogicalNodeProcessor<'a, MultiStageGetDateRange> for MultiStageGetDateRangeProcessor<'a> {
    type PhysycalNode = QueryPlan;
    fn new(builder: &'a PhysicalPlanBuilder) -> Self {
        Self { builder }
    }

    fn process(
        &self,
        get_date_range: &MultiStageGetDateRange,
        context: &PushDownBuilderContext,
    ) -> Result<Self::PhysycalNode, CubeError> {
        let query_tools = self.builder.query_tools();
        let mut render_references = HashMap::new();
        let from = self
            .builder
            .process_node(get_date_range.source.as_ref(), context)?;
        let references_builder = ReferencesBuilder::new(from.clone());
        let mut select_builder = SelectBuilder::new(from);
        let mut context_factory = context.make_sql_nodes_factory()?;
        let args = vec![get_date_range.time_dimension.clone()];
        select_builder.add_projection_function_expression(
            "MAX",
            args.clone(),
            "max_date".to_string(),
        );

        select_builder.add_projection_function_expression(
            "MIN",
            args.clone(),
            "min_date".to_string(),
        );

        self.builder.resolve_subquery_dimensions_references(
            &get_date_range.source.dimension_subqueries,
            &references_builder,
            &mut render_references,
        )?;
        context_factory.set_render_references(render_references);
        let select = Rc::new(select_builder.build(query_tools.clone(), context_factory));
        Ok(QueryPlan::Select(select))
    }
}

impl ProcessableNode for MultiStageGetDateRange {
    type ProcessorType<'a> = MultiStageGetDateRangeProcessor<'a>;
}
