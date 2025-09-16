use super::super::context::PushDownBuilderContext;
use super::super::{LogicalNodeProcessor, ProcessableNode};
use crate::logical_plan::MultiStageDimensionCalculation;
use crate::physical_plan_builder::PhysicalPlanBuilder;
use crate::plan::{Expr, MemberExpression, QueryPlan, SelectBuilder};
use crate::planner::sql_evaluator::ReferencesBuilder;
use cubenativeutils::CubeError;
use std::rc::Rc;

pub struct MultiStageDimensionCalculationProcessor<'a> {
    builder: &'a PhysicalPlanBuilder,
}

impl<'a> LogicalNodeProcessor<'a, MultiStageDimensionCalculation>
    for MultiStageDimensionCalculationProcessor<'a>
{
    type PhysycalNode = QueryPlan;
    fn new(builder: &'a PhysicalPlanBuilder) -> Self {
        Self { builder }
    }

    fn process(
        &self,
        measure_calculation: &MultiStageDimensionCalculation,
        context: &PushDownBuilderContext,
    ) -> Result<Self::PhysycalNode, CubeError> {
        let query_tools = self.builder.query_tools();
        let mut context_factory = context.make_sql_nodes_factory()?;
        let from = self
            .builder
            .process_node(measure_calculation.source().as_ref(), context)?;
        let references_builder = ReferencesBuilder::new(from.clone());

        let mut select_builder = SelectBuilder::new(from.clone());

        for member in measure_calculation.schema().all_dimensions() {
            references_builder.resolve_references_for_member(
                member.clone(),
                &None,
                context_factory.render_references_mut(),
            )?;
            select_builder.add_projection_member(&member, None);
        }

        for measure in measure_calculation.schema().measures.iter() {
            references_builder.resolve_references_for_member(
                measure.clone(),
                &None,
                context_factory.render_references_mut(),
            )?;
            let alias = references_builder.resolve_alias_for_member(&measure, &None);
            select_builder.add_projection_member(&measure, alias);
        }

        let group_by = measure_calculation
            .schema()
            .all_dimensions()
            .map(|symbol| -> Result<_, CubeError> {
                Ok(Expr::Member(MemberExpression::new(symbol.clone())))
            })
            .collect::<Result<Vec<_>, _>>()?;
        select_builder.set_group_by(group_by);

        let select = Rc::new(select_builder.build(query_tools.clone(), context_factory));
        Ok(QueryPlan::Select(select))
    }
}

impl ProcessableNode for MultiStageDimensionCalculation {
    type ProcessorType<'a> = MultiStageDimensionCalculationProcessor<'a>;
}
