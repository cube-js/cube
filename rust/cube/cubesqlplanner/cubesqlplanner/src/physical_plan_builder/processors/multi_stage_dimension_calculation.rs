use super::super::context::PushDownBuilderContext;
use super::super::{LogicalNodeProcessor, ProcessableNode};
use crate::logical_plan::MultiStageDimensionCalculation;
use crate::physical_plan::{Expr, MemberExpression, QueryPlan, SelectBuilder};
use crate::physical_plan_builder::PhysicalPlanBuilder;
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
        let context_factory = context.make_sql_nodes_factory()?;
        let from = self
            .builder
            .process_node(measure_calculation.source().as_ref(), context)?;
        let schema = measure_calculation.schema();
        let input_sources = from.input_sources();
        let measure_aliases = schema
            .measures
            .iter()
            .map(|measure| input_sources.column_name(measure))
            .collect::<Vec<_>>();

        let mut select_builder = SelectBuilder::new(from);
        for member in schema.all_dimensions() {
            select_builder.add_projection_member(member, None);
        }
        for (measure, alias) in schema.measures.iter().zip(measure_aliases) {
            select_builder.add_projection_member(measure, alias);
        }

        let group_by = schema
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
