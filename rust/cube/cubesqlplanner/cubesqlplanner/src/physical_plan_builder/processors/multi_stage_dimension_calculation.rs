use super::super::context::PushDownBuilderContext;
use super::super::{LogicalNodeProcessor, ProcessableNode};
use crate::logical_plan::transforms as logical_transforms;
use crate::logical_plan::MultiStageDimensionCalculation;
use crate::physical_plan::{
    Expr, MemberExpression, QueryPlan, ReferenceSubstitutions, ReferencesBuilder, SelectBuilder,
};
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
        let references_builder = ReferencesBuilder::new(from.clone());

        let mut substitutions = ReferenceSubstitutions::new();
        for member in measure_calculation.schema().all_members() {
            references_builder.collect_substitutions_for_member(
                member.clone(),
                &None,
                &mut substitutions,
            )?;
        }
        let schema = logical_transforms::substitute_symbols_in_schema(
            measure_calculation.schema(),
            &substitutions,
        )?;

        let mut select_builder = SelectBuilder::new(from.clone());

        for member in schema.all_dimensions() {
            select_builder.add_projection_member(&member, None);
        }

        for measure in schema.measures.iter() {
            let alias = references_builder.resolve_alias_for_member(&measure, &None);
            select_builder.add_projection_member(&measure, alias);
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
