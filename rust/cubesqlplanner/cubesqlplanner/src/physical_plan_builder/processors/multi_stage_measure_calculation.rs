use super::super::context::PushDownBuilderContext;
use super::super::{LogicalNodeProcessor, ProcessableNode};
use crate::logical_plan::{MultiStageCalculationWindowFunction, MultiStageMeasureCalculation};
use crate::physical_plan_builder::PhysicalPlanBuilder;
use crate::plan::{Expr, MemberExpression, QueryPlan, SelectBuilder};
use crate::planner::sql_evaluator::ReferencesBuilder;
use cubenativeutils::CubeError;
use itertools::Itertools;
use std::collections::HashMap;
use std::rc::Rc;

pub struct MultiStageMeasureCalculationProcessor<'a> {
    builder: &'a PhysicalPlanBuilder,
}

impl<'a> LogicalNodeProcessor<'a, MultiStageMeasureCalculation>
    for MultiStageMeasureCalculationProcessor<'a>
{
    type PhysycalNode = QueryPlan;
    fn new(builder: &'a PhysicalPlanBuilder) -> Self {
        Self { builder }
    }

    fn process(
        &self,
        measure_calculation: &MultiStageMeasureCalculation,
        context: &PushDownBuilderContext,
    ) -> Result<Self::PhysycalNode, CubeError> {
        let query_tools = self.builder.query_tools();
        let from = self
            .builder
            .process_node(measure_calculation.source.as_ref(), context)?;
        let references_builder = ReferencesBuilder::new(from.clone());
        let mut render_references = HashMap::new();

        let mut select_builder = SelectBuilder::new(from.clone());
        let all_dimensions = measure_calculation
            .schema
            .all_dimensions()
            .cloned()
            .collect_vec();

        for member in measure_calculation.schema.all_dimensions() {
            references_builder.resolve_references_for_member(
                member.clone(),
                &None,
                &mut render_references,
            )?;
            select_builder.add_projection_member(&member, None);
        }

        for measure in measure_calculation.schema.measures.iter() {
            references_builder.resolve_references_for_member(
                measure.clone(),
                &None,
                &mut render_references,
            )?;
            let alias = references_builder.resolve_alias_for_member(&measure.full_name(), &None);
            select_builder.add_projection_member(measure, alias);
        }

        if !measure_calculation.is_ungrouped {
            let group_by = all_dimensions
                .iter()
                .map(|dim| -> Result<_, CubeError> {
                    Ok(Expr::Member(MemberExpression::new(dim.clone())))
                })
                .collect::<Result<Vec<_>, _>>()?;
            select_builder.set_group_by(group_by);
            select_builder.set_order_by(
                self.builder
                    .make_order_by(&measure_calculation.schema, &measure_calculation.order_by)?,
            );
        }

        let mut context_factory = context.make_sql_nodes_factory()?;
        let partition_by = measure_calculation
            .partition_by
            .iter()
            .map(|dim| -> Result<_, CubeError> {
                if let Some(reference) =
                    references_builder.find_reference_for_member(&dim.full_name(), &None)
                {
                    Ok(format!("{}", reference))
                } else {
                    Err(CubeError::internal(format!(
                        "Alias not found for partition_by dimension {}",
                        dim.full_name()
                    )))
                }
            })
            .collect::<Result<Vec<_>, _>>()?;
        match &measure_calculation.window_function_to_use {
            MultiStageCalculationWindowFunction::Rank => {
                context_factory.set_multi_stage_rank(partition_by)
            }
            MultiStageCalculationWindowFunction::Window => {
                context_factory.set_multi_stage_window(partition_by)
            }
            MultiStageCalculationWindowFunction::None => {}
        }
        context_factory.set_render_references(render_references);
        let select = Rc::new(select_builder.build(query_tools.clone(), context_factory));
        Ok(QueryPlan::Select(select))
    }
}

impl ProcessableNode for MultiStageMeasureCalculation {
    type ProcessorType<'a> = MultiStageMeasureCalculationProcessor<'a>;
}
