use super::super::context::PushDownBuilderContext;
use super::super::{LogicalNodeProcessor, ProcessableNode};
use crate::logical_plan::transforms as logical_transforms;
use crate::logical_plan::{MultiStageCalculationWindowFunction, MultiStageMeasureCalculation};
use crate::physical_plan::{
    Expr, MemberExpression, QueryPlan, ReferenceSubstitutions, ReferencesBuilder, SelectBuilder,
};
use crate::physical_plan_builder::PhysicalPlanBuilder;
use crate::planner::symbols::transforms;
use crate::planner::MeasureRenderModifier;
use cubenativeutils::CubeError;
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

        // The partition of a window function is rendered by the measure that
        // carries it, so its dimensions are substituted into the form itself.
        let mut partition_by = Vec::new();
        for dim in measure_calculation.partition_by().iter() {
            references_builder.collect_substitutions_for_member(
                dim.clone(),
                &None,
                &mut substitutions,
            )?;
            if references_builder
                .find_reference_for_member(&dim, &None)
                .is_none()
            {
                return Err(CubeError::internal(format!(
                    "Alias not found for partition_by dimension {}",
                    dim.full_name()
                )));
            }
            partition_by.push(transforms::substitute_by_name(dim, &substitutions)?);
        }
        let measure_modifier = match measure_calculation.window_function_to_use() {
            MultiStageCalculationWindowFunction::Rank => {
                Some(MeasureRenderModifier::MultiStageRank {
                    partition: partition_by,
                })
            }
            MultiStageCalculationWindowFunction::Window => {
                Some(MeasureRenderModifier::MultiStageWindow {
                    partition: partition_by,
                })
            }
            MultiStageCalculationWindowFunction::None => None,
        };
        let schema = if let Some(modifier) = &measure_modifier {
            logical_transforms::measures_render_modifier_in_schema(
                measure_calculation.schema(),
                modifier,
            )?
        } else {
            measure_calculation.schema().clone()
        };
        // Substitution comes last: a member the source already produced is read
        // from it instead of being computed, whatever form was stamped on it.
        let schema = logical_transforms::substitute_symbols_in_schema(&schema, &substitutions)?;

        let mut select_builder = SelectBuilder::new(from.clone());

        for member in schema.all_dimensions() {
            select_builder.add_projection_member(&member, None);
        }

        for measure in schema.measures.iter() {
            let alias = references_builder.resolve_alias_for_member(&measure, &None);
            select_builder.add_projection_member(measure, alias);
        }

        if !measure_calculation.is_ungrouped() {
            let group_by = schema
                .all_dimensions()
                .map(|dim| -> Result<_, CubeError> {
                    Ok(Expr::Member(MemberExpression::new(dim.clone())))
                })
                .collect::<Result<Vec<_>, _>>()?;
            select_builder.set_group_by(group_by);
            select_builder.set_order_by(self.builder.make_order_by(
                &schema,
                measure_calculation.order_by(),
                &substitutions,
            )?);
        }

        let select = Rc::new(select_builder.build(query_tools.clone(), context_factory));
        Ok(QueryPlan::Select(select))
    }
}

impl ProcessableNode for MultiStageMeasureCalculation {
    type ProcessorType<'a> = MultiStageMeasureCalculationProcessor<'a>;
}
