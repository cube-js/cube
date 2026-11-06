use super::super::context::PushDownBuilderContext;
use super::super::{LogicalNodeProcessor, ProcessableNode};
use crate::logical_plan::transforms as logical_transforms;
use crate::logical_plan::{MultiStageCalculationWindowFunction, MultiStageMeasureCalculation};
use crate::physical_plan::{Expr, MemberExpression, QueryPlan, SelectBuilder};
use crate::physical_plan::{ReferenceSubstitutions, ReferencesBuilder};
use crate::physical_plan_builder::PhysicalPlanBuilder;
use crate::planner::symbols::transforms;
use crate::planner::MeasureRenderModifier;
use cubenativeutils::CubeError;
use itertools::Itertools;
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
        for member in measure_calculation.schema().all_dimensions() {
            references_builder.collect_substitutions_for_member(
                member.clone(),
                &None,
                &mut substitutions,
            )?;
        }

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
        }
        let partition = measure_calculation
            .partition_by()
            .iter()
            .map(|dim| transforms::substitute_by_name(dim, &substitutions))
            .collect::<Result<Vec<_>, _>>()?;
        let measure_modifier = match measure_calculation.window_function_to_use() {
            MultiStageCalculationWindowFunction::Rank => {
                Some(MeasureRenderModifier::MultiStageRank { partition })
            }
            MultiStageCalculationWindowFunction::Window => {
                Some(MeasureRenderModifier::MultiStageWindow { partition })
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

        for measure in schema.measures.iter() {
            references_builder.collect_substitutions_for_member(
                measure.clone(),
                &None,
                &mut substitutions,
            )?;
        }

        let measure_aliases = schema
            .measures
            .iter()
            .map(|measure| references_builder.resolve_alias_for_member(measure, &None))
            .collect_vec();
        let schema = logical_transforms::substitute_symbols_in_schema(&schema, &substitutions)?;
        let all_dimensions = schema.all_dimensions().cloned().collect_vec();

        let mut select_builder =
            SelectBuilder::new(from.substitute_join_conditions(&substitutions)?);
        for member in all_dimensions.iter() {
            select_builder.add_projection_member(member, None);
        }
        for (measure, alias) in schema.measures.iter().zip(measure_aliases) {
            select_builder.add_projection_member(measure, alias);
        }

        if !measure_calculation.is_ungrouped() {
            let group_by = all_dimensions
                .iter()
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
