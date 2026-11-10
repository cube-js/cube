use super::super::context::PushDownBuilderContext;
use super::super::{LogicalNodeProcessor, ProcessableNode};
use crate::logical_plan::transforms as logical_transforms;
use crate::logical_plan::{MultiStageCalculationWindowFunction, MultiStageMeasureCalculation};
use crate::physical_plan::ReferenceSubstitutions;
use crate::physical_plan::{Expr, MemberExpression, QueryPlan, SelectBuilder};
use crate::physical_plan_builder::PhysicalPlanBuilder;
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
            .process_node(measure_calculation.source(), context)?;

        let partition = measure_calculation.partition_by().clone();
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

        let input_sources = from.input_sources();
        let measure_aliases = schema
            .measures
            .iter()
            .map(|measure| input_sources.column_name(measure))
            .collect_vec();
        let all_dimensions = schema.all_dimensions().cloned().collect_vec();

        let mut select_builder = SelectBuilder::new(from);
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
                &ReferenceSubstitutions::new(),
            )?);
        }

        let select = Rc::new(select_builder.build(query_tools.clone(), context_factory));
        Ok(QueryPlan::Select(select))
    }
}

impl ProcessableNode for MultiStageMeasureCalculation {
    type ProcessorType<'a> = MultiStageMeasureCalculationProcessor<'a>;
}
