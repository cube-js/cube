use super::super::context::PushDownBuilderContext;
use super::super::{LogicalNodeProcessor, ProcessableNode};
use crate::logical_plan::{MultiStageRollingWindow, MultiStageRollingWindowType};
use crate::physical_plan_builder::PhysicalPlanBuilder;
use crate::plan::{
    Expr, From, JoinBuilder, JoinCondition, MemberExpression, QualifiedColumnName, QueryPlan,
    SelectBuilder,
};
use crate::planner::sql_evaluator::ReferencesBuilder;
use cubenativeutils::CubeError;
use std::collections::HashMap;
use std::rc::Rc;

pub struct MultiStageRollingWindowProcessor<'a> {
    builder: &'a PhysicalPlanBuilder,
}

impl<'a> LogicalNodeProcessor<'a, MultiStageRollingWindow>
    for MultiStageRollingWindowProcessor<'a>
{
    type PhysycalNode = QueryPlan;
    fn new(builder: &'a PhysicalPlanBuilder) -> Self {
        Self { builder }
    }

    fn process(
        &self,
        rolling_window: &MultiStageRollingWindow,
        context: &PushDownBuilderContext,
    ) -> Result<Self::PhysycalNode, CubeError> {
        let query_tools = self.builder.query_tools();
        let time_dimension = rolling_window.rolling_time_dimension.clone();
        let time_series_ref = rolling_window.time_series_input.name.clone();
        let measure_input_ref = rolling_window.measure_input.name.clone();

        let time_series_schema = context.get_multi_stage_schema(&time_series_ref)?;

        let measure_input_schema = context.get_multi_stage_schema(&measure_input_ref)?;

        let base_time_dimension_alias = measure_input_schema
            .resolve_member_alias(&rolling_window.time_dimension_in_measure_input);

        let root_alias = format!("time_series");
        let measure_input_alias = format!("rolling_source");

        let mut join_builder = JoinBuilder::new_from_table_reference(
            time_series_ref.clone(),
            time_series_schema,
            Some(root_alias.clone()),
        );

        let on = match &rolling_window.rolling_window {
            MultiStageRollingWindowType::Regular(regular_rolling_window) => {
                JoinCondition::new_regular_rolling_join(
                    root_alias.clone(),
                    regular_rolling_window.trailing.clone(),
                    regular_rolling_window.leading.clone(),
                    regular_rolling_window.offset.clone(),
                    Expr::Reference(QualifiedColumnName::new(
                        Some(measure_input_alias.clone()),
                        base_time_dimension_alias,
                    )),
                )
            }
            MultiStageRollingWindowType::ToDate(to_date_rolling_window) => {
                JoinCondition::new_to_date_rolling_join(
                    root_alias.clone(),
                    to_date_rolling_window.granularity_obj.clone(),
                    Expr::Reference(QualifiedColumnName::new(
                        Some(measure_input_alias.clone()),
                        base_time_dimension_alias,
                    )),
                    query_tools.clone(),
                )
            }
            MultiStageRollingWindowType::RunningTotal => JoinCondition::new_rolling_total_join(
                root_alias.clone(),
                Expr::Reference(QualifiedColumnName::new(
                    Some(measure_input_alias.clone()),
                    base_time_dimension_alias,
                )),
            ),
        };

        join_builder.left_join_table_reference(
            measure_input_ref.clone(),
            measure_input_schema.clone(),
            Some(measure_input_alias.clone()),
            on,
        );

        let mut context_factory = context.make_sql_nodes_factory()?;
        context_factory.set_rolling_window(true);
        let from = From::new_from_join(join_builder.build());
        let references_builder = ReferencesBuilder::new(from.clone());
        let mut render_references = HashMap::new();
        let mut select_builder = SelectBuilder::new(from.clone());

        //We insert render reference for main time dimension (with some granularity as in time series to avoid unnecessary date_tranc)
        render_references.insert(
            time_dimension.full_name(),
            QualifiedColumnName::new(Some(root_alias.clone()), format!("date_from")),
        );

        //We also insert render reference for the base dimension of the time dimension (i.e. without `_granularity` prefix to let other time dimensions make date_tranc)
        render_references.insert(
            time_dimension
                .as_time_dimension()?
                .base_symbol()
                .full_name(),
            QualifiedColumnName::new(Some(root_alias.clone()), format!("date_from")),
        );

        for dim in rolling_window.schema.time_dimensions.iter() {
            context_factory.add_dimensions_with_ignored_timezone(dim.full_name());
            let alias = references_builder
                .resolve_alias_for_member(&dim.full_name(), &Some(measure_input_alias.clone()));
            select_builder.add_projection_member(dim, alias);
        }

        for dim in rolling_window.schema.dimensions.iter() {
            references_builder.resolve_references_for_member(
                dim.clone(),
                &Some(measure_input_alias.clone()),
                &mut render_references,
            )?;
            let alias = references_builder
                .resolve_alias_for_member(&dim.full_name(), &Some(measure_input_alias.clone()));
            select_builder.add_projection_member(dim, alias);
        }

        for measure in rolling_window.schema.measures.iter() {
            let name_in_base_query = measure_input_schema.resolve_member_alias(measure);
            context_factory.add_ungrouped_measure_reference(
                measure.full_name(),
                QualifiedColumnName::new(Some(measure_input_alias.clone()), name_in_base_query),
            );

            select_builder.add_projection_member(&measure, None);
        }

        if !rolling_window.is_ungrouped {
            let group_by = rolling_window
                .schema
                .all_dimensions()
                .map(|dim| -> Result<_, CubeError> {
                    Ok(Expr::Member(MemberExpression::new(dim.clone())))
                })
                .collect::<Result<Vec<_>, _>>()?;
            select_builder.set_group_by(group_by);
            select_builder.set_order_by(
                self.builder
                    .make_order_by(&rolling_window.schema, &rolling_window.order_by)?,
            );
        } else {
            context_factory.set_ungrouped(true);
        }

        context_factory.set_render_references(render_references);
        let select = Rc::new(select_builder.build(query_tools.clone(), context_factory));
        Ok(QueryPlan::Select(select))
    }
}

impl ProcessableNode for MultiStageRollingWindow {
    type ProcessorType<'a> = MultiStageRollingWindowProcessor<'a>;
}
