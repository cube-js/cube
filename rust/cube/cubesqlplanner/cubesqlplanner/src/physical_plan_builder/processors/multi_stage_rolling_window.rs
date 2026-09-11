use super::super::context::PushDownBuilderContext;
use super::super::{LogicalNodeProcessor, ProcessableNode};
use crate::logical_plan::transforms as logical_transforms;
use crate::logical_plan::{MultiStageRollingWindow, MultiStageRollingWindowType};
use crate::physical_plan::symbols::column_ref_symbol::column_reference;
use crate::physical_plan::{
    Expr, From, JoinBuilder, JoinCondition, MemberExpression, QualifiedColumnName, QueryPlan,
    ReferenceSubstitutions, ReferencesBuilder, SelectBuilder,
};
use crate::physical_plan_builder::PhysicalPlanBuilder;
use crate::planner::symbols::transforms;
use crate::planner::{MeasureRenderModifier, MemberSymbol};
use cubenativeutils::CubeError;
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
        let time_series_ref = rolling_window.time_series_input.name().clone();
        let measure_input_ref = rolling_window.measure_input.name().clone();

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
        };

        join_builder.left_join_table_reference(
            measure_input_ref.clone(),
            measure_input_schema.clone(),
            Some(measure_input_alias.clone()),
            on,
        );

        let context_factory = context.make_sql_nodes_factory()?;
        let from = From::new_from_join(join_builder.build());
        let references_builder = ReferencesBuilder::new(from.clone());

        let mut substitutions = ReferenceSubstitutions::new();
        let date_from = || QualifiedColumnName::new(Some(root_alias.clone()), format!("date_from"));
        //The main time dimension is read from the time series axis at the granularity the series was built with, so it needs no date_trunc of its own
        substitutions.insert(
            time_dimension.full_name(),
            column_reference(&time_dimension, date_from()),
        );

        //The base dimension of that time dimension is read from the same axis, so a time dimension at another granularity truncates the axis column
        let base_time_dimension = time_dimension.as_time_dimension()?.base_symbol().clone();
        substitutions.insert(
            base_time_dimension.full_name(),
            column_reference(&base_time_dimension, date_from()),
        );

        // Time dimensions are read from the rolling source input, where they
        // are already timezone-converted and truncated, so they must render
        // without the conversion.
        let schema =
            logical_transforms::mark_tz_converted_at_source_in_schema(&rolling_window.schema)?;
        // An ungrouped rolling select emits row-level values: count-like
        // measures render a not-null indicator over the input column;
        // otherwise the select merges the window's partial values.
        let schema = if rolling_window.is_ungrouped {
            logical_transforms::measures_render_modifier_in_schema(
                &schema,
                &MeasureRenderModifier::UngroupedFinal,
            )?
        } else {
            logical_transforms::measures_render_modifier_in_schema(
                &schema,
                &MeasureRenderModifier::RollingMerge,
            )?
        };

        for dim in schema.dimensions.iter() {
            if dim.clone().resolve_reference_chain()
                != time_dimension.clone().resolve_reference_chain()
            {
                references_builder.collect_substitutions_for_member(
                    dim.clone(),
                    &Some(measure_input_alias.clone()),
                    &mut substitutions,
                )?;
            }
        }

        // A measure of a rolling select aggregates the row-level values the
        // rolling source produced for it, so it reads its input from that
        // source instead of computing it.
        for measure in schema.measures.iter() {
            // Only a measure reads its input through an aggregation; anything
            // else in this list renders its own SQL, as it did before.
            let Ok(measure_symbol) = measure.as_measure() else {
                continue;
            };
            let name_in_base_query = measure_input_schema.resolve_member_alias(measure);
            let input = column_reference(
                measure,
                QualifiedColumnName::new(Some(measure_input_alias.clone()), name_in_base_query),
            );
            let over_input = transforms::measure_over_reference(&measure_symbol, input);
            substitutions.insert(measure.full_name(), MemberSymbol::new_measure(over_input));
        }

        let schema = logical_transforms::substitute_symbols_in_schema(&schema, &substitutions)?;

        let mut select_builder = SelectBuilder::new(from.clone());

        for dim in schema.time_dimensions.iter() {
            let alias = references_builder
                .resolve_alias_for_member(&dim, &Some(measure_input_alias.clone()));
            select_builder.add_projection_member(dim, alias);
        }

        for dim in schema.dimensions.iter() {
            let alias = references_builder
                .resolve_alias_for_member(&dim, &Some(measure_input_alias.clone()));
            select_builder.add_projection_member(dim, alias);
        }

        for measure in schema.measures.iter() {
            select_builder.add_projection_member(&measure, None);
        }

        if !rolling_window.is_ungrouped {
            let group_by = schema
                .all_dimensions()
                .map(|dim| -> Result<_, CubeError> {
                    Ok(Expr::Member(MemberExpression::new(dim.clone())))
                })
                .collect::<Result<Vec<_>, _>>()?;
            select_builder.set_group_by(group_by);
            select_builder.set_order_by(self.builder.make_order_by(
                &schema,
                &rolling_window.order_by,
                &substitutions,
            )?);
        }

        let select = Rc::new(select_builder.build(query_tools.clone(), context_factory));
        Ok(QueryPlan::Select(select))
    }
}

impl ProcessableNode for MultiStageRollingWindow {
    type ProcessorType<'a> = MultiStageRollingWindowProcessor<'a>;
}
