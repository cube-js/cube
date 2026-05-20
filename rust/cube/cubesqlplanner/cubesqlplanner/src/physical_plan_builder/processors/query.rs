use super::super::{LogicalNodeProcessor, ProcessableNode, PushDownBuilderContext};
use crate::logical_plan::{all_symbols, FactKind, Query, QueryKind, QuerySource, StageKind};
use crate::physical_plan::{
    CalcGroupItem, CalcGroupsJoin, Expr, From, MemberExpression, QualifiedColumnName,
    ReferencesBuilder, Select, SelectBuilder,
};
use crate::physical_plan_builder::PhysicalPlanBuilder;
use crate::planner::collectors::collect_calc_group_dims_from_nodes;
use crate::planner::get_filtered_values;
use cubenativeutils::CubeError;
use itertools::Itertools;
use std::rc::Rc;

pub struct QueryProcessor<'a> {
    builder: &'a PhysicalPlanBuilder,
}

impl<'a> LogicalNodeProcessor<'a, Query> for QueryProcessor<'a> {
    type PhysycalNode = Rc<Select>;
    fn new(builder: &'a PhysicalPlanBuilder) -> Self {
        Self { builder }
    }

    fn process(
        &self,
        logical_plan: &Query,
        context: &PushDownBuilderContext,
    ) -> Result<Self::PhysycalNode, CubeError> {
        let query_tools = self.builder.query_tools();
        let modifiers = logical_plan.modifers();
        let mut context = context.clone();
        context.time_shifts = modifiers.time_shifts.clone();
        context.render_measure_as_state = modifiers.render_measure_as_state;
        context.render_measure_for_ungrouped = modifiers.render_measure_for_ungrouped;
        let mut context_factory = context.make_sql_nodes_factory()?;

        // CTE bodies (multi-stage measure stages, KS/MS bodies,
        // AggMS-Query bodies, multi-stage-dim ex-DSQ bodies) are owned by
        // the surrounding `LogicalPlan`. `PlanProcessor` renders them and
        // pre-registers their schemas on `context` before we see this
        // Query; here we just consume those references.
        context.remove_multi_stage_dimensions();

        //FIXME This is hack but good solution require refactor
        let resolved_multistage_dimension =
            if let QuerySource::FullKeyAggregate(fk_source) = logical_plan.source() {
                if let Some(first_cte_ref) = fk_source.data_inputs().first() {
                    first_cte_ref.schema().multi_stage_dimensions()?
                } else {
                    vec![]
                }
            } else {
                vec![]
            };
        for member in logical_plan.schema().multi_stage_dimensions()? {
            if resolved_multistage_dimension
                .iter()
                .all(|d| d.full_name() != member.full_name())
            {
                context.add_multi_stage_dimension(member.full_name());
            }
        }

        // Hand the MS-dim refs this Query consumes down to source
        // rendering. `LogicalJoinProcessor` wires `OnPrimaryKeys` LEFT
        // JOINs inside the cube chain; `OnOuterDimensions` is applied
        // by QueryProcessor below over the final FROM.
        context.multi_stage_dimension_refs = logical_plan.multi_stage_dimensions().clone();

        let from = self.builder.process_node(logical_plan.source(), &context)?;
        let filter = logical_plan.filter().all_filters();
        let having = logical_plan.filter().measures_filter();

        //TODO pre-aggregations support for calc-groups
        let from = if let QuerySource::LogicalJoin(_) = logical_plan.source() {
            let all_symbols = all_symbols(&logical_plan.schema(), &logical_plan.filter());
            let calc_group_dims = collect_calc_group_dims_from_nodes(all_symbols.iter())?;

            let calc_groups_items = calc_group_dims.into_iter().map(|dim| {
                let values = get_filtered_values(&dim, &filter);
                CalcGroupItem {
                    symbol: dim,
                    values,
                }
            });
            for item in calc_groups_items
                .clone()
                .filter(|itm| itm.values.len() == 1)
            {
                context_factory
                    .add_render_reference(item.symbol.full_name(), item.values[0].clone());
            }
            let calc_groups_to_join = calc_groups_items
                .filter(|itm| itm.values.len() > 1)
                .collect_vec();
            if calc_groups_to_join.is_empty() {
                from
            } else {
                let groups_join = CalcGroupsJoin::try_new(from, calc_groups_to_join)?;
                From::new_from_calc_groups_join(groups_join)
            }
        } else {
            from
        };

        match logical_plan.source() {
            QuerySource::LogicalJoin(_) => {
                let references_builder = ReferencesBuilder::new(from.clone());
                self.builder.resolve_multi_stage_dimension_references(
                    logical_plan.multi_stage_dimensions(),
                    &references_builder,
                    &mut context_factory,
                )?;
            }
            QuerySource::PreAggregation(pre_aggregation) => {
                for member in logical_plan.schema().time_dimensions.iter() {
                    context_factory.add_dimensions_with_ignored_timezone(member.full_name());
                }
                context_factory.set_use_local_tz_in_date_range(true);

                for (name, column) in pre_aggregation.all_dimensions_refererences().into_iter() {
                    context_factory.add_pre_aggregation_dimension_reference(name, column);
                }
                for (name, column) in pre_aggregation.all_measures_refererences().into_iter() {
                    context_factory.add_pre_aggregation_measure_reference(name, column);
                }
            }
            QuerySource::FullKeyAggregate(fk) => {
                // Data inputs flagged `is_ungrouped` carry raw measure
                // columns (no aggregate wrap yet); we must register
                // `ungrouped_measure_reference` per symbol so the final
                // `FinalMeasureSqlNode` still wraps in the aggregate (e.g.
                // SUM). Pushing these through `render_references` would
                // bypass the measure-processor chain and emit the column
                // raw, breaking GROUP BY. `KeysFullKeyAggregateStrategy`
                // joins each data input as `q_0`, `q_1`, ...
                for (i, data_input) in fk.data_inputs().iter().enumerate() {
                    if !data_input.is_ungrouped() {
                        continue;
                    }
                    let q_alias = format!("q_{}", i);
                    let cte_schema = context.get_cte_schema(data_input.name())?;
                    for symbol in data_input.symbols().iter() {
                        let column_alias = cte_schema.resolve_member_alias(symbol);
                        context_factory.add_ungrouped_measure_reference(
                            symbol.full_name(),
                            QualifiedColumnName::new(Some(q_alias.clone()), column_alias),
                        );
                    }
                }
            }
        }

        let is_pre_aggregation = matches!(logical_plan.source(), QuerySource::PreAggregation(_));

        let references_builder = ReferencesBuilder::new(from.clone());

        // Stage Calculation: resolve partition_by columns and route the
        // window function through the SQL nodes factory before any
        // projection is rendered.
        if let QueryKind::Stage(stage_kind) = logical_plan.kind() {
            match stage_kind {
                StageKind::Rank { partition_by } => {
                    let refs = self
                        .builder
                        .resolve_partition_refs(partition_by, &references_builder)?;
                    context_factory.set_multi_stage_rank(refs);
                }
                StageKind::Window { partition_by } => {
                    let refs = self
                        .builder
                        .resolve_partition_refs(partition_by, &references_builder)?;
                    context_factory.set_multi_stage_window(refs);
                }
                StageKind::Aggregation | StageKind::DimensionCalc { .. } => {}
            }
        }

        let mut select_builder = SelectBuilder::new(from);
        context_factory.set_ungrouped(logical_plan.modifers().ungrouped);
        let is_ungrouped_measure = matches!(
            logical_plan.kind(),
            QueryKind::InternalFact(FactKind::Measures)
        );

        // Stage Calculation projects each dimension directly off its single
        // FK-input alias — no COALESCE merging across join sides. Top-level
        // / leaf-wrapper Queries, by contrast, sit on top of the full-outer-
        // join of CTE refs and need `process_query_dimension`'s COALESCE
        // logic. MeasureSubquery-shape Queries project raw (no resolve).
        let is_stage_calculation = matches!(logical_plan.kind(), QueryKind::Stage(_));
        for dimension in logical_plan.schema().all_dimensions() {
            if is_ungrouped_measure {
                select_builder.add_projection_member(dimension, None);
            } else if is_stage_calculation {
                references_builder.resolve_references_for_member(
                    dimension.clone(),
                    &None,
                    context_factory.render_references_mut(),
                )?;
                select_builder.add_projection_member(dimension, None);
            } else {
                self.builder.process_query_dimension(
                    dimension,
                    &references_builder,
                    &mut select_builder,
                    &mut context_factory,
                    &context,
                )?;
            }
        }

        // When the source carries ungrouped data inputs we've already wired
        // measure substitutions through `ungrouped_measure_references`;
        // calling `resolve_references_for_member` here would short-circuit
        // the measure-processor chain and bypass the SUM wrap. The MS-shape
        // Query itself projects raw — the consumer applies the aggregate.
        let resolve_measure_refs = !is_ungrouped_measure
            && match logical_plan.source() {
                QuerySource::FullKeyAggregate(fk) => {
                    !fk.data_inputs().iter().any(|r| r.is_ungrouped())
                }
                _ => true,
            };
        for (measure, exists) in self
            .builder
            .measures_for_query(&logical_plan.schema().measures, &context)
        {
            if exists {
                // Resolve inner deps either for the whole measure (default)
                // or only for member-expressions when the source carries
                // ungrouped data inputs: their atomic measure column has
                // no `ungrouped_measure_reference`, but their inner dim/
                // measure refs need `render_reference` pointing at the
                // CTE columns the subquery projected (e.g. inner
                // `child.test_dim` → `q_0.child__test_dim`).
                let needs_resolve = resolve_measure_refs || measure.as_member_expression().is_ok();
                if needs_resolve {
                    references_builder.resolve_references_for_member(
                        measure.clone(),
                        &None,
                        context_factory.render_references_mut(),
                    )?;
                }
                select_builder.add_projection_member(&measure, None);
            } else {
                select_builder.add_null_projection(&measure, None);
            }
        }

        if matches!(logical_plan.kind(), QueryKind::TopLevelOverCtes { .. }) {
            references_builder
                .resolve_references_for_filter(&having, context_factory.render_references_mut())?;
            select_builder.set_filter(having);
        } else if !is_ungrouped_measure {
            if !logical_plan.modifers().ungrouped {
                let group_by = logical_plan
                    .schema()
                    .all_dimensions()
                    .map(|symbol| -> Result<_, CubeError> {
                        Ok(Expr::Member(MemberExpression::new(symbol.clone())))
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                select_builder.set_group_by(group_by);
            }
            select_builder.set_having(having);
            select_builder.set_filter(filter);
        }

        select_builder.set_limit(logical_plan.modifers().limit);
        select_builder.set_offset(logical_plan.modifers().offset);
        if matches!(logical_plan.kind(), QueryKind::InternalFact(FactKind::Keys)) {
            select_builder.set_distinct();
        }

        // MS-shape marks ALL its measures `rendered_as_multiplied` (the
        // consumer never sees the original aggregate); other shapes
        // propagate only the measures already flagged in schema.
        if is_ungrouped_measure {
            context_factory.set_rendered_as_multiplied_measures(
                logical_plan
                    .schema()
                    .measures
                    .iter()
                    .map(|m| m.full_name())
                    .collect(),
            );
            context_factory.set_ungrouped_measure(true);
        } else {
            context_factory.set_rendered_as_multiplied_measures(
                logical_plan.schema().multiplied_measures.clone(),
            );
        }

        if is_pre_aggregation {
            context_factory.clear_render_references();
        }
        if logical_plan.modifers().ungrouped {
            context_factory.set_ungrouped(true);
        }

        select_builder.set_order_by(
            self.builder
                .make_order_by(logical_plan.schema(), &logical_plan.modifers().order_by)?,
        );

        let res = Rc::new(select_builder.build(query_tools.clone(), context_factory));
        Ok(res)
    }
}

impl ProcessableNode for Query {
    type ProcessorType<'a> = QueryProcessor<'a>;
}
