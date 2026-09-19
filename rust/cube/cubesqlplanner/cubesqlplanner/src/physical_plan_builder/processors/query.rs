use super::super::{LogicalNodeProcessor, ProcessableNode, PushDownBuilderContext};
use crate::logical_plan::transforms as logical_transforms;
use crate::logical_plan::{all_symbols, Query, QuerySource};
use crate::physical_plan::symbols::column_ref_symbol::literal_reference;
use crate::physical_plan::{
    CalcGroupItem, CalcGroupsJoin, Expr, From, MemberExpression, ReferenceSubstitutions,
    ReferencesBuilder, Select, SelectBuilder,
};
use crate::physical_plan_builder::PhysicalPlanBuilder;
use crate::planner::collectors::collect_calc_group_dims_from_nodes;
use crate::planner::symbols::transforms;
use crate::planner::symbols::transforms::get_filtered_values;
use crate::planner::{MeasureRenderModifier, MemberSymbol, OrderByItem};
use cubenativeutils::CubeError;
use itertools::Itertools;
use std::collections::HashSet;
use std::rc::Rc;

pub struct QueryProcessor<'a> {
    builder: &'a PhysicalPlanBuilder,
}

impl QueryProcessor<'_> {
    fn is_over_full_aggregated_source(&self, logical_plan: &Query) -> bool {
        match logical_plan.source() {
            QuerySource::FullKeyAggregate(fk) => !fk.is_empty(),
            QuerySource::PreAggregation(_) => false,
            QuerySource::LogicalJoin(_) => false,
        }
    }
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
        let mut context_factory = context.make_sql_nodes_factory()?;
        let mut context = context.clone();

        context.remove_multi_stage_dimensions();

        //FIXME This is hack but good solution require refactor
        let resolved_multistage_dimension =
            if let QuerySource::FullKeyAggregate(fk_source) = logical_plan.source() {
                if let Some(first_cte_ref) = fk_source.multi_stage_subquery_refs().first() {
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

        let from = self.builder.process_node(logical_plan.source(), &context)?;
        let mut filter = logical_plan.filter().all_filters();
        let mut having = logical_plan.filter().measures_filter();

        // Calc-group dimensions are resolved at query time: a value pinned by
        // a filter renders as a literal, otherwise the enumeration is
        // cross-joined as a virtual values table. Over a pre-aggregation this
        // applies only to calc groups NOT stored in the rollup — stored ones
        // keep resolving to the rollup column for backward compatibility.
        let calc_group_stored_dims = match logical_plan.source() {
            QuerySource::LogicalJoin(_) => Some(HashSet::new()),
            QuerySource::PreAggregation(pre_aggregation) => Some(
                pre_aggregation
                    .all_dimensions_refererences()
                    .into_keys()
                    .collect::<HashSet<_>>(),
            ),
            QuerySource::FullKeyAggregate(_) => None,
        };
        // Values pinned by the query rather than read from a source. Kept apart
        // because a select over a pre-aggregation reads every other member from
        // the rollup columns, but still renders these as literals.
        let mut calc_group_literals = ReferenceSubstitutions::new();
        let from = if let Some(stored_dims) = calc_group_stored_dims {
            let all_symbols = all_symbols(&logical_plan.schema(), &logical_plan.filter());
            let calc_group_dims = collect_calc_group_dims_from_nodes(all_symbols.iter())?
                .into_iter()
                .filter(|dim| !stored_dims.contains(&dim.full_name()))
                .collect_vec();

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
                calc_group_literals.insert(
                    item.symbol.full_name(),
                    literal_reference(&item.symbol, item.values[0].clone()),
                );
                // A join condition can name a calc-group dimension too, and it
                // is built before there is a select symbol environment to
                // rewrite, so it resolves the value while rendering.
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

        let mut schema = logical_plan.schema().clone();
        let references_builder = ReferencesBuilder::new(from.clone());
        let mut substitutions = calc_group_literals;

        match logical_plan.source() {
            QuerySource::LogicalJoin(join) => {
                self.builder.collect_subquery_dimensions_substitutions(
                    &join.dimension_subqueries(),
                    &references_builder,
                    &mut substitutions,
                    &mut context_factory,
                )?;
            }
            QuerySource::PreAggregation(pre_aggregation) => {
                // A rollup stores time dimensions already timezone-converted,
                // so every occurrence of them in this select must render
                // without the conversion.
                let time_dimension_names = schema
                    .time_dimensions
                    .iter()
                    .map(|d| d.full_name())
                    .collect::<HashSet<_>>();
                let mark_tz_converted =
                    |symbol: &Rc<MemberSymbol>| -> Result<Rc<MemberSymbol>, CubeError> {
                        transforms::mark_tz_converted_at_source(symbol, &time_dimension_names)
                    };
                schema = logical_transforms::mark_tz_converted_at_source_in_schema(&schema)?;
                filter = transforms::map_filter_symbols(filter, &mark_tz_converted)?;
                having = transforms::map_filter_symbols(having, &mark_tz_converted)?;
                context_factory.set_use_local_tz_in_date_range(true);

                for (name, column) in pre_aggregation.all_dimensions_refererences().into_iter() {
                    context_factory.add_pre_aggregation_dimension_reference(name, column);
                }
                for (name, column) in pre_aggregation.all_measures_refererences().into_iter() {
                    context_factory.add_pre_aggregation_measure_reference(name, column);
                }
            }
            QuerySource::FullKeyAggregate(_) => {}
        }

        // An ungrouped select emits row-level measure values: raw ones
        // under a measure-rendering context (multi-stage leaves), a
        // not-null indicator form for count-likes otherwise.
        let measure_modifier = if context.render_measure_for_ungrouped {
            Some(MeasureRenderModifier::RawValue)
        } else if logical_plan.modifers().ungrouped {
            Some(MeasureRenderModifier::UngroupedFinal)
        } else {
            None
        };
        if let Some(modifier) = &measure_modifier {
            let stamp = |symbol: &Rc<MemberSymbol>| -> Result<Rc<MemberSymbol>, CubeError> {
                transforms::measures_render_modifier(symbol, modifier)
            };
            schema = logical_transforms::measures_render_modifier_in_schema(&schema, modifier)?;
            filter = transforms::map_filter_symbols(filter, &stamp)?;
            having = transforms::map_filter_symbols(having, &stamp)?;
        }

        let is_pre_aggregation = matches!(logical_plan.source(), QuerySource::PreAggregation(_));

        if !logical_plan.modifers().ungrouped {
            context_factory.set_group_by_members(
                schema
                    .all_dimensions()
                    .map(|symbol| symbol.full_name())
                    .collect(),
            );
        }

        let measures_for_query = self.builder.measures_for_query(&schema.measures, &context);
        let over_full_aggregated_source = self.is_over_full_aggregated_source(logical_plan);

        // A select over a pre-aggregation reads its members from the rollup
        // columns, which the pre-aggregation node already resolved, so nothing
        // is collected for it: the values pinned by the query, already in
        // `substitutions`, are all it substitutes.
        if !is_pre_aggregation {
            for dimension in schema.all_dimensions() {
                self.builder.collect_query_dimension_substitution(
                    dimension,
                    &references_builder,
                    &from,
                    &mut substitutions,
                )?;
            }

            for (measure, exists) in measures_for_query.iter() {
                if *exists {
                    references_builder.collect_substitutions_for_member(
                        measure.clone(),
                        &None,
                        &mut substitutions,
                    )?;
                }
            }

            if over_full_aggregated_source {
                references_builder.collect_substitutions_for_filter(&having, &mut substitutions)?;
            }
        }

        let schema = logical_transforms::substitute_symbols_in_schema(&schema, &substitutions)?;
        let filter = logical_transforms::substitute_symbols_in_filter(filter, &substitutions)?;
        let having = logical_transforms::substitute_symbols_in_filter(having, &substitutions)?;

        let mut select_builder = SelectBuilder::new(from);

        for dimension in schema.all_dimensions() {
            self.builder
                .project_query_dimension(dimension, &mut select_builder, &context)?;
        }

        for (measure, exists) in measures_for_query.iter() {
            if *exists {
                let measure = transforms::substitute_by_name(measure, &substitutions)?;
                select_builder.add_projection_member(&measure, None);
            } else {
                select_builder.add_null_projection(&measure, None);
            }
        }

        if over_full_aggregated_source {
            select_builder.set_filter(having);
        } else {
            if !logical_plan.modifers().ungrouped {
                let group_by = schema
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

        // When reading from a pre-aggregation, drop ORDER BY keys on measures that
        // are not part of the selection. CubeStore cannot ORDER BY an aggregate of a
        // rollup column that isn't projected.
        let order_by = if is_pre_aggregation {
            logical_plan
                .modifers()
                .order_by
                .iter()
                .filter(|o| {
                    !(o.member_symbol().is_measure()
                        && schema.find_member_positions(&o.name()).is_empty())
                })
                .cloned()
                .collect()
        } else {
            logical_plan.modifers().order_by.clone()
        };
        // Items present in the schema are sorted by their stamped schema
        // symbol; only a measure absent from the projection carries its own
        // symbol into the ORDER BY and needs the form stamped here.
        let order_by = if let Some(modifier) = &measure_modifier {
            order_by
                .iter()
                .map(|o| -> Result<_, CubeError> {
                    if !schema.find_member_positions(&o.name()).is_empty() {
                        return Ok(o.clone());
                    }
                    Ok(OrderByItem::new(
                        transforms::measures_render_modifier(&o.member_symbol(), modifier)?,
                        o.desc(),
                    ))
                })
                .collect::<Result<Vec<_>, _>>()?
        } else {
            order_by
        };
        select_builder.set_order_by(self.builder.make_order_by(
            &schema,
            &order_by,
            &substitutions,
        )?);

        let res = Rc::new(select_builder.build(query_tools.clone(), context_factory));
        Ok(res)
    }
}

impl ProcessableNode for Query {
    type ProcessorType<'a> = QueryProcessor<'a>;
}
