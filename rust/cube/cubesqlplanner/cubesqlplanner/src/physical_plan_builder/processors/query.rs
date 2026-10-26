use super::super::{LogicalNodeProcessor, ProcessableNode, PushDownBuilderContext};
use crate::logical_plan::transforms as logical_transforms;
use crate::logical_plan::{all_symbols, InputBase, Query};
use crate::physical_plan::{
    CalcGroupItem, CalcGroupsJoin, Expr, From, MemberExpression, ReferenceSubstitutions,
    ReferencesBuilder, Select, SelectBuilder,
};
use crate::physical_plan_builder::PhysicalPlanBuilder;
use crate::planner::collectors::{collect_calc_group_dims_from_nodes, has_multi_stage_members};
use crate::planner::filter::{Filter, FilterGroupOperator, FilterItem};
use crate::planner::query_properties::member_chain_eq;
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
        match logical_plan.source().base() {
            InputBase::Merge(fk) => !fk.is_empty(),
            InputBase::PreAggregation(_) => false,
            InputBase::QueryInputs(_) => false,
        }
    }

    fn multi_stage_dimension_filters(
        filter: Option<Filter>,
        selected_multi_stage_dimensions: &HashSet<String>,
    ) -> Result<Option<Filter>, CubeError> {
        let Some(filter) = filter else {
            return Ok(None);
        };
        let mut multi_stage_members = HashSet::new();
        for member in filter.all_member_evaluators() {
            if has_multi_stage_members(&member, true)? {
                multi_stage_members.insert(member.resolve_reference_chain().full_name());
            }
        }
        if multi_stage_members.is_empty() {
            return Ok(None);
        }
        // FIXME: a filter on a multi-stage dimension that is not selected has no
        // CTE column to resolve to here; it should be planned instead of rejected.
        if let Some(member) = multi_stage_members
            .iter()
            .sorted()
            .find(|name| !selected_multi_stage_dimensions.contains(*name))
        {
            return Err(CubeError::user(format!(
                "Filtering on multi-stage dimension {} requires it to be selected as a dimension",
                member
            )));
        }
        for item in filter.items.iter() {
            Self::check_no_mixed_or_groups(item, &multi_stage_members)?;
        }
        let target_members = multi_stage_members.iter().collect_vec();
        let items = filter
            .items
            .iter()
            .filter_map(|item| item.find_subtree_for_members(&target_members))
            .collect_vec();
        Ok((!items.is_empty()).then_some(Filter { items }))
    }

    fn check_no_mixed_or_groups(
        item: &FilterItem,
        multi_stage_members: &HashSet<String>,
    ) -> Result<(), CubeError> {
        let FilterItem::Group(group) = item else {
            return Ok(());
        };
        if group.operator == FilterGroupOperator::Or {
            let members = item.all_member_evaluators();
            let multi_stage_count = members
                .iter()
                .filter(|m| {
                    multi_stage_members
                        .contains(&(*m).clone().resolve_reference_chain().full_name())
                })
                .count();
            if multi_stage_count > 0 && multi_stage_count < members.len() {
                return Err(CubeError::user(format!(
                    "An OR filter group can't mix multi-stage dimensions with other members: {}",
                    members.iter().map(|m| m.full_name()).join(", ")
                )));
            }
            return Ok(());
        }
        for child in group.items.iter() {
            Self::check_no_mixed_or_groups(child, multi_stage_members)?;
        }
        Ok(())
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
        let mut context = context.clone();
        if let Some(evaluation_context) = logical_plan.evaluation_context() {
            context.apply_evaluation_context(evaluation_context);
        }
        let mut context_factory = context.make_sql_nodes_factory()?;
        let mut conditional_measures = logical_plan.conditional_measures().clone();

        let from = self
            .builder
            .process_node(logical_plan.source().as_ref(), &context)?;
        let mut filter = logical_plan.filter().all_filters();
        let mut having = logical_plan.filter().measures_filter();

        // Calc-group dimensions are resolved at query time: a value pinned by
        // a filter renders as a literal, otherwise the enumeration is
        // cross-joined as a virtual values table. Over a pre-aggregation this
        // applies only to calc groups NOT stored in the rollup — stored ones
        // keep resolving to the rollup column for backward compatibility.
        let calc_group_stored_dims = match logical_plan.source().base() {
            InputBase::QueryInputs(_) => Some(HashSet::new()),
            InputBase::PreAggregation(pre_aggregation) => Some(
                pre_aggregation
                    .all_dimensions_refererences()
                    .into_keys()
                    .collect::<HashSet<_>>(),
            ),
            InputBase::Merge(_) => None,
        };
        // Kept apart: a select over a pre-aggregation substitutes only these.
        let mut calc_group_literals = ReferenceSubstitutions::new();
        let from = if let Some(stored_dims) = calc_group_stored_dims {
            let all_symbols = all_symbols(&logical_plan.schema(), &logical_plan.filter());
            let calc_group_dims = collect_calc_group_dims_from_nodes(all_symbols.iter())?
                .into_iter()
                .filter(|dim| !stored_dims.contains(&dim.full_name()))
                .collect_vec();

            // Filters an enclosing select applies on this one's behalf pin a
            // calc group here as well.
            let pinning_filter = Filter {
                items: filter
                    .clone()
                    .into_iter()
                    .chain(
                        logical_plan
                            .filter_params_filters()
                            .as_ref()
                            .and_then(|filter| filter.all_filters()),
                    )
                    .flat_map(|filter| filter.items)
                    .collect(),
            };
            let pinning_filter = (!pinning_filter.items.is_empty()).then_some(pinning_filter);
            let calc_groups_items = calc_group_dims.into_iter().map(|dim| {
                let values = get_filtered_values(&dim, &pinning_filter);
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
                    item.symbol.id().clone(),
                    MemberSymbol::new_literal_ref(&item.symbol, item.values[0].clone()),
                );
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

        match logical_plan.source().base() {
            InputBase::Merge(fk) if fk.is_empty() => {
                // The select reads only multi-stage dimension CTEs. Filters on
                // plain members were applied inside them and the base cube is
                // out of scope here; only multi-stage dimension filters remain.
                let selected_multi_stage_dimensions = logical_plan
                    .schema()
                    .multi_stage_dimensions()?
                    .iter()
                    .map(|d| d.clone().resolve_reference_chain().full_name())
                    .collect::<HashSet<_>>();
                filter =
                    Self::multi_stage_dimension_filters(filter, &selected_multi_stage_dimensions)?;
            }
            InputBase::QueryInputs(inputs) => {
                self.builder.collect_subquery_dimensions_substitutions(
                    &inputs.dimension_subqueries(),
                    &references_builder,
                    &mut substitutions,
                )?;
            }
            InputBase::PreAggregation(pre_aggregation) => {
                // A rollup stores time dimensions already timezone-converted,
                // so every occurrence of them in this select must render
                // without the conversion.
                let time_dimension_ids = schema
                    .time_dimensions
                    .iter()
                    .map(|d| d.id().clone())
                    .collect::<HashSet<_>>();
                let mark_tz_converted =
                    |symbol: &Rc<MemberSymbol>| -> Result<Rc<MemberSymbol>, CubeError> {
                        transforms::mark_tz_converted_at_source(symbol, &time_dimension_ids)
                    };
                schema = logical_transforms::mark_tz_converted_at_source_in_schema(&schema)?;
                filter = transforms::map_filter_symbols(filter, &mark_tz_converted)?;
                having = transforms::map_filter_symbols(having, &mark_tz_converted)?;
                for conditional in conditional_measures.iter_mut() {
                    conditional.condition = transforms::map_filter_item_symbols(
                        &conditional.condition,
                        &mark_tz_converted,
                    )?;
                }
                context_factory.set_use_local_tz_in_date_range(true);

                for (name, column) in pre_aggregation.all_dimensions_refererences().into_iter() {
                    context_factory.add_pre_aggregation_dimension_reference(name, column);
                }
                for (name, column) in pre_aggregation.all_measures_refererences().into_iter() {
                    context_factory.add_pre_aggregation_measure_reference(name, column);
                }
            }
            InputBase::Merge(_) => {}
        }

        // An ungrouped select emits row-level measure values: a not-null
        // indicator form for count-likes. Measures already carrying a form
        // (raw values of an ungrouped multi-stage leaf) keep it.
        let measure_modifier = if logical_plan.modifers().ungrouped {
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

        let is_pre_aggregation =
            matches!(logical_plan.source().base(), InputBase::PreAggregation(_));

        if !logical_plan.modifers().ungrouped {
            context_factory.set_group_by_members(
                schema
                    .all_dimensions()
                    .map(|symbol| symbol.id().clone())
                    .collect(),
            );
        }

        let measures = &schema.measures;
        let over_full_aggregated_source = self.is_over_full_aggregated_source(logical_plan);
        let conditional_for = |measure: &Rc<MemberSymbol>| {
            conditional_measures
                .iter()
                .position(|c| member_chain_eq(&c.measure, measure))
        };

        // Over a pre-aggregation members are read from the rollup columns.
        if !is_pre_aggregation {
            for dimension in schema.all_dimensions() {
                self.builder.collect_query_dimension_substitution(
                    dimension,
                    &references_builder,
                    &from,
                    &mut substitutions,
                )?;
            }

            for measure in measures.iter() {
                let source = match conditional_for(measure) {
                    Some(i) => conditional_measures[i].base.clone(),
                    None => measure.clone(),
                };
                references_builder.collect_substitutions_for_member(
                    source,
                    &None,
                    &mut substitutions,
                )?;
            }

            if over_full_aggregated_source {
                references_builder.collect_substitutions_for_filter(&having, &mut substitutions)?;
            }
        }

        let schema = logical_transforms::substitute_symbols_in_schema(&schema, &substitutions)?;
        let binding_filter = filter.clone();
        let binding_having = having.clone();
        let filter = logical_transforms::substitute_symbols_in_filter(filter, &substitutions)?;
        let having = logical_transforms::substitute_symbols_in_filter(having, &substitutions)?;
        let substitute_filter_member = |symbol: &Rc<MemberSymbol>| {
            transforms::substitute_filter_member_by_name(symbol, &substitutions)
        };

        let mut select_builder =
            SelectBuilder::new(from.substitute_join_conditions(&substitutions)?);

        for dimension in schema.all_dimensions() {
            self.builder
                .project_query_dimension(dimension, &mut select_builder, &context)?;
        }

        for measure in measures.iter() {
            if let Some(i) = conditional_for(measure) {
                let conditional = &conditional_measures[i];
                select_builder.add_projection_conditional_measure(
                    measure,
                    &transforms::substitute_by_name(&conditional.base, &substitutions)?,
                    transforms::map_filter_item_symbols(
                        &conditional.condition,
                        &substitute_filter_member,
                    )?,
                );
            } else {
                let measure = transforms::substitute_by_name(measure, &substitutions)?;
                select_builder.add_projection_member(&measure, None);
            }
        }

        if over_full_aggregated_source {
            select_builder.set_filter(having);
            select_builder.set_binding_filter(binding_having);
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
            select_builder.set_binding_filter(binding_filter);
        }

        if let Some(filter_params_filters) = logical_plan.filter_params_filters() {
            select_builder.set_filter_params_filters(filter_params_filters.all_filters());
        }
        if logical_plan.modifers().distinct {
            select_builder.set_distinct();
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
                        && schema
                            .find_member_positions(o.member_symbol().id())
                            .is_empty())
                })
                .cloned()
                .collect()
        } else {
            logical_plan.modifers().order_by.clone()
        };
        // Items present in the schema are sorted by their stamped schema
        // symbol; only a measure absent from the projection carries its
        // own symbol into the ORDER BY and needs the form here.
        let order_by = if let Some(modifier) = &measure_modifier {
            order_by
                .iter()
                .map(|o| -> Result<_, CubeError> {
                    if !schema
                        .find_member_positions(o.member_symbol().id())
                        .is_empty()
                    {
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
