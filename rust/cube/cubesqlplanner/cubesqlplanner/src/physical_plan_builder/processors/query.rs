use super::super::{LogicalNodeProcessor, ProcessableNode, PushDownBuilderContext};
use crate::logical_plan::transforms as logical_transforms;
use crate::logical_plan::Query;
use crate::physical_plan::{
    CalcGroupItem, CalcGroupsJoin, Expr, From, MemberExpression, ReferenceSubstitutions,
    ReferencesBuilder, Select, SelectBuilder,
};
use crate::physical_plan_builder::PhysicalPlanBuilder;
use crate::planner::filter::map_filter;
use crate::planner::query_properties::member_chain_eq;
use crate::planner::symbols::transforms;
use crate::planner::{MeasureRenderModifier, MemberSymbol, OrderByItem};
use cubenativeutils::CubeError;
use std::collections::HashSet;
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
        let mut context = context.clone();
        if let Some(evaluation_context) = logical_plan.evaluation_context() {
            context.apply_evaluation_context(evaluation_context);
        }
        let mut context_factory = context.make_sql_nodes_factory()?;
        let mut conditional_measures = logical_plan.conditional_measures().clone();

        if let Some(reason) = logical_plan.unrenderable_filter() {
            return Err(CubeError::user(reason.clone()));
        }
        let from = self
            .builder
            .process_node(logical_plan.from().as_ref(), &context)?;
        let mut filter = logical_plan.applied_filter().all_filters();
        let mut having = logical_plan.applied_filter().measures_filter();

        let from_source = logical_plan.from();
        let mut calc_group_literals = ReferenceSubstitutions::new();
        let mut calc_groups_to_join = vec![];
        for calc_group in from_source.calc_groups().iter() {
            match calc_group.values.as_slice() {
                [] => {}
                [value] => {
                    calc_group_literals.insert(
                        calc_group.dimension.id().clone(),
                        MemberSymbol::new_literal_ref(&calc_group.dimension, value.clone()),
                    );
                }
                _ => calc_groups_to_join.push(CalcGroupItem {
                    symbol: calc_group.dimension.clone(),
                    values: calc_group.values.clone(),
                }),
            }
        }
        let from = if calc_groups_to_join.is_empty() {
            from
        } else {
            From::new_from_calc_groups_join(CalcGroupsJoin::try_new(from, calc_groups_to_join)?)
        };

        let mut schema = logical_plan.schema().clone();
        let references_builder = ReferencesBuilder::new(from.clone());
        let mut substitutions = calc_group_literals;

        if let Some(pre_aggregation) = from_source.as_pre_aggregation() {
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
            filter = map_filter(filter, &mark_tz_converted)?;
            having = map_filter(having, &mark_tz_converted)?;
            for conditional in conditional_measures.iter_mut() {
                conditional.condition = conditional.condition.map_symbols(&mark_tz_converted)?;
            }
            context_factory.set_use_local_tz_in_date_range(true);

            for (name, column) in pre_aggregation.all_dimensions_refererences().into_iter() {
                context_factory.add_pre_aggregation_dimension_reference(name, column);
            }
            for (name, column) in pre_aggregation.all_measures_refererences().into_iter() {
                context_factory.add_pre_aggregation_measure_reference(name, column);
            }
        } else if logical_plan.applies_whole_filter() {
            self.builder.collect_from_inputs_substitutions(
                from_source,
                &references_builder,
                &context,
                &mut substitutions,
            )?;
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
            filter = map_filter(filter, &stamp)?;
            having = map_filter(having, &stamp)?;
        }

        let is_pre_aggregation = logical_plan.from().as_pre_aggregation().is_some();

        if !logical_plan.modifers().ungrouped {
            context_factory.set_group_by_members(
                schema
                    .all_dimensions()
                    .map(|symbol| symbol.id().clone())
                    .collect(),
            );
        }

        let measures = &schema.measures;
        let over_full_aggregated_source = !logical_plan.aggregates();
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

            // A sort-only measure is read from the source like a selected one;
            // over aggregated inputs its raw expression is out of scope.
            for item in logical_plan.modifers().order_by.iter() {
                let member = item.member_symbol();
                if member.is_measure() && schema.find_member_positions(member.id()).is_empty() {
                    references_builder.collect_substitutions_for_member(
                        member,
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
                .project_query_dimension(dimension, &mut select_builder)?;
        }

        for measure in measures.iter() {
            if let Some(i) = conditional_for(measure) {
                let conditional = &conditional_measures[i];
                select_builder.add_projection_conditional_measure(
                    measure,
                    &transforms::substitute_by_name(&conditional.base, &substitutions)?,
                    conditional
                        .condition
                        .map_symbols(&substitute_filter_member)?,
                );
            } else {
                let measure = transforms::substitute_by_name(measure, &substitutions)?;
                select_builder.add_projection_member(&measure, None);
            }
        }

        if over_full_aggregated_source {
            if filter.is_some() {
                return Err(CubeError::internal(
                    "A select over aggregated rows applies only measure filters".to_string(),
                ));
            }
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
