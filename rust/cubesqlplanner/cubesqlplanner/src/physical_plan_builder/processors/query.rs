use super::super::{LogicalNodeProcessor, ProcessableNode, PushDownBuilderContext};
use crate::logical_plan::{Query, QuerySource};
use crate::physical_plan_builder::PhysicalPlanBuilder;
use crate::plan::{Cte, Expr, MemberExpression, Select, SelectBuilder};
use crate::planner::sql_evaluator::ReferencesBuilder;
use cubenativeutils::CubeError;
use std::collections::HashMap;
use std::rc::Rc;

pub struct QueryProcessor<'a> {
    builder: &'a PhysicalPlanBuilder,
}

impl QueryProcessor<'_> {
    fn is_over_full_aggregated_source(&self, logical_plan: &Query) -> bool {
        match logical_plan.source {
            QuerySource::FullKeyAggregate(_) => true,
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
        let mut render_references = HashMap::new();
        let mut context = context.clone();
        let mut ctes = vec![];

        for multi_stage_member in logical_plan.multistage_members.iter() {
            let query = self
                .builder
                .process_node(&multi_stage_member.member_type, &context)?;
            let alias = multi_stage_member.name.clone();
            context.add_multi_stage_schema(alias.clone(), query.schema());
            ctes.push(Rc::new(Cte::new(Rc::new(query), alias)));
        }

        let (from, is_pre_aggregation) = match &logical_plan.source {
            QuerySource::LogicalJoin(join) => {
                let from = self.builder.process_node(join.as_ref(), &context)?;
                let references_builder = ReferencesBuilder::new(from.clone());
                self.builder.resolve_subquery_dimensions_references(
                    &join.dimension_subqueries,
                    &references_builder,
                    &mut render_references,
                )?;
                (from, false)
            }
            QuerySource::FullKeyAggregate(full_key_aggregate) => {
                let from = self
                    .builder
                    .process_node(full_key_aggregate.as_ref(), &context)?;
                (from, false)
            }
            QuerySource::PreAggregation(pre_aggregation) => {
                let res = self
                    .builder
                    .process_node(pre_aggregation.as_ref(), &context)?;
                for member in logical_plan.schema.time_dimensions.iter() {
                    context_factory.add_dimensions_with_ignored_timezone(member.full_name());
                }
                context_factory.set_use_local_tz_in_date_range(true);

                let dimensions_references = pre_aggregation.all_dimensions_refererences();
                let measure_references = pre_aggregation.all_measures_refererences();

                context_factory.set_pre_aggregation_measures_references(measure_references);
                context_factory.set_pre_aggregation_dimensions_references(dimensions_references);
                (res, true)
            }
        };

        let references_builder = ReferencesBuilder::new(from.clone());

        let mut select_builder = SelectBuilder::new(from);
        select_builder.set_ctes(ctes);
        context_factory.set_ungrouped(logical_plan.modifers.ungrouped);

        for member in logical_plan.schema.all_dimensions() {
            references_builder.resolve_references_for_member(
                member.clone(),
                &None,
                &mut render_references,
            )?;
            if context.measure_subquery {
                select_builder.add_projection_member_without_schema(member, None);
            } else {
                select_builder.add_projection_member(member, None);
            }
        }

        for (measure, exists) in self
            .builder
            .measures_for_query(&logical_plan.schema.measures, &context)
        {
            if exists {
                references_builder.resolve_references_for_member(
                    measure.clone(),
                    &None,
                    &mut render_references,
                )?;
                select_builder.add_projection_member(&measure, None);
            } else {
                select_builder.add_null_projection(&measure, None);
            }
        }

        let filter = logical_plan.filter.all_filters();
        let having = logical_plan.filter.measures_filter();

        if self.is_over_full_aggregated_source(logical_plan) {
            references_builder.resolve_references_for_filter(&having, &mut render_references)?;
            select_builder.set_filter(having);
        } else {
            if !logical_plan.modifers.ungrouped {
                let group_by = logical_plan
                    .schema
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

        select_builder.set_limit(logical_plan.modifers.limit);
        select_builder.set_offset(logical_plan.modifers.offset);

        context_factory
            .set_rendered_as_multiplied_measures(logical_plan.schema.multiplied_measures.clone());
        if !is_pre_aggregation {
            context_factory.set_render_references(render_references);
        }
        if logical_plan.modifers.ungrouped {
            context_factory.set_ungrouped(true);
        }

        select_builder.set_order_by(
            self.builder
                .make_order_by(&logical_plan.schema, &logical_plan.modifers.order_by)?,
        );

        let res = Rc::new(select_builder.build(query_tools.clone(), context_factory));
        Ok(res)
    }
}

impl ProcessableNode for Query {
    type ProcessorType<'a> = QueryProcessor<'a>;
}
