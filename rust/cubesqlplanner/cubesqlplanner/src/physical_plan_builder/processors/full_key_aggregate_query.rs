use super::super::{LogicalNodeProcessor, ProcessableNode, PushDownBuilderContext};
use crate::logical_plan::{FullKeyAggregateQuery, SimpleQuery, SimpleQuerySource};
use crate::physical_plan_builder::PhysicalPlanBuilder;
use crate::plan::{Expr, Filter, MemberExpression, QueryPlan, Select, SelectBuilder};
use crate::planner::query_tools::QueryTools;
use crate::planner::sql_evaluator::ReferencesBuilder;
use crate::planner::sql_templates::PlanSqlTemplates;
use crate::planner::{BaseMember, MemberSymbolRef};
use cubenativeutils::CubeError;
use itertools::Itertools;
use std::collections::HashMap;
use std::rc::Rc;

pub struct FullKeyAggregateQueryProcessor<'a> {
    builder: &'a PhysicalPlanBuilder,
}

impl<'a> LogicalNodeProcessor<'a, FullKeyAggregateQuery> for FullKeyAggregateQueryProcessor<'a> {
    type PhysycalNode = Rc<Select>;
    fn new(builder: &'a PhysicalPlanBuilder) -> Self {
        Self { builder }
    }

    fn process(
        &self,
        logical_plan: &FullKeyAggregateQuery,
        context: &PushDownBuilderContext,
    ) -> Result<Self::PhysycalNode, CubeError> {
        let query_tools = self.builder.query_tools();
        //let mut multi_stage_schemas = HashMap::new();
        //let mut ctes = Vec::new();
        /* for multi_stage_member in logical_plan.multistage_members.iter() {
            ctes.push(self.processs_multi_stage_member(
                multi_stage_member,
                &mut multi_stage_schemas,
                context,
            )?);
        } */
        let from = self
            .builder
            .process_node(logical_plan.source.as_ref(), context)?;

        let references_builder = ReferencesBuilder::new(from.clone());
        let mut render_references = HashMap::new();

        let mut select_builder = SelectBuilder::new(from.clone());
        let all_dimensions = logical_plan.schema.all_dimensions().cloned().collect_vec();

        self.builder.process_full_key_aggregate_dimensions_new(
            &all_dimensions,
            &logical_plan.source,
            &mut select_builder,
            &references_builder,
            &mut render_references,
            from.all_sources(),
        )?;

        for measure in logical_plan.schema.measures.iter() {
            references_builder.resolve_references_for_member(
                measure.clone(),
                &None,
                &mut render_references,
            )?;
            let alias = references_builder.resolve_alias_for_member(&measure.full_name(), &None);
            select_builder.add_projection_member(
                &measure.clone().as_base_member(query_tools.clone())?,
                alias,
            );
        }

        let having = if logical_plan.filter.measures_filter.is_empty() {
            None
        } else {
            let filter = Filter {
                items: logical_plan.filter.measures_filter.clone(),
            };
            references_builder.resolve_references_for_filter(&filter, &mut render_references)?;
            Some(filter)
        };

        select_builder.set_order_by(
            self.builder
                .make_order_by(&logical_plan.schema, &logical_plan.modifers.order_by)?,
        );
        select_builder.set_filter(having);
        select_builder.set_limit(logical_plan.modifers.limit);
        select_builder.set_offset(logical_plan.modifers.offset);
        //select_builder.set_ctes(ctes);

        let mut context_factory = context.make_sql_nodes_factory();
        context_factory.set_render_references(render_references);

        Ok(Rc::new(select_builder.build(context_factory)))
    }
}

impl ProcessableNode for FullKeyAggregateQuery {
    type ProcessorType<'a> = FullKeyAggregateQueryProcessor<'a>;
}
