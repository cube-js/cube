use super::super::{LogicalNodeProcessor, ProcessableNode, PushDownBuilderContext};
use crate::logical_plan::{SimpleQuery, SimpleQuerySource};
use crate::physical_plan_builder::PhysicalPlanBuilder;
use crate::plan::{Expr, Filter, MemberExpression, QueryPlan, Select, SelectBuilder};
use crate::planner::query_tools::QueryTools;
use crate::planner::sql_templates::PlanSqlTemplates;
use crate::planner::{BaseMember, MemberSymbolRef};
use cubenativeutils::CubeError;
use std::collections::HashMap;
use std::rc::Rc;

pub struct SimpleQueryProcessor<'a> {
    builder: &'a PhysicalPlanBuilder,
}

impl<'a> LogicalNodeProcessor<'a, SimpleQuery> for SimpleQueryProcessor<'a> {
    type PhysycalNode = Rc<Select>;
    fn new(builder: &'a PhysicalPlanBuilder) -> Self {
        Self { builder }
    }

    fn process(
        &self,
        logical_plan: &SimpleQuery,
        context: &PushDownBuilderContext,
    ) -> Result<Self::PhysycalNode, CubeError> {
        let query_tools = self.builder.query_tools();
        let mut context_factory = context.make_sql_nodes_factory();
        let mut render_references = HashMap::new();
        let from = match &logical_plan.source {
            SimpleQuerySource::LogicalJoin(join) => {
                self.builder.process_node(join.as_ref(), context)?
            }
            SimpleQuerySource::PreAggregation(pre_aggregation) => {
                todo!()
                /* let res = self.process_pre_aggregation(
                    pre_aggregation,
                    context,
                    &mut measure_references,
                    &mut dimensions_references,
                )?;
                for member in logical_plan.schema.time_dimensions.iter() {
                    context_factory.add_dimensions_with_ignored_timezone(member.full_name());
                }
                context_factory.set_use_local_tz_in_date_range(true);
                res */
            }
        };

        let mut select_builder = SelectBuilder::new(from);
        context_factory.set_ungrouped(logical_plan.modifers.ungrouped);

        let mut group_by = Vec::new();
        for member in logical_plan.schema.dimensions.iter() {
            let member_ref: Rc<dyn BaseMember> =
                MemberSymbolRef::try_new(member.clone(), query_tools.clone())?;
            select_builder.add_projection_member(&member_ref, None);
            if !logical_plan.modifers.ungrouped {
                group_by.push(Expr::Member(MemberExpression::new(member_ref.clone())));
            }
        }
        for member in logical_plan.schema.time_dimensions.iter() {
            let member_ref: Rc<dyn BaseMember> =
                MemberSymbolRef::try_new(member.clone(), query_tools.clone())?;
            select_builder.add_projection_member(&member_ref, None);
            if !logical_plan.modifers.ungrouped {
                group_by.push(Expr::Member(MemberExpression::new(member_ref.clone())));
            }
        }
        for member in logical_plan.schema.measures.iter() {
            select_builder.add_projection_member(
                &MemberSymbolRef::try_new(member.clone(), query_tools.clone())?,
                None,
            );
        }

        let filter = logical_plan.filter.all_filters();
        let having = if logical_plan.filter.measures_filter.is_empty() {
            None
        } else {
            Some(Filter {
                items: logical_plan.filter.measures_filter.clone(),
            })
        };

        select_builder.set_filter(filter);
        select_builder.set_group_by(group_by);
        select_builder.set_order_by(
            self.builder
                .make_order_by(&logical_plan.schema, &logical_plan.modifers.order_by)?,
        );
        select_builder.set_having(having);
        select_builder.set_limit(logical_plan.modifers.limit);
        select_builder.set_offset(logical_plan.modifers.offset);

        context_factory
            .set_rendered_as_multiplied_measures(logical_plan.schema.multiplied_measures.clone());
        context_factory.set_render_references(render_references);
        if logical_plan.modifers.ungrouped {
            context_factory.set_ungrouped(true);
        }

        let res = Rc::new(select_builder.build(context_factory));
        Ok(res)
    }
}

impl ProcessableNode for SimpleQuery {
    type ProcessorType<'a> = SimpleQueryProcessor<'a>;
}
