use super::super::context::PushDownBuilderContext;
use super::super::{LogicalNodeProcessor, ProcessableNode};
use crate::logical_plan::{LogicalPlan, MultiStageMemberBody};
use crate::physical_plan::{Cte, QueryPlan, Select};
use crate::physical_plan_builder::PhysicalPlanBuilder;
use cubenativeutils::CubeError;
use std::rc::Rc;

pub struct PlanProcessor<'a> {
    builder: &'a PhysicalPlanBuilder,
}

impl<'a> LogicalNodeProcessor<'a, LogicalPlan> for PlanProcessor<'a> {
    type PhysycalNode = Rc<Select>;
    fn new(builder: &'a PhysicalPlanBuilder) -> Self {
        Self { builder }
    }

    fn process(
        &self,
        logical_plan: &LogicalPlan,
        context: &PushDownBuilderContext,
    ) -> Result<Self::PhysycalNode, CubeError> {
        let mut context = context.clone();
        let mut ctes = Vec::with_capacity(logical_plan.ctes().len());

        // Render every CTE body in declaration order — later bodies may
        // reference earlier CTE schemas via `context.add_cte_schema`.
        // The order mirrors how `cte_state` accumulated them in the planner.
        for member in logical_plan.ctes().iter() {
            let body_plan = self.render_body(&member.body, &context)?;
            let alias = member.name.clone();
            context.add_cte_schema(alias.clone(), body_plan.schema());
            ctes.push(Rc::new(Cte::new(Rc::new(body_plan), alias)));
        }

        let root_select = self
            .builder
            .process_node(logical_plan.root().as_ref(), &context)?;
        Ok(root_select.with_ctes(ctes))
    }
}

impl<'a> PlanProcessor<'a> {
    fn render_body(
        &self,
        body: &MultiStageMemberBody,
        context: &PushDownBuilderContext,
    ) -> Result<QueryPlan, CubeError> {
        match body {
            MultiStageMemberBody::Query(q) => {
                let select = self.builder.process_node(q.as_ref(), context)?;
                Ok(QueryPlan::Select(select))
            }
            MultiStageMemberBody::TimeSeries(ts) => self.builder.process_node(ts.as_ref(), context),
            MultiStageMemberBody::RollingWindow(rw) => {
                self.builder.process_node(rw.as_ref(), context)
            }
        }
    }
}

impl ProcessableNode for LogicalPlan {
    type ProcessorType<'a> = PlanProcessor<'a>;
}
