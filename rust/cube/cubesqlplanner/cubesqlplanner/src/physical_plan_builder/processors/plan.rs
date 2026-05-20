use super::super::context::PushDownBuilderContext;
use super::super::{LogicalNodeProcessor, ProcessableNode};
use crate::logical_plan::{LogicalPlan, MultiStageMemberBody, QueryKind, StageKind};
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

            if let MultiStageMemberBody::Plan(plan) = &member.body {
                if let QueryKind::Stage(StageKind::DimensionCalc {
                    multi_stage_dimension,
                }) = plan.root().kind()
                {
                    let inner_schema = plan.root().schema();
                    context.add_multi_stage_dimension_schema(
                        inner_schema.multi_stage_dimensions_resolved_names()?,
                        alias.clone(),
                        inner_schema.multi_stage_join_dimensions(multi_stage_dimension)?,
                        body_plan.schema(),
                    );
                }
            }

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
            MultiStageMemberBody::Plan(plan) => {
                let select = self.builder.process_node(plan.as_ref(), context)?;
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
