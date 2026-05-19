use super::super::context::PushDownBuilderContext;
use super::super::{LogicalNodeProcessor, ProcessableNode};
use crate::logical_plan::{LogicalPlan, PlanNode, QueryKind, StageKind};
use crate::physical_plan::{Cte, QueryPlan};
use crate::physical_plan_builder::PhysicalPlanBuilder;
use cubenativeutils::CubeError;
use std::rc::Rc;

pub struct PlanProcessor<'a> {
    builder: &'a PhysicalPlanBuilder,
}

impl<'a> LogicalNodeProcessor<'a, LogicalPlan> for PlanProcessor<'a> {
    type PhysycalNode = QueryPlan;
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
            let body_plan = self.builder.process_node(member.body.as_ref(), &context)?;
            let alias = member.name.clone();
            context.add_cte_schema(alias.clone(), body_plan.schema());

            if let PlanNode::Query(inner_query) = member.body.root() {
                if let QueryKind::Stage(StageKind::DimensionCalc {
                    multi_stage_dimension,
                }) = inner_query.kind()
                {
                    let inner_schema = inner_query.schema();
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

        let root_plan = self.render_root(logical_plan.root(), &context)?;
        if ctes.is_empty() {
            Ok(root_plan)
        } else {
            match root_plan {
                QueryPlan::Select(select) => Ok(QueryPlan::Select(select.with_ctes(ctes))),
                _ => Err(CubeError::internal(format!(
                    "LogicalPlan with CTEs requires a Select-shaped root, got {}",
                    logical_plan.root().node_name()
                ))),
            }
        }
    }
}

impl<'a> PlanProcessor<'a> {
    fn render_root(
        &self,
        root: &PlanNode,
        context: &PushDownBuilderContext,
    ) -> Result<QueryPlan, CubeError> {
        match root {
            PlanNode::Query(query) => {
                let select = self.builder.process_node(query.as_ref(), context)?;
                Ok(QueryPlan::Select(select))
            }
            PlanNode::MultiStageTimeSeries(ts) => self.builder.process_node(ts.as_ref(), context),
            PlanNode::MultiStageRollingWindow(rw) => {
                self.builder.process_node(rw.as_ref(), context)
            }
            other => Err(CubeError::internal(format!(
                "Unexpected plan node {} as LogicalPlan root",
                other.node_name()
            ))),
        }
    }
}

impl ProcessableNode for LogicalPlan {
    type ProcessorType<'a> = PlanProcessor<'a>;
}
