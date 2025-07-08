use super::super::{LogicalNodeProcessor, ProcessableNode};
use super::super::context::PushDownBuilderContext;
use crate::logical_plan::MultiStageMeasureCalculation;
use crate::physical_plan_builder::PhysicalPlanBuilder;
use crate::plan::QueryPlan;
use crate::planner::query_tools::QueryTools;
use crate::planner::sql_templates::PlanSqlTemplates;
use cubenativeutils::CubeError;
use std::rc::Rc;

pub struct MultiStageMeasureCalculationProcessor<'a> {
    builder: &'a PhysicalPlanBuilder,
}

impl<'a> LogicalNodeProcessor<'a, MultiStageMeasureCalculation>
    for MultiStageMeasureCalculationProcessor<'a>
{
    type PhysycalNode = QueryPlan;
    fn new(builder: &'a PhysicalPlanBuilder) -> Self {
        Self { builder }
    }

    fn process(
        &self,
        logical_plan: &MultiStageMeasureCalculation,
        context: &PushDownBuilderContext,
    ) -> Result<Self::PhysycalNode, CubeError> {
        todo!()
    }
}

impl ProcessableNode for MultiStageMeasureCalculation {
    type ProcessorType<'a> = MultiStageMeasureCalculationProcessor<'a>;
}

