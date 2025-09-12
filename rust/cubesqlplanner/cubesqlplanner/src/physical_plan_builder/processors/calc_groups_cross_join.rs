use super::super::{LogicalNodeProcessor, ProcessableNode, PushDownBuilderContext};
use crate::logical_plan::CalcGroupsCrossJoin;
use crate::physical_plan_builder::PhysicalPlanBuilder;
use crate::plan::{From, JoinBuilder, JoinCondition};
use crate::planner::SqlJoinCondition;
use cubenativeutils::CubeError;
use std::rc::Rc;

/* pub struct CalcGroupsCrossJoinProcessor<'a> {
    builder: &'a PhysicalPlanBuilder,
}

impl<'a> LogicalNodeProcessor<'a, LogicalJoin> for CalcGroupsCrossJoinProcessor<'a> {
    type PhysycalNode = Rc<From>;
    fn new(builder: &'a PhysicalPlanBuilder) -> Self {
        Self { builder }
    }

    fn process(
        &self,
        logical_join: &CalcGroupsCrossJoin,
        context: &PushDownBuilderContext,
    ) -> Result<Self::PhysycalNode, CubeError> {
        todo!()
    }
}

impl ProcessableNode for CalcGroupsCrossJoin {
    type ProcessorType<'a> = CalcGroupsCrossJoinProcessor<'a>;
} */
