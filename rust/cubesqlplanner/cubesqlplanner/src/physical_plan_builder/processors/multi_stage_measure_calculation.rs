use super::super::{LogicalNodeProcessor, PullUpBuilderContext, PushDownBuilderContext};
use crate::logical_plan::MultiStageMeasureCalculation;
use crate::plan::QueryPlan;
use cubenativeutils::CubeError;
use std::rc::Rc;

pub struct MultiStageMeasureCalculationProcessor;

impl LogicalNodeProcessor for MultiStageMeasureCalculationProcessor {
    type LogicalNode = MultiStageMeasureCalculation;
    type PhysycalNode = QueryPlan;

    fn process(
        logical_plan: &Rc<Self::LogicalNode>,
        context: &PushDownBuilderContext,
    ) -> Result<(Self::PhysycalNode, PullUpBuilderContext), CubeError> {
        todo!()
    }
}
