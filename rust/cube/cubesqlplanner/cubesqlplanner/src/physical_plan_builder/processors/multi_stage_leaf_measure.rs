use super::super::context::PushDownBuilderContext;
use super::super::{LogicalNodeProcessor, ProcessableNode};
use crate::logical_plan::MultiStageLeafMeasure;
use crate::physical_plan::QueryPlan;
use crate::physical_plan_builder::PhysicalPlanBuilder;
use cubenativeutils::CubeError;

pub struct MultiStageLeafMeasureProcessor<'a> {
    builder: &'a PhysicalPlanBuilder,
}

impl<'a> LogicalNodeProcessor<'a, MultiStageLeafMeasure> for MultiStageLeafMeasureProcessor<'a> {
    type PhysycalNode = QueryPlan;
    fn new(builder: &'a PhysicalPlanBuilder) -> Self {
        Self { builder }
    }

    fn process(
        &self,
        multi_stage_leaf_measure: &MultiStageLeafMeasure,
        context: &PushDownBuilderContext,
    ) -> Result<Self::PhysycalNode, CubeError> {
        let mut context = context.clone();
        context.apply_render_context(&multi_stage_leaf_measure.render_context);
        let select = self
            .builder
            .process_node(multi_stage_leaf_measure.query.as_ref(), &context)?;
        Ok(QueryPlan::Select(select))
    }
}

impl ProcessableNode for MultiStageLeafMeasure {
    type ProcessorType<'a> = MultiStageLeafMeasureProcessor<'a>;
}
