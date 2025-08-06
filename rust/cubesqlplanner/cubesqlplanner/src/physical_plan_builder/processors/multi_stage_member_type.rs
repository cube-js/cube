use super::super::context::PushDownBuilderContext;
use super::super::{LogicalNodeProcessor, ProcessableNode};
use crate::logical_plan::MultiStageMemberLogicalType;
use crate::physical_plan_builder::PhysicalPlanBuilder;
use crate::plan::QueryPlan;
use cubenativeutils::CubeError;

pub struct MultiStageMemberLogicalTypeProcessor<'a> {
    builder: &'a PhysicalPlanBuilder,
}

impl<'a> LogicalNodeProcessor<'a, MultiStageMemberLogicalType>
    for MultiStageMemberLogicalTypeProcessor<'a>
{
    type PhysycalNode = QueryPlan;
    fn new(builder: &'a PhysicalPlanBuilder) -> Self {
        Self { builder }
    }

    fn process(
        &self,
        multi_stage_member: &MultiStageMemberLogicalType,
        context: &PushDownBuilderContext,
    ) -> Result<Self::PhysycalNode, CubeError> {
        match multi_stage_member {
            MultiStageMemberLogicalType::LeafMeasure(measure) => {
                self.builder.process_node(measure.as_ref(), context)
            }
            MultiStageMemberLogicalType::MeasureCalculation(calculation) => {
                self.builder.process_node(calculation.as_ref(), context)
            }
            MultiStageMemberLogicalType::GetDateRange(get_date_range) => {
                self.builder.process_node(get_date_range.as_ref(), context)
            }
            MultiStageMemberLogicalType::TimeSeries(time_series) => {
                self.builder.process_node(time_series.as_ref(), context)
            }
            MultiStageMemberLogicalType::RollingWindow(rolling_window) => {
                self.builder.process_node(rolling_window.as_ref(), context)
            }
        }
    }
}

impl ProcessableNode for MultiStageMemberLogicalType {
    type ProcessorType<'a> = MultiStageMemberLogicalTypeProcessor<'a>;
}
