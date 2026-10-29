use super::super::context::PushDownBuilderContext;
use super::super::{LogicalNodeProcessor, ProcessableNode};
use crate::logical_plan::SubqueryBody;
use crate::physical_plan::QueryPlan;
use crate::physical_plan_builder::PhysicalPlanBuilder;
use cubenativeutils::CubeError;

pub struct SubqueryBodyProcessor<'a> {
    builder: &'a PhysicalPlanBuilder,
}

impl<'a> LogicalNodeProcessor<'a, SubqueryBody> for SubqueryBodyProcessor<'a> {
    type PhysycalNode = QueryPlan;
    fn new(builder: &'a PhysicalPlanBuilder) -> Self {
        Self { builder }
    }

    fn process(
        &self,
        body: &SubqueryBody,
        context: &PushDownBuilderContext,
    ) -> Result<Self::PhysycalNode, CubeError> {
        match body {
            SubqueryBody::LeafMeasure(query) => {
                let select = self.builder.process_node(query.as_ref(), context)?;
                Ok(QueryPlan::Select(select))
            }
            SubqueryBody::MultipliedMeasure(subquery) => {
                let select = self.builder.process_node(subquery.as_ref(), context)?;
                Ok(QueryPlan::Select(select))
            }
            SubqueryBody::MeasureCalculation(calculation) => {
                self.builder.process_node(calculation.as_ref(), context)
            }
            SubqueryBody::DimensionCalculation(calculation) => {
                self.builder.process_node(calculation.as_ref(), context)
            }
            SubqueryBody::GetDateRange(get_date_range) => {
                self.builder.process_node(get_date_range.as_ref(), context)
            }
            SubqueryBody::TimeSeries(time_series) => {
                self.builder.process_node(time_series.as_ref(), context)
            }
            SubqueryBody::RollingWindow(rolling_window) => {
                self.builder.process_node(rolling_window.as_ref(), context)
            }
        }
    }
}

impl ProcessableNode for SubqueryBody {
    type ProcessorType<'a> = SubqueryBodyProcessor<'a>;
}
