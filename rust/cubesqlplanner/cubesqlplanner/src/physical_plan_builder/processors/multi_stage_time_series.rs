use super::super::context::PushDownBuilderContext;
use super::super::{LogicalNodeProcessor, ProcessableNode};
use crate::logical_plan::MultiStageTimeSeries;
use crate::physical_plan_builder::PhysicalPlanBuilder;
use crate::plan::{QueryPlan, TimeSeries, TimeSeriesDateRange};
use cubenativeutils::CubeError;
use std::rc::Rc;

pub struct MultiStageTimeSeriesProcessor<'a> {
    builder: &'a PhysicalPlanBuilder,
}

impl<'a> LogicalNodeProcessor<'a, MultiStageTimeSeries> for MultiStageTimeSeriesProcessor<'a> {
    type PhysycalNode = QueryPlan;
    fn new(builder: &'a PhysicalPlanBuilder) -> Self {
        Self { builder }
    }

    fn process(
        &self,
        time_series: &MultiStageTimeSeries,
        _context: &PushDownBuilderContext,
    ) -> Result<Self::PhysycalNode, CubeError> {
        let (query_tools, plan_sql_templates) = self.builder.qtools_and_templates();
        let time_dimension = time_series.time_dimension.clone();
        let time_dimension_symbol = time_dimension.as_time_dimension()?;
        let date_range = time_series.date_range.clone();
        let granularity_obj = if let Some(granularity_obj) = time_dimension_symbol.granularity_obj()
        {
            granularity_obj.clone()
        } else {
            return Err(CubeError::user(
                "Time dimension granularity is required for rolling window".to_string(),
            ));
        };

        let ts_date_range = if plan_sql_templates
            .supports_generated_time_series(granularity_obj.is_predefined_granularity())?
        {
            if let Some(date_range) = time_dimension_symbol
                .get_range_for_time_series(date_range, query_tools.timezone())?
            {
                TimeSeriesDateRange::Filter(date_range.0.clone(), date_range.1.clone())
            } else {
                if let Some(date_range_cte) = &time_series.get_date_range_multistage_ref {
                    TimeSeriesDateRange::Generated(date_range_cte.clone())
                } else {
                    return Err(CubeError::internal(
                        "Date range cte is required for time series without date range".to_string(),
                    ));
                }
            }
        } else {
            if let Some(date_range) = &time_series.date_range {
                TimeSeriesDateRange::Filter(date_range[0].clone(), date_range[1].clone())
            } else {
                return Err(CubeError::user(
                    "Date range is required for time series".to_string(),
                ));
            }
        };

        let time_series = TimeSeries::new(
            query_tools.clone(),
            time_dimension.full_name(),
            ts_date_range,
            granularity_obj,
        );
        let query_plan = QueryPlan::TimeSeries(Rc::new(time_series));
        Ok(query_plan)
    }
}

impl ProcessableNode for MultiStageTimeSeries {
    type ProcessorType<'a> = MultiStageTimeSeriesProcessor<'a>;
}
