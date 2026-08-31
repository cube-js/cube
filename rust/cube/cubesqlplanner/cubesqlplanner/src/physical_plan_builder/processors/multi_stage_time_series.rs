use super::super::context::PushDownBuilderContext;
use super::super::{LogicalNodeProcessor, ProcessableNode};
use crate::logical_plan::MultiStageTimeSeries;
use crate::physical_plan::{
    CalendarPeriodSource, QueryPlan, TimeSeries, TimeSeriesDateRange, TimeSeriesSource,
};
use crate::physical_plan_builder::PhysicalPlanBuilder;
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
        context: &PushDownBuilderContext,
    ) -> Result<Self::PhysycalNode, CubeError> {
        let (query_tools, plan_sql_templates) = self.builder.qtools_and_templates();
        let time_dimension = time_series.time_dimension().clone();
        let time_dimension_symbol = time_dimension.as_time_dimension()?;
        let date_range = time_series.date_range().clone();
        let granularity_obj = if let Some(granularity_obj) = time_dimension_symbol.granularity_obj()
        {
            granularity_obj.clone()
        } else {
            return Err(CubeError::user(
                "Time dimension granularity is required for rolling window".to_string(),
            ));
        };

        if let Some(calendar_source) = time_series.calendar_source() {
            let source = self
                .builder
                .process_node(calendar_source.as_ref(), context)?;
            let schema = source.schema();
            let date_from_alias = schema.resolve_member_alias(&time_dimension);
            let period_aliases = time_series
                .period_dimensions()
                .iter()
                .map(|dimension| {
                    let granularity = dimension
                        .as_time_dimension()?
                        .granularity()
                        .clone()
                        .unwrap_or_default();
                    Ok((granularity, schema.resolve_member_alias(dimension)))
                })
                .collect::<Result<Vec<_>, CubeError>>()?;

            let time_series = TimeSeries::new(
                &time_dimension,
                TimeSeriesSource::Calendar(CalendarPeriodSource::new(
                    Rc::new(QueryPlan::Select(source)),
                    date_from_alias,
                    period_aliases,
                    time_series.get_date_range_multistage_ref().clone(),
                )),
                granularity_obj,
            );
            return Ok(QueryPlan::TimeSeries(Rc::new(time_series)));
        }

        let ts_date_range = if plan_sql_templates
            .supports_generated_time_series(granularity_obj.is_predefined_granularity())?
        {
            if let Some(date_range) = time_dimension_symbol
                .get_range_for_time_series(date_range, query_tools.timezone())?
            {
                TimeSeriesDateRange::Filter(date_range.0.clone(), date_range.1.clone())
            } else {
                if let Some(date_range_cte) = time_series.get_date_range_multistage_ref() {
                    TimeSeriesDateRange::Generated(date_range_cte.clone())
                } else {
                    return Err(CubeError::internal(
                        "Date range cte is required for time series without date range".to_string(),
                    ));
                }
            }
        } else {
            if let Some(date_range) = time_series.date_range() {
                TimeSeriesDateRange::Filter(date_range[0].clone(), date_range[1].clone())
            } else {
                return Err(CubeError::user(
                    "Date range is required for time series".to_string(),
                ));
            }
        };

        let time_series = TimeSeries::new(
            &time_dimension,
            TimeSeriesSource::Range(ts_date_range),
            granularity_obj,
        );
        let query_plan = QueryPlan::TimeSeries(Rc::new(time_series));
        Ok(query_plan)
    }
}

impl ProcessableNode for MultiStageTimeSeries {
    type ProcessorType<'a> = MultiStageTimeSeriesProcessor<'a>;
}
