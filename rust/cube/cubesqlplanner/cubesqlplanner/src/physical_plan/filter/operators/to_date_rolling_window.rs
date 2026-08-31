use super::{FilterOperationSql, FilterSqlContext};
use crate::physical_plan::TimeSeries;
use crate::planner::filter::operators::to_date_rolling_window::ToDateRollingWindowOp;
use cubenativeutils::CubeError;

impl FilterOperationSql for ToDateRollingWindowOp {
    fn to_sql(&self, ctx: &FilterSqlContext) -> Result<String, CubeError> {
        let (from, to) = ctx.date_range_from_time_series()?;

        // A calendar period cannot be derived from a timestamp — the series
        // carries the boundary it read off the calendar cube.
        let from = if self.granularity.calendar_sql().is_some() {
            ctx.time_series_bound(
                "min",
                &TimeSeries::period_start_column(self.granularity.granularity()),
            )?
        } else {
            self.granularity
                .apply_to_input_sql(ctx.plan_templates, from)?
        };

        let date_field = ctx.convert_tz(ctx.member_sql())?;
        ctx.plan_templates.time_range_filter(date_field, from, to)
    }
}
