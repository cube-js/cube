use super::{FilterOperationSql, FilterSqlContext};
use crate::planner::Granularity;
use cubenativeutils::CubeError;

#[derive(Clone)]
pub struct ToDateRollingWindowOp {
    granularity: Granularity,
}

impl std::fmt::Debug for ToDateRollingWindowOp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ToDateRollingWindowOp")
            .field("granularity", &"<Granularity>")
            .finish()
    }
}

impl ToDateRollingWindowOp {
    pub fn new(granularity: Granularity) -> Self {
        Self { granularity }
    }
}

impl FilterOperationSql for ToDateRollingWindowOp {
    fn to_sql(&self, ctx: &FilterSqlContext) -> Result<String, CubeError> {
        let (from, to) = ctx.date_range_from_time_series()?;

        let from = self
            .granularity
            .apply_to_input_sql(ctx.plan_templates, from)?;

        let date_field = ctx.convert_tz(ctx.member_sql)?;
        ctx.plan_templates.time_range_filter(date_field, from, to)
    }
}
