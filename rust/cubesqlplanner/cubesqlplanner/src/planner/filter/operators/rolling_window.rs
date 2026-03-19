use super::{FilterOperationSql, FilterSqlContext};
use cubenativeutils::CubeError;

#[derive(Clone, Debug)]
pub struct RegularRollingWindowOp {
    trailing: Option<String>,
    leading: Option<String>,
}

impl RegularRollingWindowOp {
    pub fn new(trailing: Option<String>, leading: Option<String>) -> Self {
        Self { trailing, leading }
    }
}

impl FilterOperationSql for RegularRollingWindowOp {
    fn to_sql(&self, ctx: &FilterSqlContext) -> Result<String, CubeError> {
        let (from, to) = ctx.date_range_from_time_series()?;

        let from = ctx.extend_date_range_bound(from, &self.trailing, true)?;
        let to = ctx.extend_date_range_bound(to, &self.leading, false)?;

        let date_field = ctx.convert_tz(ctx.member_sql)?;

        match (&from, &to) {
            (Some(from), Some(to)) => {
                ctx.plan_templates
                    .time_range_filter(date_field, from.clone(), to.clone())
            }
            (Some(from), None) => ctx.plan_templates.gte(date_field, from.clone()),
            (None, Some(to)) => ctx.plan_templates.lte(date_field, to.clone()),
            (None, None) => ctx.plan_templates.always_true(),
        }
    }
}
