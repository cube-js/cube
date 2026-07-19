use super::{FilterOperationSql, FilterSqlContext};
use crate::planner::filter::operators::date_range::{DateRangeKind, DateRangeOp};
use cubenativeutils::CubeError;

impl FilterOperationSql for DateRangeOp {
    fn to_sql(&self, ctx: &FilterSqlContext) -> Result<String, CubeError> {
        let from_param = ctx.format_and_allocate_from_date(&self.from)?;
        let to_param = ctx.format_and_allocate_to_date(&self.to)?;
        match self.kind {
            DateRangeKind::InRange => ctx.plan_templates.time_range_filter(
                ctx.member_sql.to_string(),
                from_param,
                to_param,
            ),
            DateRangeKind::NotInRange => ctx.plan_templates.time_not_in_range_filter(
                ctx.member_sql.to_string(),
                from_param,
                to_param,
            ),
        }
    }
}
