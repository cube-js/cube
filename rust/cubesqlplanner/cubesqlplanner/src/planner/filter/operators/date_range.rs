use super::{FilterOperationSql, FilterSqlContext};
use cubenativeutils::CubeError;

#[derive(Clone, Debug)]
pub enum DateRangeKind {
    InRange,
    NotInRange,
}

#[derive(Clone, Debug)]
pub struct DateRangeOp {
    kind: DateRangeKind,
    from: String,
    to: String,
}

impl DateRangeOp {
    pub fn new(kind: DateRangeKind, from: String, to: String) -> Self {
        Self { kind, from, to }
    }
}

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
