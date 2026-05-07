use super::{FilterOperationSql, FilterSqlContext};
use crate::planner::filter::operators::nullability::NullabilityOp;
use cubenativeutils::CubeError;

impl FilterOperationSql for NullabilityOp {
    fn to_sql(&self, ctx: &FilterSqlContext) -> Result<String, CubeError> {
        if self.negated {
            ctx.plan_templates.not_set_where(ctx.member_sql.to_string())
        } else {
            ctx.plan_templates.set_where(ctx.member_sql.to_string())
        }
    }
}
