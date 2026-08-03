use super::{FilterOperationSql, FilterSqlContext};
use crate::planner::filter::operators::equality::EqualityOp;
use cubenativeutils::CubeError;

impl FilterOperationSql for EqualityOp {
    fn to_sql(&self, ctx: &FilterSqlContext) -> Result<String, CubeError> {
        let param = ctx.allocate_and_cast(&self.value, &self.member_type)?;
        // For negated (notEquals), add OR IS NULL check when value is not null
        let need_null_check = self.negated;
        if self.negated {
            ctx.plan_templates
                .not_equals(ctx.member_sql.to_string(), param, need_null_check)
        } else {
            ctx.plan_templates
                .equals(ctx.member_sql.to_string(), param, false)
        }
    }
}
