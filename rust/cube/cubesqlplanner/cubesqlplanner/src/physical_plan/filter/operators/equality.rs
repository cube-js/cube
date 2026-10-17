use super::{FilterOperationSql, FilterSqlContext};
use crate::planner::filter::operators::equality::EqualityOp;
use cubenativeutils::CubeError;

impl FilterOperationSql for EqualityOp {
    fn to_sql(&self, ctx: &FilterSqlContext) -> Result<String, CubeError> {
        let param = ctx.allocate_and_cast(&self.value, &self.member_type)?;
        // A dialect that renders time lists its own way renders a single time value the same
        // way, as an OR group of single equalities can otherwise form the chain it avoids
        if !self.negated && self.member_type.as_deref() == Some("time") {
            if let Some(sql) = ctx.plan_templates.time_in_where(
                ctx.member_sql().to_string(),
                std::slice::from_ref(&param),
                false,
                false,
            )? {
                return Ok(sql);
            }
        }
        // For negated (notEquals), add OR IS NULL check when value is not null
        let need_null_check = self.negated;
        if self.negated {
            ctx.plan_templates
                .not_equals(ctx.member_sql().to_string(), param, need_null_check)
        } else {
            ctx.plan_templates
                .equals(ctx.member_sql().to_string(), param, false)
        }
    }
}
