use super::{FilterOperationSql, FilterSqlContext};
use crate::planner::filter::operators::in_list::InListOp;
use cubenativeutils::CubeError;

impl FilterOperationSql for InListOp {
    fn to_sql(&self, ctx: &FilterSqlContext) -> Result<String, CubeError> {
        let has_null = self.values.iter().any(|v| v.is_none());
        let need_null_check = if self.negated { !has_null } else { has_null };
        let allocated = ctx.allocate_and_cast_values(&self.values, &self.member_type)?;

        if self.negated {
            ctx.plan_templates
                .not_in_where(ctx.member_sql.to_string(), allocated, need_null_check)
        } else {
            ctx.plan_templates
                .in_where(ctx.member_sql.to_string(), allocated, need_null_check)
        }
    }
}
