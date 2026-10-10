use super::{FilterOperationSql, FilterSqlContext};
use crate::planner::filter::operators::in_list::InListOp;
use cubenativeutils::CubeError;

impl FilterOperationSql for InListOp {
    fn to_sql(&self, ctx: &FilterSqlContext) -> Result<String, CubeError> {
        let has_null = self.values.iter().any(|v| v.is_null());
        let need_null_check = if self.negated { !has_null } else { has_null };
        let mut allocated = ctx.allocate_and_cast_values(&self.values, &self.member_type)?;
        let member_sql = ctx.member_sql().to_string();
        let mut column = member_sql.clone();

        // Some dialects resolve a list of mixed types by comparing as strings, so
        // both sides of a time list are brought to the same type explicitly.
        if self.member_type.as_deref() == Some("time") {
            column = ctx.plan_templates.time_in_list_column_cast(&column)?;
            allocated = allocated
                .iter()
                .map(|v| ctx.plan_templates.time_in_list_param_cast(v))
                .collect::<Result<Vec<_>, _>>()?;
        }

        if self.negated {
            ctx.plan_templates
                .not_in_where(column, member_sql, allocated, need_null_check)
        } else {
            ctx.plan_templates
                .in_where(column, member_sql, allocated, need_null_check)
        }
    }
}
