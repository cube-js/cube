use super::{FilterOperationSql, FilterSqlContext};
use crate::planner::filter::operators::in_list::InListOp;
use cubenativeutils::CubeError;

impl FilterOperationSql for InListOp {
    fn to_sql(&self, ctx: &FilterSqlContext) -> Result<String, CubeError> {
        let has_null = self.values.iter().any(|v| v.is_null());
        let need_null_check = if self.negated { !has_null } else { has_null };
        let mut allocated = ctx.allocate_and_cast_values(&self.values, &self.member_type)?;
        let mut column = ctx.member_sql().to_string();

        // Some dialects resolve a list of mixed types by comparing as strings, so
        // both sides of a time list are brought to the same type explicitly.
        if matches!(
            self.member_type.as_deref(),
            Some("time") | Some("time_text")
        ) {
            column = ctx.plan_templates.time_in_list_column_cast(&column)?;
            allocated = allocated
                .iter()
                .map(|v| ctx.plan_templates.time_in_list_param_cast(v))
                .collect::<Result<Vec<_>, _>>()?;
        }

        if self.member_type.as_deref() == Some("time") {
            if let Some(sql) = ctx.plan_templates.time_in_where(
                column.clone(),
                &allocated,
                need_null_check,
                self.negated,
            )? {
                return Ok(sql);
            }
        }

        // The null check reads the cast column too: an unparseable value casts to NULL
        // and is treated as not set, the same way by IN and NOT IN.
        if self.negated {
            ctx.plan_templates
                .not_in_where(column, allocated, need_null_check)
        } else {
            ctx.plan_templates
                .in_where(column, allocated, need_null_check)
        }
    }
}
