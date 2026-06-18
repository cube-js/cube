use super::{FilterOperationSql, FilterSqlContext};
use crate::planner::filter::operators::like::LikeOp;
use cubenativeutils::CubeError;

impl FilterOperationSql for LikeOp {
    fn to_sql(&self, ctx: &FilterSqlContext) -> Result<String, CubeError> {
        let allocated = ctx.allocate_and_cast_values(
            &self
                .values
                .iter()
                .map(|v| Some(v.clone()))
                .collect::<Vec<_>>(),
            &self.member_type,
        )?;

        let like_parts = allocated
            .into_iter()
            .map(|v| {
                ctx.plan_templates.ilike(
                    ctx.member_sql,
                    &v,
                    self.start_wild,
                    self.end_wild,
                    self.negated,
                )
            })
            .collect::<Result<Vec<_>, _>>()?;

        let logical_symbol = if self.negated { " AND " } else { " OR " };
        let need_null_check = if self.negated {
            !self.has_null
        } else {
            self.has_null
        };
        let null_check = if need_null_check {
            ctx.plan_templates
                .or_is_null_check(ctx.member_sql.to_string())?
        } else {
            "".to_string()
        };

        Ok(format!(
            "({}){}",
            like_parts.join(logical_symbol),
            null_check
        ))
    }
}
