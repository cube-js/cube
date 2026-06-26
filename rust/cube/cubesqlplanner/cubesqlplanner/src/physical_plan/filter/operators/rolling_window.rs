use super::{FilterOperationSql, FilterSqlContext};
use crate::planner::filter::operators::rolling_window::{
    RegularRollingWindowOp, RollingWindowOffsetOp,
};
use cubenativeutils::CubeError;

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

impl FilterOperationSql for RollingWindowOffsetOp {
    fn to_sql(&self, ctx: &FilterSqlContext) -> Result<String, CubeError> {
        let from_start = self.offset == "start";
        let member = ctx.member_sql.to_string();

        // Anchor: range start (formatted to start-of-day) for offset 'start',
        // range end (formatted to end-of-day) for 'end'. Both bounds share it.
        let anchor = if from_start {
            let from = self.from.as_deref().ok_or_else(|| {
                CubeError::internal("Rolling window date range is missing its start".to_string())
            })?;
            ctx.format_and_allocate_from_date(from)?
        } else {
            let to = self.to.as_deref().ok_or_else(|| {
                CubeError::internal("Rolling window date range is missing its end".to_string())
            })?;
            ctx.format_and_allocate_to_date(to)?
        };

        let mut conditions = Vec::new();

        // trailing side -> lower bound (shifted back by the trailing interval;
        // `unbounded` drops the bound, `None` keeps the anchor unshifted).
        if let Some(bound) = ctx.extend_date_range_bound(anchor.clone(), &self.trailing, true)? {
            conditions.push(if from_start {
                ctx.plan_templates.gte(member.clone(), bound)?
            } else {
                ctx.plan_templates.gt(member.clone(), bound)?
            });
        }

        // leading side -> upper bound (shifted forward by the leading interval).
        if let Some(bound) = ctx.extend_date_range_bound(anchor.clone(), &self.leading, false)? {
            conditions.push(if from_start {
                ctx.plan_templates.lt(member.clone(), bound)?
            } else {
                ctx.plan_templates.lte(member.clone(), bound)?
            });
        }

        if conditions.is_empty() {
            ctx.plan_templates.always_true()
        } else {
            Ok(conditions.join(" AND "))
        }
    }
}
