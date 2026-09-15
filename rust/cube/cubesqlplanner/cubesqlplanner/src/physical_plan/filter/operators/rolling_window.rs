use super::{FilterOperationSql, FilterSqlContext};
use crate::planner::filter::operators::rolling_window::{
    RegularRollingWindowOp, RollingWindowOffsetOp,
};
use crate::planner::time_dimension::shift_bound_wall_clock;
use cubenativeutils::CubeError;

impl FilterOperationSql for RegularRollingWindowOp {
    fn to_sql(&self, ctx: &FilterSqlContext) -> Result<String, CubeError> {
        // A derived span already carries the frame, so the bound needs no
        // interval around it: a bare literal is what an engine can both read
        // while planning and eliminate partitions by. Only the sub-select
        // fallback still applies the frame in SQL.
        let (from, to) = match ctx.date_range_literals(&self.scan_range)? {
            Some((from, to)) => (
                FilterSqlContext::keep_bounded(from, &self.trailing),
                FilterSqlContext::keep_bounded(to, &self.leading),
            ),
            None => {
                let (from, to) = ctx.date_range_from_time_series()?;
                (
                    ctx.extend_date_range_bound(from, &self.trailing, true)?,
                    ctx.extend_date_range_bound(to, &self.leading, false)?,
                )
            }
        };

        let date_field = ctx.convert_tz(ctx.member_sql())?;

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
        let member = ctx.member_sql().to_string();

        // Anchor: range start for offset 'start', range end for 'end'. Both
        // bounds are that anchor moved by the frame — folded in here rather
        // than applied to it in SQL, so the predicate is a bare comparison a
        // dialect has nothing left to fold. See `date_range_literals`.
        let anchor = if from_start {
            let from = self.from.as_deref().ok_or_else(|| {
                CubeError::internal("Rolling window date range is missing its start".to_string())
            })?;
            ctx.format_from_date(from)?
        } else {
            let to = self.to.as_deref().ok_or_else(|| {
                CubeError::internal("Rolling window date range is missing its end".to_string())
            })?;
            ctx.format_to_date(to)?
        };
        let tz = ctx.query_tools.timezone();

        // Both bounds are the anchor moved, so both are normalised the way the
        // anchor was: an end-of-day anchor shifted by whole days is another
        // end-of-day, and reading it as a range start would round its
        // sub-second tail down on a dialect that keeps more than milliseconds.
        let allocate = |bound: &str| {
            if from_start {
                ctx.format_and_allocate_from_date(bound)
            } else {
                ctx.format_and_allocate_to_date(bound)
            }
        };

        let mut conditions = Vec::new();

        // trailing side -> lower bound; leading side -> upper bound.
        // Shifted on the wall clock first, then carried into the database's
        // timezone — this operator compares an unconverted member.
        if let Some(bound) = shift_bound_wall_clock(tz, &anchor, &self.trailing, true)? {
            let bound = allocate(&bound)?;
            conditions.push(if from_start {
                ctx.plan_templates.gte(member.clone(), bound)?
            } else {
                ctx.plan_templates.gt(member.clone(), bound)?
            });
        }

        if let Some(bound) = shift_bound_wall_clock(tz, &anchor, &self.leading, false)? {
            let bound = allocate(&bound)?;
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
