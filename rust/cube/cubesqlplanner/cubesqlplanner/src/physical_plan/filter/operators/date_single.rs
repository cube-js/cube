use super::{FilterOperationSql, FilterSqlContext};
use crate::planner::filter::operators::date_single::{DateSingleKind, DateSingleOp};
use cubenativeutils::CubeError;

impl FilterOperationSql for DateSingleOp {
    fn to_sql(&self, ctx: &FilterSqlContext) -> Result<String, CubeError> {
        match self.kind {
            DateSingleKind::Before | DateSingleKind::AfterOrOn => {
                let param = ctx.format_and_allocate_from_date(&self.value)?;
                match self.kind {
                    DateSingleKind::Before => {
                        ctx.plan_templates.lt(ctx.member_sql.to_string(), param)
                    }
                    DateSingleKind::AfterOrOn => {
                        ctx.plan_templates.gte(ctx.member_sql.to_string(), param)
                    }
                    _ => unreachable!(),
                }
            }
            DateSingleKind::BeforeOrOn | DateSingleKind::After => {
                let param = ctx.format_and_allocate_to_date(&self.value)?;
                match self.kind {
                    DateSingleKind::BeforeOrOn => {
                        ctx.plan_templates.lte(ctx.member_sql.to_string(), param)
                    }
                    DateSingleKind::After => {
                        ctx.plan_templates.gt(ctx.member_sql.to_string(), param)
                    }
                    _ => unreachable!(),
                }
            }
        }
    }
}
