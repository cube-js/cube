use super::{FilterOperationSql, FilterSqlContext};
use cubenativeutils::CubeError;

#[derive(Clone, Debug)]
pub enum DateSingleKind {
    Before,
    BeforeOrOn,
    After,
    AfterOrOn,
}

#[derive(Clone, Debug)]
pub struct DateSingleOp {
    kind: DateSingleKind,
    value: String,
}

impl DateSingleOp {
    pub fn new(kind: DateSingleKind, value: String) -> Self {
        Self { kind, value }
    }
}

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
