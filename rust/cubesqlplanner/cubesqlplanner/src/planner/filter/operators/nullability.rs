use super::{FilterOperationSql, FilterSqlContext};
use cubenativeutils::CubeError;

#[derive(Clone, Debug)]
pub struct NullabilityOp {
    negated: bool,
}

impl NullabilityOp {
    pub fn new(negated: bool) -> Self {
        Self { negated }
    }
}

impl FilterOperationSql for NullabilityOp {
    fn to_sql(&self, ctx: &FilterSqlContext) -> Result<String, CubeError> {
        if self.negated {
            ctx.plan_templates.not_set_where(ctx.member_sql.to_string())
        } else {
            ctx.plan_templates.set_where(ctx.member_sql.to_string())
        }
    }
}
