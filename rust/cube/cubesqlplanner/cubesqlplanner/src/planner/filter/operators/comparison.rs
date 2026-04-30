use super::{FilterOperationSql, FilterSqlContext};
use cubenativeutils::CubeError;

#[derive(Clone, Debug)]
pub enum ComparisonKind {
    Gt,
    Gte,
    Lt,
    Lte,
}

#[derive(Clone, Debug)]
pub struct ComparisonOp {
    kind: ComparisonKind,
    value: String,
    member_type: Option<String>,
}

impl ComparisonOp {
    pub fn new(kind: ComparisonKind, value: String, member_type: Option<String>) -> Self {
        Self {
            kind,
            value,
            member_type,
        }
    }
}

impl FilterOperationSql for ComparisonOp {
    fn to_sql(&self, ctx: &FilterSqlContext) -> Result<String, CubeError> {
        let param = ctx.allocate_and_cast(&self.value, &self.member_type)?;
        match self.kind {
            ComparisonKind::Gt => ctx.plan_templates.gt(ctx.member_sql.to_string(), param),
            ComparisonKind::Gte => ctx.plan_templates.gte(ctx.member_sql.to_string(), param),
            ComparisonKind::Lt => ctx.plan_templates.lt(ctx.member_sql.to_string(), param),
            ComparisonKind::Lte => ctx.plan_templates.lte(ctx.member_sql.to_string(), param),
        }
    }
}
