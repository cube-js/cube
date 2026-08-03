use super::super::{MemberSqlContext, ToSql};
use crate::planner::symbols::dimension_kinds::SwitchDimension;
use cubenativeutils::CubeError;

impl ToSql for SwitchDimension {
    fn to_sql(&self, ctx: &MemberSqlContext) -> Result<String, CubeError> {
        if let Some(member_sql) = self.member_sql() {
            ctx.eval_sql_call(member_sql)
        } else {
            ctx.templates.quote_identifier(ctx.name)
        }
    }
}
