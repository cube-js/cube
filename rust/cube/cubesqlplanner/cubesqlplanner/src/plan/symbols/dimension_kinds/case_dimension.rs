use super::super::{MemberSqlContext, ToSql};
use crate::planner::sql_evaluator::symbols::dimension_kinds::CaseDimension;
use cubenativeutils::CubeError;

impl ToSql for CaseDimension {
    fn to_sql(&self, ctx: &MemberSqlContext) -> Result<String, CubeError> {
        if let Some(member_sql) = self.member_sql() {
            ctx.eval_sql_call(member_sql)
        } else {
            Err(CubeError::internal(format!(
                "Dimension {} has no sql evaluator",
                ctx.full_name
            )))
        }
    }
}
