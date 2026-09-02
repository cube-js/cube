use super::super::{MemberSqlContext, ToSql};
use crate::planner::symbols::dimension_kinds::RegularDimension;
use cubenativeutils::CubeError;

impl ToSql for RegularDimension {
    fn to_sql(&self, ctx: &MemberSqlContext) -> Result<String, CubeError> {
        ctx.eval_sql_call(self.member_sql())
    }
}
