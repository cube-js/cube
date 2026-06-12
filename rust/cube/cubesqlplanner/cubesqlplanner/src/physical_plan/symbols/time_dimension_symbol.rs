use super::{MemberSqlContext, ToSql};
use crate::planner::TimeDimensionSymbol;
use cubenativeutils::CubeError;

impl ToSql for TimeDimensionSymbol {
    fn to_sql(&self, ctx: &MemberSqlContext) -> Result<String, CubeError> {
        let visitor = ctx.visitor.with_ignore_tz_convert();
        visitor.apply(
            &self.base_symbol(),
            ctx.node_processor.clone(),
            ctx.templates,
        )
    }
}
