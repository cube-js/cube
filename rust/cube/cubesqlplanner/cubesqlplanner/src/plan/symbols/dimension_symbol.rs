use super::{MemberSqlContext, ToSql};
use crate::planner::sql_evaluator::DimensionSymbol;
use cubenativeutils::CubeError;

impl ToSql for DimensionSymbol {
    fn to_sql(&self, ctx: &MemberSqlContext) -> Result<String, CubeError> {
        self.kind().to_sql(ctx)
    }
}
