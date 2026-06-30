use super::{MemberSqlContext, ToSql};
use crate::planner::MeasureSymbol;
use cubenativeutils::CubeError;

impl ToSql for MeasureSymbol {
    fn to_sql(&self, ctx: &MemberSqlContext) -> Result<String, CubeError> {
        self.kind().to_sql(ctx)
    }
}
