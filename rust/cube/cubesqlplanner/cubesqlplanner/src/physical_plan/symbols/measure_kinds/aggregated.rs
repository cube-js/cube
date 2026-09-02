use super::super::{MemberSqlContext, ToSql};
use crate::planner::symbols::measure_kinds::AggregatedMeasure;
use cubenativeutils::CubeError;

impl ToSql for AggregatedMeasure {
    fn to_sql(&self, ctx: &MemberSqlContext) -> Result<String, CubeError> {
        match self.member_sql() {
            Some(sql) => ctx.eval_sql_call(sql),
            None => Err(CubeError::internal(
                "Aggregated measure without sql cannot be evaluated directly".to_string(),
            )),
        }
    }
}
