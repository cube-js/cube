use super::{MemberSqlContext, ToSql};
use crate::planner::sql_evaluator::MemberSymbol;
use cubenativeutils::CubeError;

impl ToSql for MemberSymbol {
    fn to_sql(&self, ctx: &MemberSqlContext) -> Result<String, CubeError> {
        match self {
            Self::Dimension(d) => d.to_sql(ctx),
            Self::TimeDimension(t) => t.to_sql(ctx),
            Self::Measure(m) => m.to_sql(ctx),
            Self::MemberExpression(e) => e.to_sql(ctx),
        }
    }
}
