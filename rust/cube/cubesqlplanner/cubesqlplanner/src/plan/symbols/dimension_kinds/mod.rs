pub mod case_dimension;
pub mod regular;
pub mod switch;

use super::{MemberSqlContext, ToSql};
use crate::planner::sql_evaluator::DimensionKind;
use cubenativeutils::CubeError;

impl ToSql for DimensionKind {
    fn to_sql(&self, ctx: &MemberSqlContext) -> Result<String, CubeError> {
        match self {
            Self::Regular(r) => r.to_sql(ctx),
            Self::Geo(_) => Err(CubeError::internal(format!(
                "Geo dimension {} doesn't support evaluate_sql directly",
                ctx.full_name
            ))),
            Self::Switch(s) => s.to_sql(ctx),
            Self::Case(c) => c.to_sql(ctx),
        }
    }
}
