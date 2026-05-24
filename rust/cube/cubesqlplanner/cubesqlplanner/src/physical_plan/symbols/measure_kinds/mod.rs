pub mod aggregated;
pub mod calculated;
pub mod count;

use super::{MemberSqlContext, ToSql};
use crate::planner::MeasureKind;
use cubenativeutils::CubeError;

impl ToSql for MeasureKind {
    fn to_sql(&self, ctx: &MemberSqlContext) -> Result<String, CubeError> {
        match self {
            Self::Count(c) => c.to_sql(ctx),
            Self::Aggregated(a) => a.to_sql(ctx),
            Self::Calculated(c) => c.to_sql(ctx),
            Self::Rank => Err(CubeError::internal(format!(
                "Rank measure doesn't support direct evaluation for {}",
                ctx.full_name
            ))),
        }
    }
}
