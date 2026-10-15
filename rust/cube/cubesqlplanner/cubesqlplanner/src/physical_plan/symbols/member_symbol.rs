use super::{MemberSqlContext, ToSql};
use crate::planner::{MemberSymbol, RefTarget};
use cubenativeutils::CubeError;

impl ToSql for MemberSymbol {
    fn to_sql(&self, ctx: &MemberSqlContext) -> Result<String, CubeError> {
        match self {
            Self::Dimension(d) => d.to_sql(ctx),
            Self::TimeDimension(t) => t.to_sql(ctx),
            Self::Measure(m) => m.to_sql(ctx),
            Self::MemberExpression(e) => e.to_sql(ctx),
            // Rendered like a direct-reference `SqlCall`: the target gets no
            // operator context; wrapping is left to the chain around the ref.
            Self::Ref(r) => match r.target() {
                RefTarget::Member(target) => ctx.visitor.with_arg_needs_paren_safe(false).apply(
                    target,
                    ctx.node_processor.clone(),
                    ctx.templates,
                ),
            },
        }
    }
}
