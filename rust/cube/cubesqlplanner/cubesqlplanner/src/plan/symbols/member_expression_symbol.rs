use super::{MemberSqlContext, ToSql};
use crate::planner::sql_evaluator::{MemberExpressionExpression, MemberExpressionSymbol};
use cubenativeutils::CubeError;

impl ToSql for MemberExpressionSymbol {
    fn to_sql(&self, ctx: &MemberSqlContext) -> Result<String, CubeError> {
        let sql = match self.expression() {
            MemberExpressionExpression::SqlCall(sql_call) => ctx.eval_sql_call(sql_call)?,
            MemberExpressionExpression::PatchedSymbol(symbol) => {
                ctx.visitor
                    .apply(symbol, ctx.node_processor.clone(), ctx.templates)?
            }
        };
        if self.is_parenthesized() {
            Ok(format!("({})", sql))
        } else {
            Ok(sql)
        }
    }
}
