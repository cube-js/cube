use crate::utils::sql_expression_scanner::is_top_level_compound;
use cubenativeutils::CubeError;

use super::{OpCtx, OpExec};

/// Wraps the rendered SQL of the rest of the pipeline in parentheses when
/// the visitor signals `arg_needs_paren_safe` and the inner expression is
/// compound at the top level.
///
/// Mirrors the legacy `ParenthesizeSqlNode`.
pub struct ParenthesizeOp;

impl OpExec for ParenthesizeOp {
    fn exec(&self, ctx: &mut OpCtx<'_>) -> Result<String, CubeError> {
        let needs_paren = ctx.visitor.arg_needs_paren_safe();
        let input_sql = ctx.render_tail()?;
        if needs_paren && is_top_level_compound(&input_sql) {
            Ok(format!("({})", input_sql))
        } else {
            Ok(input_sql)
        }
    }
}
