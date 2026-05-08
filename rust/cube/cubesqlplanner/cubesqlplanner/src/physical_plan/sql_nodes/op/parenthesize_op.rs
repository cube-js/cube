use crate::utils::sql_expression_scanner::is_top_level_compound;
use cubenativeutils::CubeError;

use super::{OpCtx, OpExec};

/// Protects a compound expression from operator-precedence breakage when it
/// is being substituted into a position that expects an atomic argument.
#[derive(Clone, Debug)]
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
