use cubenativeutils::CubeError;

use super::OpCtx;

pub trait OpExec {
    fn exec(&self, ctx: &mut OpCtx<'_>) -> Result<String, CubeError>;

    /// Whether this op terminates a pipeline — i.e. it never calls
    /// `render_tail`, so any ops after it in the same pipeline would be
    /// unreachable. A well-formed pipeline ends with exactly one terminal
    /// op; this is enforced by [`Op::validate_pipeline`].
    fn is_terminal(&self) -> bool {
        false
    }
}
