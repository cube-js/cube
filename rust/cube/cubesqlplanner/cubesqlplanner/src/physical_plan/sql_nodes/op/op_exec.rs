use cubenativeutils::CubeError;

use super::{Op, OpCtx};

/// Behavior of a single op at render time. One impl per Op variant; the
/// dispatch on [`Op`] forwards to it.
pub trait OpExec {
    fn exec(&self, ctx: &mut OpCtx<'_>) -> Result<String, CubeError>;

    /// Whether this op terminates a pipeline — i.e. it never calls
    /// `render_tail`, so any ops after it in the same pipeline would be
    /// unreachable. A well-formed pipeline ends with exactly one terminal
    /// op; this is enforced by [`Op::validate_pipeline`].
    fn is_terminal(&self) -> bool {
        false
    }

    /// Sub-pipelines this op carries as data — branches of a kind dispatch,
    /// the input/else legs of a multi-stage window, etc. Default is empty:
    /// only branching ops override.
    fn nested_pipelines(&self) -> Vec<&[Op]> {
        Vec::new()
    }
}
