use cubenativeutils::CubeError;

use super::OpCtx;

pub trait OpExec {
    fn exec(&self, ctx: &mut OpCtx<'_>) -> Result<String, CubeError>;
}
