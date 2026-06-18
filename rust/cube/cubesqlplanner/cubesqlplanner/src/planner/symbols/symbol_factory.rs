use super::MemberSymbol;
use crate::planner::Compiler;
use cubenativeutils::CubeError;
use std::rc::Rc;

/// Builds a `MemberSymbol` from a `SymbolPath`. Implementations
/// hold the data-model definition; `build` runs through the
/// `Compiler` to resolve any dependencies.
pub trait SymbolFactory: Sized {
    fn build(self, compiler: &mut Compiler) -> Result<Rc<MemberSymbol>, CubeError>;
}
