use super::MemberSymbol;
use crate::planner::sql_evaluator::Compiler;
use cubenativeutils::CubeError;
use std::rc::Rc;

pub trait SymbolFactory: Sized {
    fn build(self, compiler: &mut Compiler) -> Result<Rc<MemberSymbol>, CubeError>;
}
