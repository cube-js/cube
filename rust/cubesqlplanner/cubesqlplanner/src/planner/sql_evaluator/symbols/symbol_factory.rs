use super::MemberSymbol;
use crate::cube_bridge::member_sql::MemberSql;
use crate::planner::sql_evaluator::Compiler;
use cubenativeutils::CubeError;
use std::rc::Rc;

pub trait SymbolFactory: Sized {
    fn symbol_name() -> String; //FIXME maybe Enum should be used
    fn is_cachable() -> bool {
        true
    }
    fn cube_name(&self) -> &String;
    fn deps_names(&self) -> Result<Vec<String>, CubeError>;
    fn member_sql(&self) -> Option<Rc<dyn MemberSql>>;
    fn build(self, compiler: &mut Compiler) -> Result<Rc<MemberSymbol>, CubeError>;
}
