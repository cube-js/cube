use crate::cube_bridge::memeber_sql::MemberSql;
use crate::planner::sql_evaluator::dependecy::Dependency;
use crate::planner::sql_evaluator::{Compiler, EvaluationNode};
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
    fn build(
        self,
        deps: Vec<Dependency>,
        compiler: &mut Compiler,
    ) -> Result<Rc<EvaluationNode>, CubeError>;
}
