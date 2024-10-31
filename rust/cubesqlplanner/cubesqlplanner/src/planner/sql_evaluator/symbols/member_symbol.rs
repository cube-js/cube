use crate::cube_bridge::memeber_sql::MemberSql;
use crate::planner::sql_evaluator::dependecy::Dependency;
use crate::planner::sql_evaluator::{Compiler, EvaluationNode};
use cubenativeutils::CubeError;
use std::rc::Rc;

pub trait MemberSymbol {
    fn cube_name(&self) -> &String;
}
