use super::{MemberSymbol, MemberSymbolFactory};
use crate::cube_bridge::memeber_sql::{MemberSql, MemberSqlArg};
use crate::planner::sql_evaluator::{Compiler, Dependency, EvaluationNode};
use cubenativeutils::CubeError;
use std::rc::Rc;

pub struct JoinConditionSymbol {
    cube_name: String,
    member_sql: Rc<dyn MemberSql>,
}

impl JoinConditionSymbol {
    pub fn new(cube_name: String, member_sql: Rc<dyn MemberSql>) -> Self {
        Self {
            cube_name,
            member_sql,
        }
    }
    pub fn evaluate_sql(&self, args: Vec<MemberSqlArg>) -> Result<String, CubeError> {
        self.member_sql.call(args)
    }
}

impl MemberSymbol for JoinConditionSymbol {
    fn cube_name(&self) -> &String {
        &self.cube_name
    }
}

pub struct JoinConditionSymbolFactory {
    cube_name: String,
    sql: Rc<dyn MemberSql>,
}

impl JoinConditionSymbolFactory {
    pub fn try_new(cube_name: &String, sql: Rc<dyn MemberSql>) -> Result<Self, CubeError> {
        Ok(Self {
            cube_name: cube_name.clone(),
            sql,
        })
    }
}

impl MemberSymbolFactory for JoinConditionSymbolFactory {
    fn symbol_name() -> String {
        "join".to_string()
    }

    fn is_cachable() -> bool {
        false
    }

    fn cube_name(&self) -> &String {
        &self.cube_name
    }

    fn deps_names(&self) -> Result<Vec<String>, CubeError> {
        Ok(self.sql.args_names().clone())
    }

    fn member_sql(&self) -> Option<Rc<dyn MemberSql>> {
        Some(self.sql.clone())
    }

    fn build(
        self,
        deps: Vec<Dependency>,
        _compiler: &mut Compiler,
    ) -> Result<Rc<EvaluationNode>, CubeError> {
        let Self { cube_name, sql } = self;
        Ok(EvaluationNode::new_join_condition(
            JoinConditionSymbol::new(cube_name, sql),
            deps,
        ))
    }
}
