use super::SymbolFactory;
use crate::cube_bridge::memeber_sql::{MemberSql, MemberSqlArg};
use crate::planner::sql_evaluator::{Compiler, Dependency, EvaluationNode};
use cubenativeutils::CubeError;
use std::rc::Rc;

pub struct SimpleSqlSymbol {
    cube_name: String,
    member_sql: Rc<dyn MemberSql>,
}

impl SimpleSqlSymbol {
    pub fn new(cube_name: String, member_sql: Rc<dyn MemberSql>) -> Self {
        Self {
            cube_name,
            member_sql,
        }
    }

    pub fn full_name(&self) -> String {
        format!("{}.simple_sql", self.cube_name)
    }

    pub fn evaluate_sql(&self, args: Vec<MemberSqlArg>) -> Result<String, CubeError> {
        let sql = self.member_sql.call(args)?;
        Ok(sql)
    }
}

pub struct SimpleSqlSymbolFactory {
    cube_name: String,
    sql: Rc<dyn MemberSql>,
}

impl SimpleSqlSymbolFactory {
    pub fn try_new(cube_name: &String, sql: Rc<dyn MemberSql>) -> Result<Self, CubeError> {
        Ok(Self {
            cube_name: cube_name.clone(),
            sql,
        })
    }
}

impl SymbolFactory for SimpleSqlSymbolFactory {
    fn is_cachable() -> bool {
        false
    }
    fn symbol_name() -> String {
        "simple_sql".to_string()
    }
    fn cube_name(&self) -> &String {
        &self.cube_name
    }

    fn member_sql(&self) -> Option<Rc<dyn MemberSql>> {
        Some(self.sql.clone())
    }

    fn deps_names(&self) -> Result<Vec<String>, CubeError> {
        Ok(self.sql.args_names().clone())
    }

    fn build(
        self,
        deps: Vec<Dependency>,
        _compiler: &mut Compiler,
    ) -> Result<Rc<EvaluationNode>, CubeError> {
        let Self { cube_name, sql } = self;
        Ok(EvaluationNode::new_simple_sql(
            SimpleSqlSymbol::new(cube_name, sql),
            deps,
        ))
    }
}
