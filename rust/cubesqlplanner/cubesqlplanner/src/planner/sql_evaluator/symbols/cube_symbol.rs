use super::SymbolFactory;
use crate::cube_bridge::cube_definition::CubeDefinition;
use crate::cube_bridge::evaluator::CubeEvaluator;
use crate::cube_bridge::memeber_sql::{MemberSql, MemberSqlArg};
use crate::planner::query_tools::QueryTools;
use crate::planner::sql_evaluator::{
    dependecy::Dependency, Compiler, EvaluationNode, SqlEvaluatorVisitor,
};
use cubenativeutils::CubeError;
use lazy_static::lazy_static;
use regex::Regex;
use std::rc::Rc;

pub struct CubeNameSymbol {
    cube_name: String,
}

impl CubeNameSymbol {
    pub fn new(cube_name: String) -> Self {
        Self { cube_name }
    }

    pub fn evaluate_sql(&self, _args: Vec<MemberSqlArg>) -> Result<String, CubeError> {
        Ok(self.cube_name.clone())
    }
    pub fn cube_name(&self) -> &String {
        &self.cube_name
    }
}

pub struct CubeNameSymbolFactory {
    cube_name: String,
}

impl CubeNameSymbolFactory {
    pub fn try_new(
        full_name: &String,
        _cube_evaluator: Rc<dyn CubeEvaluator>,
    ) -> Result<Self, CubeError> {
        //TODO check that cube exists
        Ok(Self {
            cube_name: full_name.clone(),
        })
    }
}

impl SymbolFactory for CubeNameSymbolFactory {
    fn symbol_name() -> String {
        "cube_name".to_string()
    }

    fn cube_name(&self) -> &String {
        &self.cube_name
    }

    fn member_sql(&self) -> Option<Rc<dyn MemberSql>> {
        None
    }

    fn deps_names(&self) -> Result<Vec<String>, CubeError> {
        Ok(vec![])
    }

    fn build(
        self,
        _deps: Vec<Dependency>,
        _compiler: &mut Compiler,
    ) -> Result<Rc<EvaluationNode>, CubeError> {
        let Self { cube_name } = self;
        Ok(EvaluationNode::new_cube_name(CubeNameSymbol::new(
            cube_name,
        )))
    }
}

pub struct CubeTableSymbol {
    cube_name: String,
    member_sql: Rc<dyn MemberSql>,
    #[allow(dead_code)]
    definition: Rc<dyn CubeDefinition>,
    is_table_sql: bool,
}

impl CubeTableSymbol {
    pub fn new(
        cube_name: String,
        member_sql: Rc<dyn MemberSql>,
        definition: Rc<dyn CubeDefinition>,
        is_table_sql: bool,
    ) -> Self {
        Self {
            cube_name,
            member_sql,
            definition,
            is_table_sql,
        }
    }
    pub fn evaluate_sql(&self, args: Vec<MemberSqlArg>) -> Result<String, CubeError> {
        lazy_static! {
            static ref SIMPLE_ASTERIX_RE: Regex =
                Regex::new(r#"(?i)^\s*select\s+\*\s+from\s+([a-zA-Z0-9_\-`".*]+)\s*$"#).unwrap();
        }
        let sql = self.member_sql.call(args)?;
        let res = if self.is_table_sql {
            sql
        } else {
            if let Some(captures) = SIMPLE_ASTERIX_RE.captures(&sql) {
                if let Some(table) = captures.get(1) {
                    table.as_str().to_owned()
                } else {
                    format!("({})", sql)
                }
            } else {
                format!("({})", sql)
            }
        };
        Ok(res)
    }
    pub fn cube_name(&self) -> &String {
        &self.cube_name
    }
}

pub struct CubeTableSymbolFactory {
    cube_name: String,
    sql: Rc<dyn MemberSql>,
    definition: Rc<dyn CubeDefinition>,
    is_table_sql: bool,
}

impl CubeTableSymbolFactory {
    pub fn try_new(
        cube_name: &String,
        cube_evaluator: Rc<dyn CubeEvaluator>,
    ) -> Result<Self, CubeError> {
        let definition = cube_evaluator.cube_from_path(cube_name.clone())?;
        let table_sql = definition.sql_table()?;
        let is_table_sql = table_sql.is_some();
        let sql = definition.sql()?;
        let sql = if let Some(sql) = table_sql.or(sql) {
            sql
        } else {
            return Err(CubeError::user(format!(
                "Cube {} sould have sql or sqlTable field",
                cube_name
            )));
        };

        Ok(Self {
            cube_name: cube_name.clone(),
            sql,
            definition,
            is_table_sql,
        })
    }
}

impl SymbolFactory for CubeTableSymbolFactory {
    fn symbol_name() -> String {
        "cube_table".to_string()
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
        let Self {
            cube_name,
            sql,
            definition,
            is_table_sql,
        } = self;
        Ok(EvaluationNode::new_cube_table(
            CubeTableSymbol::new(cube_name, sql, definition, is_table_sql),
            deps,
        ))
    }
}
