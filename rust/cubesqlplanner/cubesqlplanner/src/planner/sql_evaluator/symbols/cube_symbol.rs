use super::{MemberSymbol, SymbolFactory};
use crate::cube_bridge::cube_definition::CubeDefinition;
use crate::cube_bridge::evaluator::CubeEvaluator;
use crate::cube_bridge::member_sql::MemberSql;
use crate::planner::query_tools::QueryTools;
use crate::planner::sql_evaluator::{sql_nodes::SqlNode, Compiler, SqlCall, SqlEvaluatorVisitor};
use crate::planner::sql_templates::PlanSqlTemplates;
use cubenativeutils::CubeError;
use lazy_static::lazy_static;
use regex::Regex;
use std::rc::Rc;

pub struct CubeNameSymbol {
    cube_name: String,
}

impl CubeNameSymbol {
    pub fn new(cube_name: String) -> Rc<Self> {
        Rc::new(Self { cube_name })
    }

    pub fn evaluate_sql(&self) -> Result<String, CubeError> {
        Ok(self.cube_name.clone())
    }
    pub fn cube_name(&self) -> &String {
        &self.cube_name
    }
    pub fn alias(&self) -> String {
        PlanSqlTemplates::alias_name(&self.cube_name)
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

    fn build(self, _compiler: &mut Compiler) -> Result<Rc<MemberSymbol>, CubeError> {
        let Self { cube_name } = self;
        Ok(MemberSymbol::new_cube_name(CubeNameSymbol::new(cube_name)))
    }
}

pub struct CubeTableSymbol {
    cube_name: String,
    member_sql: Option<Rc<SqlCall>>,
    #[allow(dead_code)]
    definition: Rc<dyn CubeDefinition>,
    is_table_sql: bool,
}

impl CubeTableSymbol {
    pub fn new(
        cube_name: String,
        member_sql: Option<Rc<SqlCall>>,
        definition: Rc<dyn CubeDefinition>,
        is_table_sql: bool,
    ) -> Rc<Self> {
        Rc::new(Self {
            cube_name,
            member_sql,
            definition,
            is_table_sql,
        })
    }

    pub fn evaluate_sql(
        &self,
        visitor: &SqlEvaluatorVisitor,
        node_processor: Rc<dyn SqlNode>,
        query_tools: Rc<QueryTools>,
        templates: &PlanSqlTemplates,
    ) -> Result<String, CubeError> {
        if let Some(member_sql) = &self.member_sql {
            lazy_static! {
                static ref SIMPLE_ASTERIX_RE: Regex =
                    Regex::new(r#"(?i)^\s*select\s+\*\s+from\s+([a-zA-Z0-9_\-`".*]+)\s*$"#)
                        .unwrap();
            }
            let sql = member_sql.eval(visitor, node_processor, query_tools, templates)?;
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
        } else {
            Err(CubeError::internal(format!(
                "Cube {} doesn't have sql evaluator",
                self.cube_name
            )))
        }
    }
    pub fn cube_name(&self) -> &String {
        &self.cube_name
    }

    pub fn alias(&self) -> String {
        PlanSqlTemplates::alias_name(&self.cube_name)
    }
}

pub struct CubeTableSymbolFactory {
    cube_name: String,
    sql: Option<Rc<dyn MemberSql>>,
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
        let sql = table_sql.or(sql);
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
        Ok(self
            .sql
            .as_ref()
            .map_or_else(|| vec![], |sql| sql.args_names().clone()))
    }

    fn member_sql(&self) -> Option<Rc<dyn MemberSql>> {
        self.sql.clone()
    }

    fn build(self, compiler: &mut Compiler) -> Result<Rc<MemberSymbol>, CubeError> {
        let Self {
            cube_name,
            sql,
            definition,
            is_table_sql,
        } = self;
        let sql = if let Some(sql) = sql {
            Some(compiler.compile_sql_call(&cube_name, sql)?)
        } else {
            None
        };
        Ok(MemberSymbol::new_cube_table(CubeTableSymbol::new(
            cube_name,
            sql,
            definition,
            is_table_sql,
        )))
    }
}
