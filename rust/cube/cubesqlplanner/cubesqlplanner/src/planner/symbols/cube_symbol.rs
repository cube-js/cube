use crate::cube_bridge::cube_definition::CubeDefinition;
use crate::cube_bridge::evaluator::CubeEvaluator;
use crate::cube_bridge::member_sql::MemberSql;
use crate::physical_plan::sql_nodes::SqlNode;
use crate::physical_plan::SqlEvaluatorVisitor;
use crate::planner::query_tools::QueryTools;
use crate::planner::sql_templates::PlanSqlTemplates;
use crate::planner::{Compiler, CubeId, SqlCall};
use cubenativeutils::CubeError;
use lazy_static::lazy_static;
use regex::Regex;
use std::rc::Rc;

/// Symbol for a cube referenced as an identifier (`{CUBE}` /
/// `{TABLE}`); renders to the cube's quoted name or alias.
#[derive(Debug)]
pub struct CubeNameSymbol {
    cube_name: CubeId,
    path: Vec<CubeId>,
}

impl CubeNameSymbol {
    pub fn new(cube_name: CubeId, path: Vec<CubeId>) -> Rc<Self> {
        let path = Self::normalize_path(path, &cube_name);
        Rc::new(Self { cube_name, path })
    }

    pub(crate) fn normalize_path(mut path: Vec<CubeId>, cube_name: &CubeId) -> Vec<CubeId> {
        if path.last() != Some(cube_name) {
            path.push(cube_name.clone());
        }
        path
    }

    pub fn evaluate_sql(&self) -> Result<String, CubeError> {
        Ok(self.cube_name.to_string())
    }
    pub fn cube_name(&self) -> &CubeId {
        &self.cube_name
    }
    pub fn path(&self) -> &Vec<CubeId> {
        &self.path
    }
    pub fn alias(&self) -> String {
        PlanSqlTemplates::alias_name(&self.cube_name.to_string())
    }
}

pub struct CubeNameSymbolFactory {
    cube_name: CubeId,
    path: Vec<CubeId>,
}

impl CubeNameSymbolFactory {
    pub fn try_new(
        full_name: &CubeId,
        _cube_evaluator: Rc<dyn CubeEvaluator>,
        path: Vec<CubeId>,
    ) -> Result<Self, CubeError> {
        //TODO check that cube exists
        Ok(Self {
            cube_name: full_name.clone(),
            path,
        })
    }
}

impl CubeNameSymbolFactory {
    pub fn build(self, _compiler: &mut Compiler) -> Result<Rc<CubeNameSymbol>, CubeError> {
        let Self { cube_name, path } = self;
        Ok(CubeNameSymbol::new(cube_name, path))
    }
}

/// Symbol for a cube referenced as a table expression
/// (`{CUBE.sql()}`); renders by evaluating the cube's `sql:`
/// function or its raw `sql_table:` value.
#[derive(Debug)]
pub struct CubeTableSymbol {
    cube_name: CubeId,
    path: Vec<CubeId>,
    member_sql: Option<Rc<SqlCall>>,
    alias: String,
    is_table_sql: bool,
    join_map: Option<Vec<Vec<String>>>,
}

impl CubeTableSymbol {
    pub fn new(
        cube_name: CubeId,
        path: Vec<CubeId>,
        member_sql: Option<Rc<SqlCall>>,
        alias: String,
        is_table_sql: bool,
        join_map: Option<Vec<Vec<String>>>,
    ) -> Rc<Self> {
        let path = CubeNameSymbol::normalize_path(path, &cube_name);
        Rc::new(Self {
            cube_name,
            path,
            member_sql,
            alias,
            is_table_sql,
            join_map,
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
    pub fn cube_name(&self) -> &CubeId {
        &self.cube_name
    }

    pub fn path(&self) -> &Vec<CubeId> {
        &self.path
    }

    pub fn alias(&self) -> String {
        self.alias.clone()
    }

    pub fn join_map(&self) -> &Option<Vec<Vec<String>>> {
        &self.join_map
    }
}

pub struct CubeTableSymbolFactory {
    cube_name: CubeId,
    path: Vec<CubeId>,
    sql: Option<Rc<dyn MemberSql>>,
    definition: Rc<dyn CubeDefinition>,
    is_table_sql: bool,
}

impl CubeTableSymbolFactory {
    pub fn try_new(
        cube_name: &CubeId,
        cube_evaluator: Rc<dyn CubeEvaluator>,
        path: Vec<CubeId>,
    ) -> Result<Self, CubeError> {
        let definition = cube_evaluator.cube_from_path(cube_name.target().to_string())?;
        let table_sql = definition.sql_table()?;
        let is_table_sql = table_sql.is_some();
        let sql = definition.sql()?;
        let sql = table_sql.or(sql);
        Ok(Self {
            cube_name: cube_name.clone(),
            path,
            sql,
            definition,
            is_table_sql,
        })
    }

    pub fn build(self, compiler: &mut Compiler) -> Result<Rc<CubeTableSymbol>, CubeError> {
        let Self {
            cube_name,
            path,
            sql,
            definition,
            is_table_sql,
        } = self;
        let sql = if let Some(sql) = sql {
            Some(compiler.compile_cube_sql_call(&cube_name, sql)?)
        } else {
            None
        };
        let alias = if let Some(alias) = definition.static_data().sql_alias.clone() {
            alias.clone()
        } else {
            PlanSqlTemplates::alias_name(&cube_name.to_string())
        };
        Ok(CubeTableSymbol::new(
            cube_name,
            path,
            sql,
            alias,
            is_table_sql,
            definition.static_data().join_map.clone(),
        ))
    }
}

impl crate::utils::debug::DebugSql for CubeNameSymbol {
    fn debug_sql(&self, _expand_deps: bool) -> String {
        self.cube_name().to_string()
    }
}

impl crate::utils::debug::DebugSql for CubeTableSymbol {
    fn debug_sql(&self, expand_deps: bool) -> String {
        let sql_debug = if let Some(sql) = &self.member_sql {
            sql.debug_sql(expand_deps)
        } else {
            "NULL".to_string()
        };
        format!("{}({})", self.cube_name(), sql_debug)
    }
}
