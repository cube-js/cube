use super::{MemberSymbol, SymbolFactory};
use crate::cube_bridge::dimension_definition::DimensionDefinition;
use crate::cube_bridge::evaluator::CubeEvaluator;
use crate::cube_bridge::memeber_sql::{MemberSql, MemberSqlArg};
use crate::planner::query_tools::QueryTools;
use crate::planner::sql_evaluator::{Compiler, Dependency, EvaluationNode, SqlEvaluatorVisitor};
use cubenativeutils::CubeError;
use std::rc::Rc;

pub struct DimensionSymbol {
    cube_name: String,
    name: String,
    member_sql: Rc<dyn MemberSql>,
    #[allow(dead_code)]
    definition: Rc<dyn DimensionDefinition>,
}

impl DimensionSymbol {
    pub fn new(
        cube_name: String,
        name: String,
        member_sql: Rc<dyn MemberSql>,
        definition: Rc<dyn DimensionDefinition>,
    ) -> Self {
        Self {
            cube_name,
            name,
            member_sql,
            definition,
        }
    }
    pub fn evaluate_sql(&self, args: Vec<MemberSqlArg>) -> Result<String, CubeError> {
        let sql = self.member_sql.call(args)?;
        Ok(sql)
    }
    pub fn default_evaluate_sql(
        &self,
        visitor: &SqlEvaluatorVisitor,
        args: Vec<MemberSqlArg>,
        tools: Rc<QueryTools>,
    ) -> Result<String, CubeError> {
        let sql = tools.auto_prefix_with_cube_name(
            &self.cube_name,
            &self.member_sql.call(args)?,
            visitor.cube_alias_prefix(),
        );
        Ok(sql)
    }

    pub fn full_name(&self) -> String {
        format!("{}.{}", self.cube_name, self.name)
    }

    pub fn owned_by_cube(&self) -> bool {
        self.definition.static_data().owned_by_cube.unwrap_or(true)
    }

    pub fn is_multi_stage(&self) -> bool {
        self.definition.static_data().multi_stage.unwrap_or(false)
    }
}

impl MemberSymbol for DimensionSymbol {
    fn cube_name(&self) -> &String {
        &self.cube_name
    }
}

pub struct DimensionSymbolFactory {
    cube_name: String,
    name: String,
    sql: Rc<dyn MemberSql>,
    definition: Rc<dyn DimensionDefinition>,
}

impl DimensionSymbolFactory {
    pub fn try_new(
        full_name: &String,
        cube_evaluator: Rc<dyn CubeEvaluator>,
    ) -> Result<Self, CubeError> {
        let mut iter = cube_evaluator
            .parse_path("dimensions".to_string(), full_name.clone())?
            .into_iter();
        let cube_name = iter.next().unwrap();
        let name = iter.next().unwrap();
        let definition = cube_evaluator.dimension_by_path(full_name.clone())?;
        Ok(Self {
            cube_name,
            name,
            sql: definition.sql()?,
            definition,
        })
    }
}

impl SymbolFactory for DimensionSymbolFactory {
    fn symbol_name() -> String {
        "dimension".to_string()
    }

    fn cube_name(&self) -> &String {
        &self.cube_name
    }

    fn deps_names(&self) -> Result<Vec<String>, CubeError> {
        Ok(self.definition.sql()?.args_names().clone())
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
            name,
            sql,
            definition,
        } = self;
        Ok(EvaluationNode::new_dimension(
            DimensionSymbol::new(cube_name, name, sql, definition),
            deps,
        ))
    }
}
