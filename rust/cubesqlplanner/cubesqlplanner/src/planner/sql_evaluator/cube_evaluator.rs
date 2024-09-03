use super::dependecy::Dependency;
use super::{default_visitor::DefaultEvaluatorVisitor, EvaluationNode, MemberEvaluatorType};
use super::{MemberEvaluator, MemberEvaluatorFactory};
use crate::cube_bridge::cube_definition::CubeDefinition;
use crate::cube_bridge::dimension_definition::DimensionDefinition;
use crate::cube_bridge::evaluator::CubeEvaluator;
use crate::cube_bridge::memeber_sql::{MemberSql, MemberSqlArg};
use crate::planner::query_tools::QueryTools;
use cubenativeutils::CubeError;
use std::any::Any;
use std::rc::Rc;

pub struct CubeNameEvaluator {
    cube_name: String,
}

impl CubeNameEvaluator {
    pub fn new(cube_name: String) -> Self {
        Self { cube_name }
    }
    pub fn default_evaluate_sql(
        &self,
        visitor: &DefaultEvaluatorVisitor,
        tools: Rc<QueryTools>,
    ) -> Result<String, CubeError> {
        Ok(tools.escape_column_name(
            &tools.cube_alias_name(&self.cube_name, visitor.cube_alias_prefix()),
        ))
    }
}

impl MemberEvaluator for CubeNameEvaluator {
    fn cube_name(&self) -> &String {
        &self.cube_name
    }
}

pub struct CubeNameEvaluatorFactory {
    cube_name: String,
}

impl CubeNameEvaluatorFactory {
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

impl MemberEvaluatorFactory for CubeNameEvaluatorFactory {
    fn evaluator_name() -> String {
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

    fn build(self, deps: Vec<Dependency>) -> Result<Rc<EvaluationNode>, CubeError> {
        let Self { cube_name } = self;
        Ok(EvaluationNode::new_cube_name(CubeNameEvaluator::new(
            cube_name,
        )))
    }
}

pub struct CubeTableEvaluator {
    cube_name: String,
    member_sql: Rc<dyn MemberSql>,
    definition: Rc<dyn CubeDefinition>,
}

impl CubeTableEvaluator {
    pub fn new(
        cube_name: String,
        member_sql: Rc<dyn MemberSql>,
        definition: Rc<dyn CubeDefinition>,
    ) -> Self {
        Self {
            cube_name,
            member_sql,
            definition,
        }
    }
    pub fn default_evaluate_sql(
        &self,
        args: Vec<MemberSqlArg>,
        tools: Rc<QueryTools>,
    ) -> Result<String, CubeError> {
        self.member_sql.call(args)
    }
}

impl MemberEvaluator for CubeTableEvaluator {
    fn cube_name(&self) -> &String {
        &self.cube_name
    }
}

pub struct CubeTableEvaluatorFactory {
    cube_name: String,
    sql: Rc<dyn MemberSql>,
    definition: Rc<dyn CubeDefinition>,
}

impl CubeTableEvaluatorFactory {
    pub fn try_new(
        cube_name: &String,
        cube_evaluator: Rc<dyn CubeEvaluator>,
    ) -> Result<Self, CubeError> {
        let definition = cube_evaluator.cube_from_path(cube_name.clone())?;
        Ok(Self {
            cube_name: cube_name.clone(),
            sql: definition.sql_table()?,
            definition,
        })
    }
}

impl MemberEvaluatorFactory for CubeTableEvaluatorFactory {
    fn evaluator_name() -> String {
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

    fn build(self, deps: Vec<Dependency>) -> Result<Rc<EvaluationNode>, CubeError> {
        let Self {
            cube_name,
            sql,
            definition,
        } = self;
        Ok(EvaluationNode::new_cube_table(
            CubeTableEvaluator::new(cube_name, sql, definition),
            deps,
        ))
    }
}
