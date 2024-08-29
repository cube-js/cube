use super::dependecy::Dependency;
use super::{EvaluationNode, MemberEvaluatorType};
use super::{MemberEvaluator, MemberEvaluatorFactory};
use crate::cube_bridge::dimension_definition::DimensionDefinition;
use crate::cube_bridge::evaluator::CubeEvaluator;
use crate::cube_bridge::memeber_sql::{self, MemberSql, MemberSqlArg};
use crate::planner::query_tools::QueryTools;
use cubenativeutils::CubeError;
use std::any::Any;
use std::rc::Rc;

pub struct DimensionEvaluator {
    cube_name: String,
    name: String,
    member_sql: Rc<dyn MemberSql>,
    definition: Rc<dyn DimensionDefinition>,
}

impl DimensionEvaluator {
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
    pub fn default_evaluate_sql(
        &self,
        args: Vec<MemberSqlArg>,
        tools: Rc<QueryTools>,
    ) -> Result<String, CubeError> {
        let sql = tools.auto_prefix_with_cube_name(&self.cube_name, &self.member_sql.call(args)?);
        Ok(sql)
    }
}

impl MemberEvaluator for DimensionEvaluator {
    fn cube_name(&self) -> &String {
        &self.cube_name
    }
}

pub struct DimensionEvaluatorFactory {
    cube_name: String,
    name: String,
    sql: Rc<dyn MemberSql>,
    definition: Rc<dyn DimensionDefinition>,
}

impl DimensionEvaluatorFactory {
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

impl MemberEvaluatorFactory for DimensionEvaluatorFactory {
    fn cube_name(&self) -> &String {
        &self.cube_name
    }

    fn deps_names(&self) -> Result<Vec<String>, CubeError> {
        Ok(self.definition.sql()?.args_names().clone())
    }

    fn member_sql(&self) -> Option<Rc<dyn MemberSql>> {
        Some(self.sql.clone())
    }

    fn build(self, deps: Vec<Dependency>) -> Result<Rc<EvaluationNode>, CubeError> {
        let Self {
            cube_name,
            name,
            sql,
            definition,
        } = self;
        Ok(EvaluationNode::new_dimension(
            DimensionEvaluator::new(cube_name, name, sql, definition),
            deps,
        ))
    }
}
