use super::dependecy::Dependency;
use super::{EvaluationNode, MemberEvaluatorType};
use super::{MemberEvaluator, MemberEvaluatorFactory};
use crate::cube_bridge::dimension_definition::DimensionDefinition;
use crate::cube_bridge::evaluator::CubeEvaluator;
use crate::cube_bridge::memeber_sql::MemberSql;
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
    pub fn default_evaluate_sql(&self, tools: Rc<QueryTools>) -> Result<String, CubeError> {
        Ok(tools.escape_column_name(&self.cube_name))
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
