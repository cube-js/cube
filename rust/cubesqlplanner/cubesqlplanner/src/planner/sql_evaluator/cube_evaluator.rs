use super::dependecy::Dependency;
use super::{evaluate_sql, MemberEvaluator, MemberEvaluatorFactory};
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
    pub fn new(cube_name: String) -> Rc<Self> {
        Rc::new(Self { cube_name })
    }
}

impl MemberEvaluator for CubeNameEvaluator {
    fn evaluate(&self, tools: Rc<QueryTools>) -> Result<String, CubeError> {
        Ok(tools.escape_column_name(&self.cube_name))
    }

    fn as_any(self: Rc<Self>) -> Rc<dyn Any> {
        self.clone()
    }
}

pub struct CubeNameEvaluatorFactory {
    cube_name: String,
}

impl MemberEvaluatorFactory for CubeNameEvaluatorFactory {
    type Result = CubeNameEvaluator;

    fn try_new(
        full_name: String,
        cube_evaluator: Rc<dyn CubeEvaluator>,
    ) -> Result<Self, CubeError> {
        //TODO check that cube exists
        Ok(Self {
            cube_name: full_name,
        })
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

    fn build(self, deps: Vec<Dependency>) -> Result<Rc<Self::Result>, CubeError> {
        let Self { cube_name } = self;
        Ok(CubeNameEvaluator::new(cube_name))
    }
}
