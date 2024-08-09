use crate::cube_bridge::base_tools::BaseTools;
use crate::cube_bridge::evaluator::CubeEvaluator;
use convert_case::{Case, Casing};
use cubenativeutils::CubeError;
use std::rc::Rc;

pub struct QueryTools {
    cube_evaluator: Rc<dyn CubeEvaluator>,
    base_tools: Rc<dyn BaseTools>,
}

impl QueryTools {
    pub fn new(cube_evaluator: Rc<dyn CubeEvaluator>, base_tools: Rc<dyn BaseTools>) -> Rc<Self> {
        Rc::new(Self {
            cube_evaluator,
            base_tools,
        })
    }

    pub fn cube_evaluator(&self) -> &Rc<dyn CubeEvaluator> {
        &self.cube_evaluator
    }

    pub fn base_tools(&self) -> &Rc<dyn BaseTools> {
        &self.base_tools
    }

    pub fn alias_name(&self, name: &str) -> Result<String, CubeError> {
        Ok(name.to_case(Case::Snake).replace(".", "__"))
    }
}
