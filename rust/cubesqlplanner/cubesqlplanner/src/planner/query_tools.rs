use crate::cube_bridge::evaluator::CubeEvaluator;
use std::rc::Rc;

pub struct QueryTools {
    cube_evaluator: Rc<dyn CubeEvaluator>,
}

impl QueryTools {
    pub fn new(cube_evaluator: Rc<dyn CubeEvaluator>) -> Rc<Self> {
        Rc::new(Self { cube_evaluator })
    }
    pub fn cube_evaluator(&self) -> &Rc<dyn CubeEvaluator> {
        &self.cube_evaluator
    }
}
