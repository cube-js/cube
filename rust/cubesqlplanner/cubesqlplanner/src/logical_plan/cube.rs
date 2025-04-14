use crate::planner::BaseCube;
use std::rc::Rc;
#[derive(Clone)]
pub struct Cube {
    pub name: String,
    pub cube: Rc<BaseCube>,
}

impl Cube {
    pub fn new(cube: Rc<BaseCube>) -> Rc<Self> {
        Rc::new(Self {
            name: cube.name().clone(),
            cube,
        })
    }
}
