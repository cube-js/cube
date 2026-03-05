use crate::planner::sql_evaluator::CubeTableSymbol;
use std::rc::Rc;

#[derive(Clone, Debug)]
pub struct CompiledMemberPath {
    cube: Rc<CubeTableSymbol>,
    full_name: String,
    name: String,
    alias: String,
    path: Vec<String>,
}

impl CompiledMemberPath {
    pub fn new(
        cube: Rc<CubeTableSymbol>,
        full_name: String,
        name: String,
        alias: String,
        path: Vec<String>,
    ) -> Self {
        Self {
            cube,
            full_name,
            name,
            alias,
            path,
        }
    }

    pub fn full_name(&self) -> &String {
        &self.full_name
    }

    pub fn cube_name(&self) -> &String {
        self.cube.cube_name()
    }

    pub fn cube(&self) -> &Rc<CubeTableSymbol> {
        &self.cube
    }

    pub fn join_map(&self) -> &Option<Vec<Vec<String>>> {
        self.cube.join_map()
    }

    pub fn name(&self) -> &String {
        &self.name
    }

    pub fn alias(&self) -> &String {
        &self.alias
    }

    pub fn path(&self) -> &Vec<String> {
        &self.path
    }
}
