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
        mut path: Vec<String>,
    ) -> Self {
        let cn = cube.cube_name();
        if path.is_empty() || path.last() != Some(cn) {
            path.push(cn.clone());
        }
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

    /// Returns a copy with the path reduced to just the owning cube,
    /// stripping any join chain prefix from views or other contexts.
    pub fn own_path(&self) -> Self {
        Self {
            cube: self.cube.clone(),
            full_name: self.full_name.clone(),
            name: self.name.clone(),
            alias: self.alias.clone(),
            path: vec![self.cube_name().clone()],
        }
    }
}
