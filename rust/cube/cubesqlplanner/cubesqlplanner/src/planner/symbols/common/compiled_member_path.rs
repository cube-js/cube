use crate::planner::{CubeId, CubeNameSymbol, CubeTableSymbol, MemberId};
use std::rc::Rc;

#[derive(Clone, Debug)]
pub struct CompiledMemberPath {
    cube: Rc<CubeTableSymbol>,
    id: MemberId,
    name: String,
    alias: String,
    path: Vec<CubeId>,
}

impl CompiledMemberPath {
    pub fn new(
        cube: Rc<CubeTableSymbol>,
        id: MemberId,
        name: String,
        alias: String,
        path: Vec<CubeId>,
    ) -> Self {
        let path = CubeNameSymbol::normalize_path(path, cube.cube_name());
        Self {
            cube,
            id,
            name,
            alias,
            path,
        }
    }

    pub fn id(&self) -> &MemberId {
        &self.id
    }

    pub fn full_name(&self) -> &String {
        self.id.full_name()
    }

    pub fn cube_name(&self) -> &CubeId {
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

    pub fn path(&self) -> &Vec<CubeId> {
        &self.path
    }

    /// Returns a copy with the path reduced to just the owning cube,
    /// stripping any join chain prefix (e.g. from views or cross-cube references).
    pub fn strip_join_prefix(&self) -> Self {
        Self {
            cube: self.cube.clone(),
            id: self.id.clone(),
            name: self.name.clone(),
            alias: self.alias.clone(),
            path: vec![self.cube_name().clone()],
        }
    }
}
