#[derive(Clone, Debug)]
pub struct CompiledMemberPath {
    full_name: String,
    cube_name: String,
    name: String,
    alias: String,
    path: Vec<String>,
}

impl CompiledMemberPath {
    pub fn new(
        full_name: String,
        cube_name: String,
        name: String,
        alias: String,
        path: Vec<String>,
    ) -> Self {
        Self {
            full_name,
            cube_name,
            name,
            alias,
            path,
        }
    }

    pub fn full_name(&self) -> &String {
        &self.full_name
    }

    pub fn cube_name(&self) -> &String {
        &self.cube_name
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
