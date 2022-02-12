use cubeclient::models::V1CubeMeta;

pub struct MetaContext {
    pub cubes: Vec<V1CubeMeta>,
}

impl MetaContext {
    pub fn find_cube_with_name(&self, name: String) -> Option<V1CubeMeta> {
        for cube in self.cubes.iter() {
            if cube.name.eq(&name) {
                return Some(cube.clone());
            }
        }

        None
    }
}
