use cubeclient::models::V1CubeMeta;

pub struct TenantContext {
    pub cubes: Vec<V1CubeMeta>,
}

impl TenantContext {
    pub fn find_cube_with_name(&self, name: String) -> Option<V1CubeMeta> {
        for cube in self.cubes.iter() {
            if cube.name.eq(&name) {
                return Some(cube.clone());
            }
        }

        None
    }
}
