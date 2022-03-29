use cubeclient::models::V1CubeMeta;

#[derive(Debug)]
pub struct MetaContext {
    pub cubes: Vec<V1CubeMeta>,
    pub tables: Vec<CubeMetaTable>,
}

#[derive(Debug, Clone)]
pub struct CubeMetaTable {
    oid: u32,
    name: String,
}

impl MetaContext {
    pub fn new(cubes: Vec<V1CubeMeta>) -> Self {
        // 18000 - max system table oid
        let mut oid: u32 = 18000;
        let tables: Vec<CubeMetaTable> = cubes.iter().map(|cube| { oid += 10; CubeMetaTable { oid, name: cube.name.clone() } }).collect();

        Self { cubes, tables }
    }

    pub fn find_cube_with_name(&self, name: String) -> Option<V1CubeMeta> {
        for cube in self.cubes.iter() {
            if cube.name.eq(&name) {
                return Some(cube.clone());
            }
        }

        None
    }

    pub fn find_cube_table_with_oid(&self, oid: u32) -> Option<CubeMetaTable> {
        self.tables.iter().find(|table| table.oid == oid).cloned()
    }

    pub fn find_cube_table_with_name(&self, name: String) -> Option<CubeMetaTable> {
        self.tables.iter().find(|table| table.name == name).cloned()
    }
}
