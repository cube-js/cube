use cubenativeutils::CubeError;

pub struct SchemaColumn {
    table_name: String,
    name: String,
    origin_member: String,
}

impl SchemaColumn {
    pub fn new(table_name: String, name: String, origin_member: String) -> Self {
        Self {
            table_name,
            name,
            origin_member,
        }
    }
}
pub struct Schema {}
