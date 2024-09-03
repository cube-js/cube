use super::Context;
use cubenativeutils::CubeError;
use std::rc::Rc;
pub trait BaseMember {
    fn to_sql(&self, context: Rc<Context>) -> Result<String, CubeError>;
    fn alias_name(&self) -> Result<String, CubeError>;
}

pub trait IndexedMember: BaseMember {
    fn index(&self) -> usize;
}
