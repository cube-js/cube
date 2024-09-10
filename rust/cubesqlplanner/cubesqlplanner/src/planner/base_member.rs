use cubenativeutils::CubeError;
pub trait BaseMember {
    fn to_sql(&self) -> Result<String, CubeError>;
}

pub trait IndexedMember: BaseMember {
    fn index(&self) -> usize;
}
