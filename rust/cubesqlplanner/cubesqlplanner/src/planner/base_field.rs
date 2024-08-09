use cubenativeutils::CubeError;
pub trait BaseField {
    fn to_sql(&self) -> Result<String, CubeError>;
    fn index(&self) -> usize;
}
