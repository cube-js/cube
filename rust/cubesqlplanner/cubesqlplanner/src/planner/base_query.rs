use cubenativeutils::wrappers::context::NativeContextHolder;
use cubenativeutils::CubeError;

pub struct BaseQuery {
    context: NativeContextHolder,
}

impl BaseQuery {
    pub fn new(context: NativeContextHolder) -> Self {
        Self { context }
    }
    pub fn build_sql_and_params(&self) -> Result<String, CubeError> {
        Ok("Select".to_string())
    }
}
