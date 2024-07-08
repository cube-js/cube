use async_trait::async_trait;
use cubenativeutils::wrappers::object::{NativeObject, NativeObjectHolder};
use cubenativeutils::CubeError;
use std::sync::Arc;

#[neonservice::neon_service]
pub trait CubeEvaluatorTest {
    async fn parse_path(&self, path_type: String, path: String) -> Result<Vec<String>, CubeError>;
}
