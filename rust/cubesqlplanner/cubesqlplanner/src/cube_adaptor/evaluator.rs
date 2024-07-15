use async_trait::async_trait;
use cubenativeutils::wrappers::object::{NativeObject, NativeObjectHolder};
use cubenativeutils::CubeError;

#[nativebridge::native_bridge]
pub trait CubeEvaluator {
    async fn parse_path(&self, path_type: String, path: String) -> Result<Vec<String>, CubeError>;
}
