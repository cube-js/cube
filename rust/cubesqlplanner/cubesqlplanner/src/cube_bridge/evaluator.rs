use async_trait::async_trait;
use cubenativeutils::wrappers::serializer::{
    NativeDeserialize, NativeDeserializer, NativeSerialize,
};
use cubenativeutils::wrappers::NativeContextHolder;
use cubenativeutils::wrappers::{NativeObjectHandler, NativeObjectHolder};
use cubenativeutils::CubeError;

#[nativebridge::native_bridge]
pub trait CubeEvaluator {
    fn parse_path(&self, path_type: String, path: String) -> Result<Vec<String>, CubeError>;
}
