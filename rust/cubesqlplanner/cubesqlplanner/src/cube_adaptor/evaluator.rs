use async_trait::async_trait;
use cubenativeutils::wrappers::object::{NativeObject, NativeObjectHolder};
use cubenativeutils::wrappers::object_handler::NativeObjectHandler;
use cubenativeutils::wrappers::serializer::deserializer::NativeDeserializer;
use cubenativeutils::wrappers::serializer::serializer::NativeSerializer;
use cubenativeutils::CubeError;

#[nativebridge::native_bridge]
pub trait CubeEvaluator {
    fn parse_path(&self, path_type: String, path: String) -> Result<Vec<String>, CubeError>;
}
