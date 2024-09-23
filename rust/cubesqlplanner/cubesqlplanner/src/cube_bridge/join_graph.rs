use super::join_definition::{JoinDefinition, NativeJoinDefinition};
use cubenativeutils::wrappers::serializer::{
    NativeDeserialize, NativeDeserializer, NativeSerialize,
};
use cubenativeutils::wrappers::NativeContextHolder;
use cubenativeutils::wrappers::NativeObjectHandle;
use cubenativeutils::CubeError;
use std::any::Any;
use std::rc::Rc;

#[nativebridge::native_bridge]
pub trait JoinGraph {
    fn build_join(&self, cubes_to_join: Vec<String>) -> Result<Rc<dyn JoinDefinition>, CubeError>;
}
