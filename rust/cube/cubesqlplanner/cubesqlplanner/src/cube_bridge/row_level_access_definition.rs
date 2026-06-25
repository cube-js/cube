use super::access_filter_definition::{AccessFilterDefinition, NativeAccessFilterDefinition};
use cubenativeutils::wrappers::serializer::{
    NativeDeserialize, NativeDeserializer, NativeSerialize,
};
use cubenativeutils::wrappers::NativeArray;
use cubenativeutils::wrappers::NativeContextHolder;
use cubenativeutils::wrappers::NativeObjectHandle;
use cubenativeutils::CubeError;
use std::any::Any;
use std::rc::Rc;

#[nativebridge::native_bridge]
pub trait RowLevelAccessDefinition {
    #[nbridge(field, vec)]
    fn filters(&self) -> Result<Vec<Rc<dyn AccessFilterDefinition>>, CubeError>;
}
