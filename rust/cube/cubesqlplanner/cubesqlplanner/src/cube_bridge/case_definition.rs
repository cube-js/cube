use super::case_else_item::{CaseElseItem, NativeCaseElseItem};
use super::case_item::{CaseItem, NativeCaseItem};
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
pub trait CaseDefinition {
    #[nbridge(field, vec)]
    fn when(&self) -> Result<Vec<Rc<dyn CaseItem>>, CubeError>;
    #[nbridge(field, rename = "else")]
    fn else_label(&self) -> Result<Rc<dyn CaseElseItem>, CubeError>;
}
