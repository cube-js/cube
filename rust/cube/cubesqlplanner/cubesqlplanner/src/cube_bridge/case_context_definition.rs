use super::case_context_item::{CaseContextItem, NativeCaseContextItem};
use super::case_switch_else_item::{CaseSwitchElseItem, NativeCaseSwitchElseItem};
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
pub trait CaseContextDefinition {
    #[nbridge(field, vec)]
    fn when(&self) -> Result<Vec<Rc<dyn CaseContextItem>>, CubeError>;
    #[nbridge(field, rename = "else")]
    fn else_sql(&self) -> Result<Rc<dyn CaseSwitchElseItem>, CubeError>;
}
