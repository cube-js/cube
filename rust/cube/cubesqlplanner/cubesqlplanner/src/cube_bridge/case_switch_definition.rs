use crate::cube_bridge::member_sql::{MemberSql, NativeMemberSql};

use super::case_switch_else_item::{CaseSwitchElseItem, NativeCaseSwitchElseItem};
use super::case_switch_item::{CaseSwitchItem, NativeCaseSwitchItem};
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
pub trait CaseSwitchDefinition {
    #[nbridge(field)]
    fn switch(&self) -> Result<Rc<dyn MemberSql>, CubeError>;
    #[nbridge(field, vec)]
    fn when(&self) -> Result<Vec<Rc<dyn CaseSwitchItem>>, CubeError>;
    #[nbridge(field, rename = "else")]
    fn else_sql(&self) -> Result<Rc<dyn CaseSwitchElseItem>, CubeError>;
}
