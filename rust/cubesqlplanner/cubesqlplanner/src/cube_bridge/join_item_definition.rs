use super::member_sql::{MemberSql, NativeMemberSql};
use cubenativeutils::wrappers::serializer::{
    NativeDeserialize, NativeDeserializer, NativeSerialize,
};
use cubenativeutils::wrappers::NativeContextHolder;
use cubenativeutils::wrappers::NativeObjectHandle;
use cubenativeutils::CubeError;
use serde::{Deserialize, Serialize};
use std::any::Any;
use std::rc::Rc;

#[derive(Serialize, Deserialize, Debug)]
pub struct JoinItemDefinitionStatic {
    pub relationship: String,
}

#[nativebridge::native_bridge(JoinItemDefinitionStatic)]
pub trait JoinItemDefinition {
    #[field]
    fn sql(&self) -> Result<Rc<dyn MemberSql>, CubeError>;
}
