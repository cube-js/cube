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

#[derive(Serialize, Deserialize, Debug, nativebridge::NativeBridgeStatic)]
pub struct JoinItemDefinitionStatic {
    pub name: String,
    pub alias: Option<String>,
    pub relationship: String,
}

impl JoinItemDefinitionStatic {
    /// The name the declaring cube knows the join by: its alias, or the joined cube.
    pub fn effective_name(&self) -> &String {
        self.alias.as_ref().unwrap_or(&self.name)
    }
}

#[nativebridge::native_bridge(JoinItemDefinitionStatic, with_static_meta)]
pub trait JoinItemDefinition {
    #[nbridge(field)]
    fn sql(&self) -> Result<Rc<dyn MemberSql>, CubeError>;
}
