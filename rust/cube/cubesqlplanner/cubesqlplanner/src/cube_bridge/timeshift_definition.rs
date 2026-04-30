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

#[derive(Deserialize, Serialize, Clone, Debug, PartialEq, Eq, Hash)]
pub struct TimeShiftDefinitionStatic {
    pub interval: Option<String>,
    #[serde(rename = "type")]
    pub timeshift_type: Option<String>,
    pub name: Option<String>,
}

#[nativebridge::native_bridge(TimeShiftDefinitionStatic)]
pub trait TimeShiftDefinition {
    #[nbridge(field, optional)]
    fn sql(&self) -> Result<Option<Rc<dyn MemberSql>>, CubeError>;
}
