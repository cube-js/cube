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

#[derive(
    Deserialize, Serialize, Clone, Debug, PartialEq, Eq, Hash, nativebridge::NativeBridgeStatic,
)]
pub struct GranularityDefinitionStatic {
    pub interval: String,
    pub origin: Option<String>,
    pub offset: Option<String>,
}

#[nativebridge::native_bridge(GranularityDefinitionStatic, with_static_meta)]
pub trait GranularityDefinition {
    #[nbridge(field, optional)]
    fn sql(&self) -> Result<Option<Rc<dyn MemberSql>>, CubeError>;
}
