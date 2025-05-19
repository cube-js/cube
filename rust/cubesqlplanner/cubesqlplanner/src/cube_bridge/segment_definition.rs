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
pub struct SegmentDefinitionStatic {
    #[serde(rename = "type")]
    pub segment_type: Option<String>,
    #[serde(rename = "ownedByCube")]
    pub owned_by_cube: Option<bool>,
}

#[nativebridge::native_bridge(SegmentDefinitionStatic)]
pub trait SegmentDefinition {
    #[nbridge(field)]
    fn sql(&self) -> Result<Rc<dyn MemberSql>, CubeError>;
}
