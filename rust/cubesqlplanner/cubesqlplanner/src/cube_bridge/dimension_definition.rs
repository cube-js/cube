use super::memeber_sql::{MemberSql, NativeMemberSql};
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
pub struct DimenstionDefinitionStatic {
    #[serde(rename = "type")]
    pub dimension_type: String,
    #[serde(rename = "ownedByCube")]
    pub owned_by_cube: Option<bool>,
    #[serde(rename = "multiStage")]
    pub multi_stage: Option<bool>,
}

#[nativebridge::native_bridge(DimenstionDefinitionStatic)]
pub trait DimensionDefinition {
    #[field]
    fn sql(&self) -> Result<Rc<dyn MemberSql>, CubeError>;
}
