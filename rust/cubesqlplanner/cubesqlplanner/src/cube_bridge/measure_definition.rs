use super::cube_definition::{CubeDefinition, NativeCubeDefinition};
use super::memeber_sql::{MemberSql, NativeMemberSql};
use cubenativeutils::wrappers::serializer::{
    NativeDeserialize, NativeDeserializer, NativeSerialize,
};
use cubenativeutils::wrappers::NativeContextHolder;
use cubenativeutils::wrappers::NativeObjectHandle;
use cubenativeutils::CubeError;
use serde::{Deserialize, Serialize};
use std::rc::Rc;

#[derive(Serialize, Deserialize, Debug)]
pub struct MeasureDefinitionStatic {
    #[serde(rename = "type")]
    pub measure_type: String,
    pub owned_by_cube: Option<bool>,
}

#[nativebridge::native_bridge(MeasureDefinitionStatic)]
pub trait MeasureDefinition {
    #[optional]
    #[field]
    fn sql(&self) -> Result<Option<Rc<dyn MemberSql>>, CubeError>;

    fn cube(&self) -> Result<Rc<dyn CubeDefinition>, CubeError>;
}
