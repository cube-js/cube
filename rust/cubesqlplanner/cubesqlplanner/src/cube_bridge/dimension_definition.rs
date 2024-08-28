use cubenativeutils::wrappers::serializer::{
    NativeDeserialize, NativeDeserializer, NativeSerialize,
};
use cubenativeutils::wrappers::NativeContextHolder;
use cubenativeutils::wrappers::NativeObjectHandle;
use cubenativeutils::CubeError;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
pub struct DimenstionDefinitionStatic {
    #[serde(rename = "type")]
    pub dimension_type: String,
    pub owned_by_cube: Option<bool>,
}

#[nativebridge::native_bridge(DimenstionDefinitionStatic)]
pub trait DimensionDefinition {
    fn sql(&self) -> Result<String, CubeError>;
}
