use cubenativeutils::wrappers::serializer::{
    NativeDeserialize, NativeDeserializer, NativeSerialize,
};
use cubenativeutils::wrappers::NativeContextHolder;
use cubenativeutils::wrappers::NativeObjectHandle;
use cubenativeutils::CubeError;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
pub struct CubeDefinitionStatic {
    pub name: String,
}

#[nativebridge::native_bridge(CubeDefinitionStatic)]
pub trait CubeDefinition {
    fn sql_table(&self) -> Result<String, CubeError>;
}
