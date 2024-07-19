use super::deserializer::NativeSerdeDeserializer;
use crate::wrappers::NativeObjectHandle;
use crate::CubeError;
use serde::de::DeserializeOwned;

pub trait NativeDeserialize: Sized {
    fn from_native(v: NativeObjectHandle) -> Result<Self, CubeError>;
}

impl<T: DeserializeOwned + Sized> NativeDeserialize for T {
    fn from_native(v: NativeObjectHandle) -> Result<Self, CubeError> {
        NativeSerdeDeserializer::new(v)
            .deserialize()
            .map_err(|e| CubeError::internal(format!("Failed to deserialize: {}", e)))
    }
}

impl NativeDeserialize for NativeObjectHandle {
    fn from_native(v: NativeObjectHandle) -> Result<Self, CubeError> {
        Ok(v)
    }
}

pub struct NativeDeserializer {}

impl NativeDeserializer {
    pub fn deserialize<T: NativeDeserialize>(v: NativeObjectHandle) -> Result<T, CubeError> {
        T::from_native(v)
    }
}
