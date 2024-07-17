use super::deserializer::NativeSerdeDeserializer;
use crate::wrappers::NativeObjectHandler;
use crate::CubeError;
use serde::de::DeserializeOwned;

pub trait NativeDeserialize: Sized {
    fn from_native(v: NativeObjectHandler) -> Result<Self, CubeError>;
}

impl<T: DeserializeOwned + Sized> NativeDeserialize for T {
    fn from_native(v: NativeObjectHandler) -> Result<Self, CubeError> {
        NativeSerdeDeserializer::new(v)
            .deserialize()
            .map_err(|e| CubeError::internal(format!("Failed to deserialize: {}", e)))
    }
}

impl NativeDeserialize for NativeObjectHandler {
    fn from_native(v: NativeObjectHandler) -> Result<Self, CubeError> {
        Ok(v)
    }
}

pub struct NativeDeserializer {}

impl NativeDeserializer {
    pub fn deserialize<T: NativeDeserialize>(v: NativeObjectHandler) -> Result<T, CubeError> {
        T::from_native(v)
    }
}
