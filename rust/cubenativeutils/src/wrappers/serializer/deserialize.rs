use super::deserializer::NativeSerdeDeserializer;
use crate::{
    wrappers::{inner_types::InnerTypes, NativeObjectHandle},
    CubeError,
};
use serde::de::DeserializeOwned;

pub trait NativeDeserialize<IT: InnerTypes>: Sized {
    fn from_native(v: NativeObjectHandle<IT>) -> Result<Self, CubeError>;
}

impl<IT: InnerTypes, T: DeserializeOwned + Sized> NativeDeserialize<IT> for T {
    fn from_native(v: NativeObjectHandle<IT>) -> Result<Self, CubeError> {
        NativeSerdeDeserializer::new(v)
            .deserialize()
            .map_err(|e| CubeError::internal(format!("Failed to deserialize: {}", e)))
    }
}

impl<IT: InnerTypes> NativeDeserialize<IT> for NativeObjectHandle<IT> {
    fn from_native(v: NativeObjectHandle<IT>) -> Result<Self, CubeError> {
        Ok(v)
    }
}

pub struct NativeDeserializer {}

impl NativeDeserializer {
    pub fn deserialize<IT: InnerTypes, T: NativeDeserialize<IT>>(
        v: NativeObjectHandle<IT>,
    ) -> Result<T, CubeError> {
        T::from_native(v)
    }
}
