use super::serializer::NativeSerdeSerializer;
use crate::wrappers::{NativeContextHolder, NativeObjectHandle};
use crate::CubeError;
use serde::Serialize;

pub trait NativeSerialize {
    fn to_native(&self, context: NativeContextHolder) -> Result<NativeObjectHandle, CubeError>;
}

impl<T: Serialize> NativeSerialize for T {
    fn to_native(&self, context: NativeContextHolder) -> Result<NativeObjectHandle, CubeError> {
        NativeSerdeSerializer::serialize(self, context)
            .map_err(|e| CubeError::internal(format!("Serialize error: {}", e)))
    }
}

impl NativeSerialize for NativeObjectHandle {
    fn to_native(&self, _context: NativeContextHolder) -> Result<NativeObjectHandle, CubeError> {
        Ok(self.clone())
    }
}
