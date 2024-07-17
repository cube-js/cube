use super::serializer::NativeSerdeSerializer;
use crate::wrappers::{NativeContextHolder, NativeObjectHandler};
use crate::CubeError;
use serde::Serialize;

pub trait NativeSerialize {
    fn to_native(&self, context: NativeContextHolder) -> Result<NativeObjectHandler, CubeError>;
}

impl<T: Serialize> NativeSerialize for T {
    fn to_native(&self, context: NativeContextHolder) -> Result<NativeObjectHandler, CubeError> {
        NativeSerdeSerializer::serialize(self, context)
            .map_err(|e| CubeError::internal(format!("Serialize error: {}", e)))
    }
}

impl NativeSerialize for NativeObjectHandler {
    fn to_native(&self, _context: NativeContextHolder) -> Result<NativeObjectHandler, CubeError> {
        Ok(self.clone())
    }
}
