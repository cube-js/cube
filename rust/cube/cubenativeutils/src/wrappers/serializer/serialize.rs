use super::serializer::NativeSerdeSerializer;
use crate::{
    wrappers::{inner_types::InnerTypes, NativeContextHolder, NativeObjectHandle},
    CubeError,
};
use serde::Serialize;

pub trait NativeSerialize<IT: InnerTypes> {
    fn to_native(
        &self,
        context: NativeContextHolder<IT>,
    ) -> Result<NativeObjectHandle<IT>, CubeError>;
}

impl<IT: InnerTypes, T: Serialize> NativeSerialize<IT> for T {
    fn to_native(
        &self,
        context: NativeContextHolder<IT>,
    ) -> Result<NativeObjectHandle<IT>, CubeError> {
        NativeSerdeSerializer::serialize(self, context)
            .map_err(|e| CubeError::internal(format!("Serialize error: {}", e)))
    }
}

impl<IT: InnerTypes> NativeSerialize<IT> for NativeObjectHandle<IT> {
    fn to_native(
        &self,
        _context: NativeContextHolder<IT>,
    ) -> Result<NativeObjectHandle<IT>, CubeError> {
        Ok(self.clone())
    }
}
