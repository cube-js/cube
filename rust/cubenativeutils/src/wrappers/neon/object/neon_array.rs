use super::NeonObject;
use crate::wrappers::object::{NativeArray, NativeBoxedClone, NativeObject, NativeType};
use crate::wrappers::object_handler::NativeObjectHandler;
use cubesql::CubeError;
use neon::prelude::*;

#[derive(Clone)]
pub struct NeonArray<C: Context<'static>> {
    object: Box<NeonObject<C>>,
}

impl<C: Context<'static> + 'static> NeonArray<C> {
    pub fn new(object: Box<NeonObject<C>>) -> Box<Self> {
        Box::new(Self { object })
    }
}

impl<C: Context<'static> + 'static> NativeType for NeonArray<C> {
    fn into_object(self: Box<Self>) -> Box<dyn NativeObject> {
        self.object
    }
    fn get_object(&self) -> Box<dyn NativeObject> {
        self.object.boxed_clone()
    }
}

impl<C: Context<'static> + 'static> NativeArray for NeonArray<C> {
    fn len(&self) -> Result<u32, CubeError> {
        self.object
            .map_downcast_neon_object::<JsArray, _, _>(|cx, object| Ok(object.len(cx)))
    }
    fn to_vec(&self) -> Result<Vec<NativeObjectHandler>, CubeError> {
        let neon_vec = self
            .object
            .map_downcast_neon_object::<JsArray, _, _>(|cx, object| {
                object
                    .to_vec(cx)
                    .map_err(|_| CubeError::internal("Error converting JsArray to Vec".to_string()))
            })?;
        Ok(neon_vec
            .into_iter()
            .map(|o| NativeObjectHandler::new(NeonObject::new(self.object.get_context(), o)))
            .collect())
    }
    fn set(&self, index: u32, value: NativeObjectHandler) -> Result<bool, CubeError> {
        let value = value.downcast_object::<NeonObject<C>>()?.into_object();
        self.object
            .map_downcast_neon_object::<JsArray, _, _>(|cx, object| {
                object
                    .set(cx, index, value)
                    .map_err(|_| CubeError::internal(format!("Error setting index {}", index)))
            })
    }
    fn get(&self, index: u32) -> Result<NativeObjectHandler, CubeError> {
        let r = self
            .object
            .map_downcast_neon_object::<JsArray, _, _>(|cx, object| {
                object
                    .get(cx, index)
                    .map_err(|_| CubeError::internal(format!("Error setting index {}", index)))
            })?;
        Ok(NativeObjectHandler::new(NeonObject::new(
            self.object.get_context(),
            r,
        )))
    }
}
