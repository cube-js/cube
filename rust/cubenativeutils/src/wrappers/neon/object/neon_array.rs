use super::NeonObject;
use crate::wrappers::{
    neon::inner_types::NeonInnerTypes,
    object::{NativeArray, NativeObject, NativeType},
    object_handle::NativeObjectHandle,
};
use cubesql::CubeError;
use neon::prelude::*;

#[derive(Clone)]
pub struct NeonArray<'cx: 'static, C: Context<'cx>> {
    object: NeonObject<'cx, C>,
}

/* impl<C: Context<'static>> InnerTyped for NeonArray<C> {
    type Inner = NeonObject<C>;
} */

impl<'cx, C: Context<'cx> + 'cx> NeonArray<'cx, C> {
    pub fn new(object: NeonObject<'cx, C>) -> Self {
        Self { object }
    }
}

impl<'cx, C: Context<'cx> + 'cx> NativeType<NeonInnerTypes<'cx, C>> for NeonArray<'cx, C> {
    fn into_object(self) -> NeonObject<'cx, C> {
        self.object
    }
}

impl<'cx, C: Context<'cx> + 'cx> NativeArray<NeonInnerTypes<'cx, C>> for NeonArray<'cx, C> {
    fn len(&self) -> Result<u32, CubeError> {
        self.object
            .map_downcast_neon_object::<JsArray, _, _>(|cx, object| Ok(object.len(cx)))
    }
    fn to_vec(&self) -> Result<Vec<NativeObjectHandle<NeonInnerTypes<'cx, C>>>, CubeError> {
        let neon_vec = self
            .object
            .map_downcast_neon_object::<JsArray, _, _>(|cx, object| {
                object
                    .to_vec(cx)
                    .map_err(|_| CubeError::internal("Error converting JsArray to Vec".to_string()))
            })?;

        Ok(neon_vec
            .into_iter()
            .map(|o| NativeObjectHandle::new(NeonObject::new(self.object.get_context(), o)))
            .collect())
    }
    fn set(
        &self,
        index: u32,
        value: NativeObjectHandle<NeonInnerTypes<'cx, C>>,
    ) -> Result<bool, CubeError> {
        let value = value.into_object().into_object();
        self.object
            .map_downcast_neon_object::<JsArray, _, _>(|cx, object| {
                object
                    .set(cx, index, value)
                    .map_err(|_| CubeError::internal(format!("Error setting index {}", index)))
            })
    }
    fn get(&self, index: u32) -> Result<NativeObjectHandle<NeonInnerTypes<'cx, C>>, CubeError> {
        let r = self
            .object
            .map_downcast_neon_object::<JsArray, _, _>(|cx, object| {
                object
                    .get(cx, index)
                    .map_err(|_| CubeError::internal(format!("Error setting index {}", index)))
            })?;
        Ok(NativeObjectHandle::new(NeonObject::new(
            self.object.get_context(),
            r,
        )))
    }
}
