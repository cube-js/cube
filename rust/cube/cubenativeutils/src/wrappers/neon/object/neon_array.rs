use super::{NeonObject, ObjectNeonTypeHolder, RootHolder};
use crate::wrappers::{
    neon::{inner_types::NeonInnerTypes, object::IntoNeonObject},
    object::{NativeArray, NativeType},
    object_handle::NativeObjectHandle,
};
use crate::CubeError;
use neon::prelude::*;

pub struct NeonArray<C: Context<'static>> {
    object: ObjectNeonTypeHolder<C, JsArray>,
}

impl<C: Context<'static> + 'static> NeonArray<C> {
    pub fn new(object: ObjectNeonTypeHolder<C, JsArray>) -> Self {
        Self { object }
    }
}

impl<C: Context<'static>> Clone for NeonArray<C> {
    fn clone(&self) -> Self {
        Self {
            object: self.object.clone(),
        }
    }
}

impl<C: Context<'static> + 'static> NativeType<NeonInnerTypes<C>> for NeonArray<C> {
    fn into_object(self) -> NeonObject<C> {
        let root_holder = RootHolder::from_typed(self.object);
        NeonObject::from_root(root_holder)
    }
}

impl<C: Context<'static> + 'static> NativeArray<NeonInnerTypes<C>> for NeonArray<C> {
    fn len(&self) -> Result<u32, CubeError> {
        self.object
            .map_neon_object::<_, _>(|cx, object| Ok(object.len(cx)))
    }
    fn to_vec(&self) -> Result<Vec<NativeObjectHandle<NeonInnerTypes<C>>>, CubeError> {
        let context = self.object.get_context();
        self.object.map_neon_object(|cx, object| {
            let len = object.len(cx);
            let mut items = Vec::with_capacity(len as usize);
            for idx in 0..len {
                let item = match object
                    .get_value(cx, idx)
                    .map_err(CubeError::from)
                    .and_then(|value| RootHolder::new_in(cx, &context, value))
                {
                    Ok(item) => item,
                    Err(mut err) => {
                        err.message =
                            format!("element {idx}: failed to read value: {}", err.message);
                        return Ok(Err(err));
                    }
                };
                items.push(NeonObject::from_root(item).into());
            }
            Ok(Ok(items))
        })?
    }
    fn set(
        &self,
        index: u32,
        value: NativeObjectHandle<NeonInnerTypes<C>>,
    ) -> Result<bool, CubeError> {
        let value = value.into_object().get_js_value()?;
        self.object
            .map_neon_object::<_, _>(|cx, object| object.set(cx, index, value))
    }
    fn get(&self, index: u32) -> Result<NativeObjectHandle<NeonInnerTypes<C>>, CubeError> {
        let r = self
            .object
            .map_neon_object::<_, _>(|cx, object| object.get::<JsValue, _, _>(cx, index))?;
        Ok(r.into_neon_object(self.object.get_context())?.into())
    }
}
