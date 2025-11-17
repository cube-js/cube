use super::{NeonObject, ObjectNeonTypeHolder, RootHolder};
use crate::wrappers::{
    neon::{inner_types::NeonInnerTypes, object::IntoNeonObject},
    object::{NativeStruct, NativeType},
    object_handle::NativeObjectHandle,
};
use crate::CubeError;
use neon::prelude::*;

pub struct NeonStruct<C: Context<'static>> {
    object: ObjectNeonTypeHolder<C, JsObject>,
}

impl<C: Context<'static> + 'static> NeonStruct<C> {
    pub fn new(object: ObjectNeonTypeHolder<C, JsObject>) -> Self {
        Self { object }
    }
}

impl<C: Context<'static>> Clone for NeonStruct<C> {
    fn clone(&self) -> Self {
        Self {
            object: self.object.clone(),
        }
    }
}

impl<C: Context<'static> + 'static> NativeType<NeonInnerTypes<C>> for NeonStruct<C> {
    fn into_object(self) -> NeonObject<C> {
        let root_holder = RootHolder::from_typed(self.object);
        NeonObject::form_root(root_holder)
    }
}

impl<C: Context<'static> + 'static> NativeStruct<NeonInnerTypes<C>> for NeonStruct<C> {
    fn get_field(
        &self,
        field_name: &str,
    ) -> Result<NativeObjectHandle<NeonInnerTypes<C>>, CubeError> {
        let neon_result = self
            .object
            .map_neon_object(|cx, neon_object| neon_object.get::<JsValue, _, _>(cx, field_name))?;
        Ok(NativeObjectHandle::new(NeonObject::new(
            self.object.get_context(),
            neon_result,
        )?))
    }

    fn has_field(&self, field_name: &str) -> Result<bool, CubeError> {
        let result = self.object.map_neon_object(|cx, neon_object| {
            let res = neon_object
                .get_opt::<JsValue, _, _>(cx, field_name)?
                .is_some();
            Ok(res)
        })?;
        Ok(result)
    }

    fn set_field(
        &self,
        field_name: &str,
        value: NativeObjectHandle<NeonInnerTypes<C>>,
    ) -> Result<bool, CubeError> {
        let value = value.into_object().get_js_value()?;
        self.object
            .map_neon_object::<_, _>(|cx, object| object.set(cx, field_name, value))
    }
    fn get_own_property_names(
        &self,
    ) -> Result<Vec<NativeObjectHandle<NeonInnerTypes<C>>>, CubeError> {
        self.object
            .map_neon_object(|cx, neon_object| {
                let neon_array = neon_object.get_own_property_names(cx)?;
                neon_array.to_vec(cx)
            })?
            .into_iter()
            .map(|o| Ok(o.into_neon_object(self.object.get_context())?.into()))
            .collect()
    }
    fn call_method(
        &self,
        method: &str,
        args: Vec<NativeObjectHandle<NeonInnerTypes<C>>>,
    ) -> Result<NativeObjectHandle<NeonInnerTypes<C>>, CubeError> {
        let neon_args = args
            .into_iter()
            .map(|arg| -> Result<_, CubeError> { arg.into_object().get_js_value() })
            .collect::<Result<Vec<_>, _>>()?;

        let result = self
            .object
            .map_neon_object(|cx, neon_object| {
                neon_object
                    .get::<JsFunction, _, _>(cx, method)?
                    .call(cx, *neon_object, neon_args)
            })?
            .into_neon_object(self.object.get_context())?;
        Ok(result.into())
    }
}
