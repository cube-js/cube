use super::NeonObject;
use crate::wrappers::{
    neon::inner_types::NeonInnerTypes,
    object::{NativeStruct, NativeType},
    object_handle::NativeObjectHandle,
};
use cubesql::CubeError;
use neon::prelude::*;

#[derive(Clone)]
pub struct NeonStruct<'cx: 'static, C: Context<'cx>> {
    object: NeonObject<'cx, C>,
}

impl<'cx, C: Context<'cx> + 'cx> NeonStruct<'cx, C> {
    pub fn new(object: NeonObject<'cx, C>) -> Self {
        Self { object }
    }
}

impl<'cx, C: Context<'cx> + 'cx> NativeType<NeonInnerTypes<'cx, C>> for NeonStruct<'cx, C> {
    fn into_object(self) -> NeonObject<'cx, C> {
        self.object
    }
}

impl<'cx, C: Context<'cx> + 'cx> NativeStruct<NeonInnerTypes<'cx, C>> for NeonStruct<'cx, C> {
    fn get_field(
        &self,
        field_name: &str,
    ) -> Result<NativeObjectHandle<NeonInnerTypes<'cx, C>>, CubeError> {
        let neon_result = self.object.map_neon_object(|cx, neon_object| {
            let this = neon_object
                .downcast::<JsObject, _>(cx)
                .map_err(|_| CubeError::internal(format!("Neon object is not JsObject")))?;
            this.get::<JsValue, _, _>(cx, field_name)
                .map_err(|_| CubeError::internal(format!("Field `{}` not found", field_name)))
        })?;
        Ok(NativeObjectHandle::new(NeonObject::new(
            self.object.context.clone(),
            neon_result,
        )))
    }

    fn has_field(&self, field_name: &str) -> Result<bool, CubeError> {
        let result = self
            .object
            .map_neon_object(|cx, neon_object| -> Result<bool, CubeError> {
                let this = neon_object
                    .downcast::<JsObject, _>(cx)
                    .map_err(|_| CubeError::internal(format!("Neon object is not JsObject")))?;
                let res = this
                    .get_opt::<JsValue, _, _>(cx, field_name)
                    .map_err(|_| {
                        CubeError::internal(format!(
                            "Error while getting field `{}` not found",
                            field_name
                        ))
                    })?
                    .is_some();
                Ok(res)
            })?;
        Ok(result)
    }

    fn set_field(
        &self,
        field_name: &str,
        value: NativeObjectHandle<NeonInnerTypes<'cx, C>>,
    ) -> Result<bool, CubeError> {
        let value = value.into_object().into_object();
        self.object
            .map_downcast_neon_object::<JsObject, _, _>(|cx, object| {
                object
                    .set(cx, field_name, value)
                    .map_err(|_| CubeError::internal(format!("Error setting field {}", field_name)))
            })
    }
    fn get_own_property_names(
        &self,
    ) -> Result<Vec<NativeObjectHandle<NeonInnerTypes<'cx, C>>>, CubeError> {
        let neon_array = self.object.map_neon_object(|cx, neon_object| {
            let this = neon_object
                .downcast::<JsObject, _>(cx)
                .map_err(|_| CubeError::internal(format!("Neon object is not JsObject")))?;
            let neon_array = this
                .get_own_property_names(cx)
                .map_err(|_| CubeError::internal(format!("Cannot get own properties not found")))?;

            neon_array
                .to_vec(cx)
                .map_err(|_| CubeError::internal(format!("Failed to convert array")))
        })?;
        Ok(neon_array
            .into_iter()
            .map(|o| NativeObjectHandle::new(NeonObject::new(self.object.context.clone(), o)))
            .collect())
    }
    fn call_method(
        &self,
        method: &str,
        args: Vec<NativeObjectHandle<NeonInnerTypes<'cx, C>>>,
    ) -> Result<NativeObjectHandle<NeonInnerTypes<'cx, C>>, CubeError> {
        let neon_args = args
            .into_iter()
            .map(|arg| -> Result<_, CubeError> { Ok(arg.into_object().get_object()) })
            .collect::<Result<Vec<_>, _>>()?;

        let neon_reuslt = self.object.map_neon_object(|cx, neon_object| {
            let this = neon_object
                .downcast::<JsObject, _>(cx)
                .map_err(|_| CubeError::internal(format!("Neon object is not JsObject")))?;
            let neon_method = this
                .get::<JsFunction, _, _>(cx, method)
                .map_err(|_| CubeError::internal(format!("Method `{}` not found", method)))?;
            neon_method.call(cx, this, neon_args).map_err(|err| {
                CubeError::internal(format!(
                    "Failed to call method `{} {} {:?}",
                    method, err, err
                ))
            })
        })?;
        Ok(NativeObjectHandle::new(NeonObject::new(
            self.object.context.clone(),
            neon_reuslt,
        )))
    }
}
