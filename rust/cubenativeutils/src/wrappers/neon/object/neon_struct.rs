use super::{NeonObject, NeonTypeHandle};
use crate::wrappers::{
    neon::inner_types::NeonInnerTypes,
    object::{NativeStruct, NativeType},
    object_handle::NativeObjectHandle,
};
use cubesql::CubeError;
use neon::prelude::*;

#[derive(Clone)]
pub struct NeonStruct<C: Context<'static>> {
    object: NeonTypeHandle<C, JsObject>,
}

impl<C: Context<'static> + 'static> NeonStruct<C> {
    pub fn new(object: NeonTypeHandle<C, JsObject>) -> Self {
        Self { object }
    }
}

impl<C: Context<'static> + 'static> NativeType<NeonInnerTypes<C>> for NeonStruct<C> {
    fn into_object(self) -> NeonObject<C> {
        self.object.upcast()
    }
}

impl<C: Context<'static> + 'static> NativeStruct<NeonInnerTypes<C>> for NeonStruct<C> {
    fn get_field(
        &self,
        field_name: &str,
    ) -> Result<NativeObjectHandle<NeonInnerTypes<C>>, CubeError> {
        let neon_result = self.object.map_neon_object(|cx, neon_object| {
            neon_object
                .get::<JsValue, _, _>(cx, field_name)
                .map_err(|_| CubeError::internal(format!("Field `{}` not found", field_name)))
        })??;
        Ok(NativeObjectHandle::new(NeonObject::new(
            self.object.context.clone(),
            neon_result,
        )))
    }

    fn has_field(&self, field_name: &str) -> Result<bool, CubeError> {
        let result =
            self.object
                .map_neon_object(|cx, neon_object| -> Result<bool, CubeError> {
                    let res = neon_object
                        .get_opt::<JsValue, _, _>(cx, field_name)
                        .map_err(|_| {
                            CubeError::internal(format!(
                                "Error while getting field `{}` not found",
                                field_name
                            ))
                        })?
                        .is_some();
                    Ok(res)
                })??;
        Ok(result)
    }

    fn set_field(
        &self,
        field_name: &str,
        value: NativeObjectHandle<NeonInnerTypes<C>>,
    ) -> Result<bool, CubeError> {
        let value = value.into_object().into_object();
        self.object.map_neon_object::<_, _>(|cx, object| {
            object
                .set(cx, field_name, value)
                .map_err(|_| CubeError::internal(format!("Error setting field {}", field_name)))
        })?
    }
    fn get_own_property_names(
        &self,
    ) -> Result<Vec<NativeObjectHandle<NeonInnerTypes<C>>>, CubeError> {
        let neon_array = self.object.map_neon_object(|cx, neon_object| {
            let neon_array = neon_object.get_own_property_names(cx).map_err(|_| {
                CubeError::internal("Cannot get own properties not found".to_string())
            })?;

            neon_array
                .to_vec(cx)
                .map_err(|_| CubeError::internal("Failed to convert array".to_string()))
        })??;
        Ok(neon_array
            .into_iter()
            .map(|o| NativeObjectHandle::new(NeonObject::new(self.object.context.clone(), o)))
            .collect())
    }
    fn call_method(
        &self,
        method: &str,
        args: Vec<NativeObjectHandle<NeonInnerTypes<C>>>,
    ) -> Result<NativeObjectHandle<NeonInnerTypes<C>>, CubeError> {
        let neon_args = args
            .into_iter()
            .map(|arg| -> Result<_, CubeError> { Ok(arg.into_object().get_object()) })
            .collect::<Result<Vec<_>, _>>()?;

        let neon_reuslt = self.object.map_neon_object(|cx, neon_object| {
            let neon_method = neon_object
                .get::<JsFunction, _, _>(cx, method)
                .map_err(|_| CubeError::internal(format!("Method `{}` not found", method)))?;
            neon_method
                .call(cx, *neon_object, neon_args)
                .map_err(|err| {
                    CubeError::internal(format!(
                        "Failed to call method `{} {} {:?}",
                        method, err, err
                    ))
                })
        })??;
        Ok(NativeObjectHandle::new(NeonObject::new(
            self.object.context.clone(),
            neon_reuslt,
        )))
    }
}
