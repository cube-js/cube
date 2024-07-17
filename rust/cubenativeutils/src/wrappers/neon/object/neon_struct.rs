use super::NeonObject;
use crate::wrappers::object::{
    NativeArray, NativeBoxedClone, NativeObject, NativeStruct, NativeType,
};
use crate::wrappers::object_handler::NativeObjectHandler;
use cubesql::CubeError;
use neon::prelude::*;

#[derive(Clone)]
pub struct NeonStruct<C: Context<'static>> {
    object: Box<NeonObject<C>>,
}

impl<C: Context<'static> + 'static> NeonStruct<C> {
    pub fn new(object: Box<NeonObject<C>>) -> Box<Self> {
        Box::new(Self { object })
    }
}

impl<C: Context<'static> + 'static> NativeType for NeonStruct<C> {
    fn into_object(self: Box<Self>) -> Box<dyn NativeObject> {
        self.object
    }
    fn get_object(&self) -> Box<dyn NativeObject> {
        self.object.boxed_clone()
    }
}

impl<C: Context<'static> + 'static> NativeStruct for NeonStruct<C> {
    fn get_field(&self, field_name: &str) -> Result<NativeObjectHandler, CubeError> {
        let neon_reuslt = self.object.map_neon_object(|cx, neon_object| {
            let this = neon_object
                .downcast::<JsObject, _>(cx)
                .map_err(|_| CubeError::internal(format!("Neon object is not JsObject")))?;
            this.get::<JsValue, _, _>(cx, field_name)
                .map_err(|_| CubeError::internal(format!("Field `{}` not found", field_name)))
        })??;
        Ok(NativeObjectHandler::new(NeonObject::new(
            self.object.context.clone(),
            neon_reuslt,
        )))
    }

    fn set_field(&self, field_name: &str, value: NativeObjectHandler) -> Result<bool, CubeError> {
        let value = value.downcast_object::<NeonObject<C>>()?.into_object();
        self.object
            .map_downcast_neon_object::<JsObject, _, _>(|cx, object| {
                object
                    .set(cx, field_name, value)
                    .map_err(|_| CubeError::internal(format!("Error setting field {}", field_name)))
            })
    }
    fn get_own_property_names(&self) -> Result<Vec<NativeObjectHandler>, CubeError> {
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
        })??;
        Ok(neon_array
            .into_iter()
            .map(|o| NativeObjectHandler::new(NeonObject::new(self.object.context.clone(), o)))
            .collect())
    }
    fn call_method(
        &self,
        method: &str,
        args: Vec<NativeObjectHandler>,
    ) -> Result<NativeObjectHandler, CubeError> {
        let neon_args = args
            .into_iter()
            .map(|arg| -> Result<_, CubeError> {
                let arg = arg.downcast_object::<NeonObject<C>>()?;
                Ok(arg.get_object())
            })
            .collect::<Result<Vec<_>, _>>()?;

        let neon_reuslt = self.object.map_neon_object(|cx, neon_object| {
            let this = neon_object
                .downcast::<JsObject, _>(cx)
                .map_err(|_| CubeError::internal(format!("Neon object is not JsObject")))?;
            let neon_method = this
                .get::<JsFunction, _, _>(cx, method)
                .map_err(|_| CubeError::internal(format!("Method `{}` not found", method)))?;
            neon_method
                .call(cx, this, neon_args)
                .map_err(|_| CubeError::internal(format!("Failed to call method `{}`", method)))
        })??;
        Ok(NativeObjectHandler::new(NeonObject::new(
            self.object.context.clone(),
            neon_reuslt,
        )))
    }
}
