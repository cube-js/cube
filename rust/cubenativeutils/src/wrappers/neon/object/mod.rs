pub mod base_types;
pub mod neon_array;
pub mod neon_function;
pub mod neon_struct;

use self::{
    base_types::{NeonBoolean, NeonNumber, NeonString},
    neon_array::NeonArray,
    neon_function::NeonFunction,
    neon_struct::NeonStruct,
};
use super::inner_types::NeonInnerTypes;
use crate::wrappers::{neon::context::ContextHolder, object::NativeObject};
use cubesql::CubeError;
use neon::prelude::*;

pub struct NeonObject<'cx: 'static, C: Context<'cx>> {
    context: ContextHolder<'cx, C>,
    object: Handle<'cx, JsValue>,
}

impl<'cx: 'static, C: Context<'cx> + 'cx> NeonObject<'cx, C> {
    pub fn new(context: ContextHolder<'cx, C>, object: Handle<'cx, JsValue>) -> Self {
        Self { context, object }
    }

    pub fn get_object(&self) -> Handle<'cx, JsValue> {
        self.object.clone()
    }

    pub fn get_object_ref(&self) -> &Handle<'cx, JsValue> {
        &self.object
    }

    pub fn into_object(self) -> Handle<'cx, JsValue> {
        self.object
    }

    pub fn map_neon_object<T, F>(&self, f: F) -> T
    where
        F: FnOnce(&mut C, &Handle<'cx, JsValue>) -> T,
    {
        self.context.with_context(|cx| f(cx, &self.object))
    }
    pub fn map_downcast_neon_object<JT: Value, T, F>(&self, f: F) -> Result<T, CubeError>
    where
        F: FnOnce(&mut C, &Handle<'cx, JT>) -> Result<T, CubeError>,
    {
        self.context.with_context(|cx| {
            let obj = self
                .object
                .downcast::<JT, _>(cx)
                .map_err(|_| CubeError::internal("Downcast error".to_string()))?;
            f(cx, &obj)
        })
    }

    pub fn is_a<U: Value>(&self) -> bool {
        self.context.with_context(|cx| self.object.is_a::<U, _>(cx))
    }
}

impl<'cx: 'static, C: Context<'cx> + 'cx> NativeObject<NeonInnerTypes<'cx, C>>
    for NeonObject<'cx, C>
{
    fn get_context(&self) -> ContextHolder<'cx, C> {
        self.context.clone()
    }

    fn into_struct(self) -> Result<NeonStruct<'cx, C>, CubeError> {
        if !self.is_a::<JsObject>() {
            return Err(CubeError::internal(format!(
                "NeonObject is not the JsObject"
            )));
        }
        Ok(NeonStruct::new(self))
    }
    fn into_function(self) -> Result<NeonFunction<'cx, C>, CubeError> {
        if !self.is_a::<JsFunction>() {
            return Err(CubeError::internal(format!(
                "NeonObject is not the JsFunction"
            )));
        }
        Ok(NeonFunction::new(self))
    }
    fn into_array(self) -> Result<NeonArray<'cx, C>, CubeError> {
        if !self.is_a::<JsArray>() {
            return Err(CubeError::internal(format!(
                "NeonObject is not the JsArray"
            )));
        }
        Ok(NeonArray::new(self))
    }
    fn into_string(self) -> Result<NeonString<'cx, C>, CubeError> {
        if !self.is_a::<JsString>() {
            return Err(CubeError::internal(format!(
                "NeonObject is not the JsString"
            )));
        }
        Ok(NeonString::new(self))
    }
    fn into_number(self) -> Result<NeonNumber<'cx, C>, CubeError> {
        if !self.is_a::<JsNumber>() {
            return Err(CubeError::internal(format!(
                "NeonObject is not the JsNumber"
            )));
        }
        Ok(NeonNumber::new(self))
    }
    fn into_boolean(self) -> Result<NeonBoolean<'cx, C>, CubeError> {
        if !self.is_a::<JsBoolean>() {
            return Err(CubeError::internal(format!(
                "NeonObject is not the JsBoolean"
            )));
        }
        Ok(NeonBoolean::<C>::new(self))
    }

    fn is_null(&self) -> bool {
        self.is_a::<JsNull>()
    }

    fn is_undefined(&self) -> bool {
        self.is_a::<JsUndefined>()
    }
}

impl<'cx: 'static, C: Context<'cx>> Clone for NeonObject<'cx, C> {
    fn clone(&self) -> Self {
        Self {
            context: self.context.clone(),
            object: self.object.clone(),
        }
    }
}
