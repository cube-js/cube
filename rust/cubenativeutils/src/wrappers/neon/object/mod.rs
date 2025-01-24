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

pub struct NeonTypeHandle<'cx, C: Context<'cx>, V: Value + 'cx> {
    context: ContextHolder<'cx, C>,
    object: Handle<'cx, V>,
}

impl<'cx: 'static, C: Context<'cx> + 'cx, V: Value + 'cx> NeonTypeHandle<'cx, C, V> {
    pub fn new(context: ContextHolder<'cx, C>, object: Handle<'cx, V>) -> Self {
        Self { context, object }
    }

    fn get_context(&self) -> ContextHolder<'cx, C> {
        self.context.clone()
    }

    pub fn get_object(&self) -> Handle<'cx, V> {
        self.object.clone()
    }

    pub fn get_object_ref(&self) -> &Handle<'cx, V> {
        &self.object
    }

    pub fn into_object(self) -> Handle<'cx, V> {
        self.object
    }

    pub fn upcast(&self) -> NeonObject<'cx, C> {
        NeonObject::new(self.context.clone(), self.object.upcast())
    }

    pub fn map_neon_object<T, F>(&self, f: F) -> T
    where
        F: FnOnce(&mut C, &Handle<'cx, V>) -> T,
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

impl<'cx: 'static, C: Context<'cx>, V: Value + 'cx> Clone for NeonTypeHandle<'cx, C, V> {
    fn clone(&self) -> Self {
        Self {
            context: self.context.clone(),
            object: self.object.clone(),
        }
    }
}

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

    pub fn is_a<U: Value>(&self) -> bool {
        self.context.with_context(|cx| self.object.is_a::<U, _>(cx))
    }

    pub fn downcast<U: Value>(&self) -> Result<NeonTypeHandle<'cx, C, U>, CubeError> {
        let obj = self.context.with_context(|cx| {
            self.object
                .downcast::<U, _>(cx)
                .map_err(|_| CubeError::internal("Downcast error".to_string()))
        })?;
        Ok(NeonTypeHandle::new(self.context.clone(), obj))
    }

    pub fn downcast_with_err_msg<U: Value>(
        &self,
        msg: &str,
    ) -> Result<NeonTypeHandle<'cx, C, U>, CubeError> {
        let obj = self.context.with_context(|cx| {
            self.object
                .downcast::<U, _>(cx)
                .map_err(|_| CubeError::internal(msg.to_string()))
        })?;
        Ok(NeonTypeHandle::new(self.context.clone(), obj))
    }
}

impl<'cx: 'static, C: Context<'cx> + 'cx> NativeObject<NeonInnerTypes<'cx, C>>
    for NeonObject<'cx, C>
{
    fn get_context(&self) -> ContextHolder<'cx, C> {
        self.context.clone()
    }

    fn into_struct(self) -> Result<NeonStruct<'cx, C>, CubeError> {
        let obj = self.downcast_with_err_msg::<JsObject>("NeonObject is not the JsObject")?;
        Ok(NeonStruct::new(obj))
    }
    fn into_function(self) -> Result<NeonFunction<'cx, C>, CubeError> {
        let obj = self.downcast_with_err_msg::<JsFunction>("NeonObject is not the JsArray")?;
        Ok(NeonFunction::new(obj))
    }
    fn into_array(self) -> Result<NeonArray<'cx, C>, CubeError> {
        let obj = self.downcast_with_err_msg::<JsArray>("NeonObject is not the JsArray")?;
        Ok(NeonArray::new(obj))
    }
    fn into_string(self) -> Result<NeonString<'cx, C>, CubeError> {
        let obj = self.downcast_with_err_msg::<JsString>("NeonObject is not the JsString")?;
        Ok(NeonString::new(obj))
    }
    fn into_number(self) -> Result<NeonNumber<'cx, C>, CubeError> {
        let obj = self.downcast_with_err_msg::<JsNumber>("NeonObject is not the JsNumber")?;
        Ok(NeonNumber::new(obj))
    }
    fn into_boolean(self) -> Result<NeonBoolean<'cx, C>, CubeError> {
        let obj = self.downcast_with_err_msg::<JsBoolean>("NeonObject is not the JsBoolean")?;
        Ok(NeonBoolean::new(obj))
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
