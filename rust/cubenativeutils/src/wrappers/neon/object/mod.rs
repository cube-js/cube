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

pub struct NeonTypeHandle<C: Context<'static>, V: Value + 'static> {
    context: ContextHolder<C>,
    object: Handle<'static, V>,
}

impl<C: Context<'static> + 'static, V: Value + 'static> NeonTypeHandle<C, V> {
    pub fn new(context: ContextHolder<C>, object: Handle<'static, V>) -> Self {
        Self { context, object }
    }

    fn get_context(&self) -> ContextHolder<C> {
        self.context.clone()
    }

    pub fn get_object(&self) -> Handle<'static, V> {
        self.object
    }

    pub fn get_object_ref(&self) -> &Handle<'static, V> {
        &self.object
    }

    pub fn into_object(self) -> Handle<'static, V> {
        self.object
    }

    pub fn upcast(&self) -> NeonObject<C> {
        NeonObject::new(self.context.clone(), self.object.upcast())
    }

    pub fn map_neon_object<T, F>(&self, f: F) -> Result<T, CubeError>
    where
        F: FnOnce(&mut C, &Handle<'static, V>) -> T,
    {
        self.context.with_context(|cx| f(cx, &self.object))
    }

    pub fn map_downcast_neon_object<JT: Value, T, F>(&self, f: F) -> Result<T, CubeError>
    where
        F: FnOnce(&mut C, &Handle<'static, JT>) -> Result<T, CubeError>,
    {
        self.context.with_context(|cx| {
            let obj = self
                .object
                .downcast::<JT, _>(cx)
                .map_err(|_| CubeError::internal("Downcast error".to_string()))?;
            f(cx, &obj)
        })?
    }

    pub fn is_a<U: Value>(&self) -> Result<bool, CubeError> {
        self.context.with_context(|cx| self.object.is_a::<U, _>(cx))
    }
}

impl<C: Context<'static>, V: Value + 'static> Clone for NeonTypeHandle<C, V> {
    fn clone(&self) -> Self {
        Self {
            context: self.context.clone(),
            object: self.object,
        }
    }
}

pub struct NeonObject<C: Context<'static>> {
    context: ContextHolder<C>,
    object: Handle<'static, JsValue>,
}

impl<C: Context<'static> + 'static> NeonObject<C> {
    pub fn new(context: ContextHolder<C>, object: Handle<'static, JsValue>) -> Self {
        Self { context, object }
    }

    pub fn get_object(&self) -> Handle<'static, JsValue> {
        self.object
    }

    pub fn get_object_ref(&self) -> &Handle<'static, JsValue> {
        &self.object
    }

    pub fn into_object(self) -> Handle<'static, JsValue> {
        self.object
    }

    pub fn is_a<U: Value>(&self) -> Result<bool, CubeError> {
        self.context.with_context(|cx| self.object.is_a::<U, _>(cx))
    }

    pub fn downcast<U: Value>(&self) -> Result<NeonTypeHandle<C, U>, CubeError> {
        let obj = self.context.with_context(|cx| {
            self.object
                .downcast::<U, _>(cx)
                .map_err(|_| CubeError::internal("Downcast error".to_string()))
        })??;
        Ok(NeonTypeHandle::new(self.context.clone(), obj))
    }

    pub fn downcast_with_err_msg<U: Value>(
        &self,
        msg: &str,
    ) -> Result<NeonTypeHandle<C, U>, CubeError> {
        let obj = self.context.with_context(|cx| {
            self.object
                .downcast::<U, _>(cx)
                .map_err(|_| CubeError::internal(msg.to_string()))
        })??;
        Ok(NeonTypeHandle::new(self.context.clone(), obj))
    }
}

impl<C: Context<'static> + 'static> NativeObject<NeonInnerTypes<C>> for NeonObject<C> {
    fn get_context(&self) -> ContextHolder<C> {
        self.context.clone()
    }

    fn into_struct(self) -> Result<NeonStruct<C>, CubeError> {
        let obj = self.downcast_with_err_msg::<JsObject>("NeonObject is not the JsObject")?;
        Ok(NeonStruct::new(obj))
    }
    fn into_function(self) -> Result<NeonFunction<C>, CubeError> {
        let obj = self.downcast_with_err_msg::<JsFunction>("NeonObject is not the JsArray")?;
        Ok(NeonFunction::new(obj))
    }
    fn into_array(self) -> Result<NeonArray<C>, CubeError> {
        let obj = self.downcast_with_err_msg::<JsArray>("NeonObject is not the JsArray")?;
        Ok(NeonArray::new(obj))
    }
    fn into_string(self) -> Result<NeonString<C>, CubeError> {
        let obj = self.downcast_with_err_msg::<JsString>("NeonObject is not the JsString")?;
        Ok(NeonString::new(obj))
    }
    fn into_number(self) -> Result<NeonNumber<C>, CubeError> {
        let obj = self.downcast_with_err_msg::<JsNumber>("NeonObject is not the JsNumber")?;
        Ok(NeonNumber::new(obj))
    }
    fn into_boolean(self) -> Result<NeonBoolean<C>, CubeError> {
        let obj = self.downcast_with_err_msg::<JsBoolean>("NeonObject is not the JsBoolean")?;
        Ok(NeonBoolean::new(obj))
    }

    fn is_null(&self) -> Result<bool, CubeError> {
        self.is_a::<JsNull>()
    }

    fn is_undefined(&self) -> Result<bool, CubeError> {
        self.is_a::<JsUndefined>()
    }
}

impl<C: Context<'static>> Clone for NeonObject<C> {
    fn clone(&self) -> Self {
        Self {
            context: self.context.clone(),
            object: self.object,
        }
    }
}
