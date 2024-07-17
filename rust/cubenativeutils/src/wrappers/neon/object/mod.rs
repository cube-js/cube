pub mod base_types;
pub mod neon_array;
pub mod neon_struct;

use self::base_types::{NeonBoolean, NeonNumber, NeonString};
use self::neon_array::NeonArray;
use self::neon_struct::NeonStruct;
use crate::wrappers::context::NativeContextHolder;
use crate::wrappers::neon::context::WeakContextHolder;
use crate::wrappers::object::{
    NativeArray, NativeBoolean, NativeNumber, NativeObject, NativeString, NativeStruct,
};
use cubesql::CubeError;
use neon::prelude::*;
use std::any::Any;

pub struct NeonObject<C: Context<'static>> {
    context: WeakContextHolder<C>,
    object: Handle<'static, JsValue>,
}

impl<C: Context<'static> + 'static> NeonObject<C> {
    pub fn new(context: WeakContextHolder<C>, object: Handle<'static, JsValue>) -> Box<Self> {
        Box::new(Self { context, object })
    }

    pub fn map_native<T, F>(object: &Box<dyn NativeObject>, f: F) -> Result<T, CubeError>
    where
        F: FnOnce(&Self) -> T,
    {
        object
            .as_any()
            .downcast_ref::<Self>()
            .map(f)
            .ok_or(CubeError::internal(
                "NativeObject is not NeonObject".to_string(),
            ))
    }

    pub fn get_object(&self) -> Handle<'static, JsValue> {
        self.object.clone()
    }

    pub fn into_object(self) -> Handle<'static, JsValue> {
        self.object
    }

    pub fn map_neon_object<T, F>(&self, f: F) -> Result<T, CubeError>
    where
        F: FnOnce(&mut C, &Handle<'static, JsValue>) -> T,
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

    pub fn get_context(&self) -> WeakContextHolder<C> {
        self.context.clone()
    }
}

impl<C: Context<'static> + 'static> NativeObject for NeonObject<C> {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn into_any(self: Box<Self>) -> Box<dyn Any> {
        self
    }
    fn get_context(&self) -> Result<NativeContextHolder, CubeError> {
        let context_box = Box::new(self.context.try_upgrade()?);
        Ok(NativeContextHolder::new(context_box))
    }
    fn into_struct(self: Box<Self>) -> Result<Box<dyn NativeStruct>, CubeError> {
        if !self.is_a::<JsObject>()? {
            return Err(CubeError::internal(format!(
                "NeonObject is not the JsObject"
            )));
        }
        Ok(NeonStruct::<C>::new(self))
    }
    fn into_array(self: Box<Self>) -> Result<Box<dyn NativeArray>, CubeError> {
        if !self.is_a::<JsArray>()? {
            return Err(CubeError::internal(format!(
                "NeonObject is not the JsArray"
            )));
        }
        Ok(NeonArray::<C>::new(self))
    }
    fn into_string(self: Box<Self>) -> Result<Box<dyn NativeString>, CubeError> {
        if !self.is_a::<JsString>()? {
            return Err(CubeError::internal(format!(
                "NeonObject is not the JsString"
            )));
        }
        Ok(NeonString::<C>::new(self))
    }
    fn into_number(self: Box<Self>) -> Result<Box<dyn NativeNumber>, CubeError> {
        if !self.is_a::<JsNumber>()? {
            return Err(CubeError::internal(format!(
                "NeonObject is not the JsNumber"
            )));
        }
        Ok(NeonNumber::<C>::new(self))
    }
    fn into_boolean(self: Box<Self>) -> Result<Box<dyn NativeBoolean>, CubeError> {
        if !self.is_a::<JsBoolean>()? {
            return Err(CubeError::internal(format!(
                "NeonObject is not the JsBoolean"
            )));
        }
        Ok(NeonBoolean::<C>::new(self))
    }

    fn is_null(&self) -> bool {
        self.is_a::<JsNull>().unwrap()
    }

    fn is_undefined(&self) -> bool {
        self.is_a::<JsUndefined>().unwrap()
    }
}

impl<C: Context<'static>> Clone for NeonObject<C> {
    fn clone(&self) -> Self {
        Self {
            context: self.context.clone(),
            object: self.object.clone(),
        }
    }
}
