use super::{NeonObject, NeonTypeHandle};
use crate::wrappers::neon::inner_types::NeonInnerTypes;
use std::marker::PhantomData;

use crate::wrappers::object::{NativeBoolean, NativeBox, NativeNumber, NativeString, NativeType};
use cubesql::CubeError;
use neon::prelude::*;
use std::ops::Deref;

pub struct NeonString<'cx: 'static, C: Context<'cx>> {
    object: NeonTypeHandle<'cx, C, JsString>,
}

impl<'cx, C: Context<'cx>> NeonString<'cx, C> {
    pub fn new(object: NeonTypeHandle<'cx, C, JsString>) -> Self {
        Self { object }
    }
}

impl<'cx, C: Context<'cx> + 'cx> NativeType<NeonInnerTypes<'cx, C>> for NeonString<'cx, C> {
    fn into_object(self) -> NeonObject<'cx, C> {
        self.object.upcast()
    }
}

impl<'cx, C: Context<'cx> + 'cx> NativeString<NeonInnerTypes<'cx, C>> for NeonString<'cx, C> {
    fn value(&self) -> Result<String, CubeError> {
        self.object
            .map_neon_object::<_, _>(|cx, object| Ok(object.value(cx)))
    }
}

pub struct NeonNumber<'cx: 'static, C: Context<'cx>> {
    object: NeonTypeHandle<'cx, C, JsNumber>,
}

impl<'cx, C: Context<'cx>> NeonNumber<'cx, C> {
    pub fn new(object: NeonTypeHandle<'cx, C, JsNumber>) -> Self {
        Self { object }
    }
}

impl<'cx, C: Context<'cx> + 'cx> NativeType<NeonInnerTypes<'cx, C>> for NeonNumber<'cx, C> {
    fn into_object(self) -> NeonObject<'cx, C> {
        self.object.upcast()
    }
}

impl<'cx, C: Context<'cx> + 'cx> NativeNumber<NeonInnerTypes<'cx, C>> for NeonNumber<'cx, C> {
    fn value(&self) -> Result<f64, CubeError> {
        self.object
            .map_neon_object::<_, _>(|cx, object| Ok(object.value(cx)))
    }
}

pub struct NeonBoolean<'cx: 'static, C: Context<'cx>> {
    object: NeonTypeHandle<'cx, C, JsBoolean>,
}

impl<'cx, C: Context<'cx>> NeonBoolean<'cx, C> {
    pub fn new(object: NeonTypeHandle<'cx, C, JsBoolean>) -> Self {
        Self { object }
    }
}

impl<'cx, C: Context<'cx> + 'cx> NativeType<NeonInnerTypes<'cx, C>> for NeonBoolean<'cx, C> {
    fn into_object(self) -> NeonObject<'cx, C> {
        self.object.upcast()
    }
}

impl<'cx, C: Context<'cx> + 'cx> NativeBoolean<NeonInnerTypes<'cx, C>> for NeonBoolean<'cx, C> {
    fn value(&self) -> Result<bool, CubeError> {
        self.object
            .map_neon_object::<_, _>(|cx, object| Ok(object.value(cx)))
    }
}

pub struct NeonBox<'cx: 'static, C: Context<'cx>, T: 'static> {
    object: NeonTypeHandle<'cx, C, JsBox<T>>,
    _marker: PhantomData<T>,
}

impl<'cx: 'static, C: Context<'cx>, T: 'static> NeonBox<'cx, C, T> {
    pub fn new(object: NeonTypeHandle<'cx, C, JsBox<T>>) -> Self {
        Self {
            object,
            _marker: PhantomData::default(),
        }
    }
}

impl<'cx: 'static, C: Context<'cx> + 'cx, T: 'static> NativeType<NeonInnerTypes<'cx, C>>
    for NeonBox<'cx, C, T>
{
    fn into_object(self) -> NeonObject<'cx, C> {
        self.object.upcast()
    }
}

impl<'cx: 'static, C: Context<'cx> + 'cx, T: 'static> NativeBox<NeonInnerTypes<'cx, C>, T>
    for NeonBox<'cx, C, T>
{
    fn deref_value(&self) -> &T {
        self.object.get_object_ref().deref()
    }
}
