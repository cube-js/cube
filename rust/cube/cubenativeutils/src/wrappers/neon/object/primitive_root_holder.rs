use crate::wrappers::neon::context::ContextHolder;
use crate::CubeError;
use neon::prelude::*;

pub trait NeonPrimitiveMapping: Value {
    type NativeType: Clone;
    fn from_neon<C: Context<'static> + 'static>(
        cx: &mut C,
        value: &Handle<'static, Self>,
    ) -> Self::NativeType;
    fn to_neon<C: Context<'static> + 'static>(
        cx: &mut C,
        value: &Self::NativeType,
    ) -> Handle<'static, Self>;

    fn is_null(&self) -> bool {
        false
    }
    fn is_undefined(&self) -> bool {
        false
    }
}

impl NeonPrimitiveMapping for JsBoolean {
    type NativeType = bool;
    fn from_neon<C: Context<'static> + 'static>(
        cx: &mut C,
        value: &Handle<'static, Self>,
    ) -> Self::NativeType {
        value.value(cx)
    }

    fn to_neon<C: Context<'static> + 'static>(
        cx: &mut C,
        value: &Self::NativeType,
    ) -> Handle<'static, Self> {
        cx.boolean(value.clone())
    }
}

impl NeonPrimitiveMapping for JsNumber {
    type NativeType = f64;
    fn from_neon<C: Context<'static> + 'static>(
        cx: &mut C,
        value: &Handle<'static, Self>,
    ) -> Self::NativeType {
        value.value(cx)
    }
    fn to_neon<C: Context<'static> + 'static>(
        cx: &mut C,
        value: &Self::NativeType,
    ) -> Handle<'static, Self> {
        cx.number(value.clone())
    }
}

impl NeonPrimitiveMapping for JsString {
    type NativeType = String;
    fn from_neon<C: Context<'static> + 'static>(
        cx: &mut C,
        value: &Handle<'static, Self>,
    ) -> Self::NativeType {
        value.value(cx)
    }
    fn to_neon<C: Context<'static> + 'static>(
        cx: &mut C,
        value: &Self::NativeType,
    ) -> Handle<'static, Self> {
        cx.string(value)
    }
}

impl NeonPrimitiveMapping for JsNull {
    type NativeType = ();
    fn from_neon<C: Context<'static> + 'static>(
        _cx: &mut C,
        _value: &Handle<'static, Self>,
    ) -> Self::NativeType {
    }
    fn to_neon<C: Context<'static> + 'static>(
        cx: &mut C,
        _value: &Self::NativeType,
    ) -> Handle<'static, Self> {
        cx.null()
    }

    fn is_null(&self) -> bool {
        true
    }
}

impl NeonPrimitiveMapping for JsUndefined {
    type NativeType = ();
    fn from_neon<C: Context<'static> + 'static>(
        _cx: &mut C,
        _value: &Handle<'static, Self>,
    ) -> Self::NativeType {
    }
    fn to_neon<C: Context<'static> + 'static>(
        cx: &mut C,
        _value: &Self::NativeType,
    ) -> Handle<'static, Self> {
        cx.undefined()
    }
    fn is_undefined(&self) -> bool {
        false
    }
}

pub struct PrimitiveNeonTypeHolder<C: Context<'static>, V: NeonPrimitiveMapping + 'static> {
    context: ContextHolder<C>,
    value: V::NativeType,
}

impl<C: Context<'static> + 'static, V: Value + NeonPrimitiveMapping + 'static>
    PrimitiveNeonTypeHolder<C, V>
{
    pub fn new(context: ContextHolder<C>, object: Handle<'static, V>, cx: &mut C) -> Self {
        let value = V::from_neon(cx, &object);
        Self { context, value }
    }

    pub fn get_context(&self) -> ContextHolder<C> {
        self.context.clone()
    }

    pub fn map_neon_object<T, F>(&self, f: F) -> Result<T, CubeError>
    where
        F: FnOnce(&mut C, &Handle<'static, V>) -> Result<T, CubeError>,
    {
        self.context.with_context(|cx| {
            let object = V::to_neon(cx, &self.value);
            f(cx, &object)
        })?
    }

    pub fn into_object(self) -> Result<Handle<'static, V>, CubeError> {
        self.context.with_context(|cx| V::to_neon(cx, &self.value))
    }

    pub fn clone_to_context<CC: Context<'static> + 'static>(
        &self,
        context: &ContextHolder<CC>,
    ) -> PrimitiveNeonTypeHolder<CC, V> {
        PrimitiveNeonTypeHolder {
            context: context.clone(),
            value: self.value.clone(),
        }
    }
}

impl<C: Context<'static>, V: NeonPrimitiveMapping + 'static> Clone
    for PrimitiveNeonTypeHolder<C, V>
{
    fn clone(&self) -> Self {
        Self {
            context: self.context.clone(),
            value: self.value.clone(),
        }
    }
}
