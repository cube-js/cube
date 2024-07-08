use crate::wrappers::object::{NativeObject, NativeObjectHolder};
use neon::prelude::*;

pub trait NeonDeSerialize<T> {
    fn from_neon<'a, C: Context<'a>>(cx: &mut C, value: Handle<JsValue>) -> NeonResult<T>;
}

impl NeonDeSerialize<String> for String {
    fn from_neon<'a, C: Context<'a>>(cx: &mut C, value: Handle<JsValue>) -> NeonResult<String> {
        Ok(value.downcast_or_throw::<JsString, _>(cx)?.value(cx))
    }
}

impl NeonDeSerialize<i64> for i64 {
    fn from_neon<'a, C: Context<'a>>(cx: &mut C, value: Handle<JsValue>) -> NeonResult<i64> {
        Ok(value.downcast_or_throw::<JsNumber, _>(cx)?.value(cx) as i64)
    }
}

impl NeonDeSerialize<f64> for f64 {
    fn from_neon<'a, C: Context<'a>>(cx: &mut C, value: Handle<JsValue>) -> NeonResult<f64> {
        Ok(value.downcast_or_throw::<JsNumber, _>(cx)?.value(cx) as f64)
    }
}

impl NeonDeSerialize<bool> for bool {
    fn from_neon<'a, C: Context<'a>>(cx: &mut C, value: Handle<JsValue>) -> NeonResult<bool> {
        Ok(value.downcast_or_throw::<JsBoolean, _>(cx)?.value(cx) as bool)
    }
}

impl<T: NativeObjectHolder> NeonDeSerialize<T> for T {
    fn from_neon<'a, C: Context<'a>>(cx: &mut C, value: Handle<JsValue>) -> NeonResult<T> {
        let js_object = value.downcast_or_throw::<JsObject, _>(cx)?.root(cx);
        let native_obj = NativeObject::new(cx.channel(), js_object);
        Ok(T::new_from_native(native_obj))
    }
}

impl<T: NeonDeSerialize<T>> NeonDeSerialize<Vec<T>> for Vec<T> {
    fn from_neon<'a, C: Context<'a>>(cx: &mut C, value: Handle<JsValue>) -> NeonResult<Vec<T>> {
        let js_array = value.downcast_or_throw::<JsArray, _>(cx)?;
        let res = js_array
            .to_vec(cx)?
            .into_iter()
            .map(|v| -> NeonResult<T> { T::from_neon(cx, v) })
            .collect::<NeonResult<Vec<_>>>()?;
        Ok(res)
    }
}
