use crate::wrappers::neon::context::ContextHolder;
use crate::CubeError;
use neon::prelude::*;
use neon::sys::bindings as napi;
use std::{borrow::Cow, mem::MaybeUninit};

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
        read_js_string(cx, *value)
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

/// Neon's `JsString::value` makes two N-API calls, the first of which walks the whole string
/// just to compute its UTF-8 length. The UTF-16 length is O(1) and is the exact UTF-8 length of
/// an ASCII string, so ASCII strings are read with a single copy; others fall back to neon's way.
pub(crate) fn read_js_string<'cx, C: Context<'cx>>(
    cx: &mut C,
    value: Handle<'cx, JsString>,
) -> String {
    let utf16_len = value.size_utf16(cx);
    let mut buf = Vec::with_capacity(utf16_len + 1);
    let bytes = copy_js_utf8(cx, value, buf.spare_capacity_mut());

    // A full ASCII copy proves the string wasn't truncated; length alone doesn't ("éa").
    if bytes.len() != utf16_len || !bytes.is_ascii() {
        return value.value(cx);
    }
    let written = bytes.len();
    // SAFETY: copy_js_utf8 initialized the first `written` bytes, all checked as ASCII.
    unsafe {
        buf.set_len(written);
        String::from_utf8_unchecked(buf)
    }
}

/// Borrows the complete UTF-8 string from `buf` when it fits; otherwise reads an owned copy.
pub(crate) fn read_js_string_into<'cx, 'buf, C: Context<'cx>>(
    cx: &mut C,
    value: Handle<'cx, JsString>,
    buf: &'buf mut [MaybeUninit<u8>],
) -> Cow<'buf, [u8]> {
    let capacity = buf.len();
    let bytes = copy_js_utf8(cx, value, buf);
    // N-API stops before a character that doesn't fit, including its NUL terminator.
    // Only trust a copy with enough room left for any UTF-8 character.
    if bytes.len() + char::MAX_LEN_UTF8 < capacity {
        Cow::Borrowed(bytes)
    } else {
        Cow::Owned(read_js_string(cx, value).into_bytes())
    }
}

/// Copies into spare capacity and returns its initialized prefix, possibly truncated.
fn copy_js_utf8<'cx, 'buf, C: Context<'cx>>(
    cx: &mut C,
    value: Handle<'cx, JsString>,
    buf: &'buf mut [MaybeUninit<u8>],
) -> &'buf [u8] {
    if buf.is_empty() {
        return &[];
    }
    let mut written = 0usize;
    // SAFETY: `buf` is writable for `buf.len()` bytes, which N-API never exceeds (NUL included).
    unsafe {
        napi::get_value_string_utf8(
            cx.to_raw(),
            value.to_raw(),
            buf.as_mut_ptr().cast(),
            buf.len(),
            &mut written,
        )
    }
    .expect("napi_get_value_string_utf8 on a string");
    // SAFETY: N-API initialized the first `written` bytes without exceeding `buf.len()`.
    // The returned slice borrows the buffer, so it cannot outlive or alias a write to it.
    unsafe { std::slice::from_raw_parts(buf.as_ptr().cast(), written) }
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

    /// The value was already copied out of JS in `new`; reading it back does not
    /// need the context.
    pub fn value_ref(&self) -> &V::NativeType {
        &self.value
    }

    pub fn into_value(self) -> V::NativeType {
        self.value
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
