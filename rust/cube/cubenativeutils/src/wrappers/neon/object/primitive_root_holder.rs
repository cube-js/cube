use crate::wrappers::neon::context::ContextHolder;
use crate::CubeError;
use neon::prelude::*;
use neon::sys::bindings as napi;

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
/// just to compute its UTF-8 length. The UTF-16 length is O(1) and bounds the UTF-8 one.
pub(crate) fn read_js_string<'cx, C: Context<'cx>>(
    cx: &mut C,
    value: Handle<'cx, JsString>,
) -> String {
    let env = cx.to_raw();
    let raw = value.to_raw();
    let read = |buf: &mut Vec<u8>| -> usize {
        let mut written = 0usize;
        // SAFETY: `buf` has `capacity()` writable bytes and N-API writes at most `bufsize`
        // bytes including the NUL terminator; `raw` is a live string in `env`.
        unsafe {
            napi::get_value_string_utf8(
                env,
                raw,
                buf.as_mut_ptr().cast(),
                buf.capacity(),
                &mut written,
            )
        }
        .expect("napi_get_value_string_utf8 on a string");
        written
    };

    let mut utf16_len = 0usize;
    // SAFETY: a null buffer asks N-API only for the length, written to `utf16_len`.
    unsafe { napi::get_value_string_utf16(env, raw, std::ptr::null_mut(), 0, &mut utf16_len) }
        .expect("napi_get_value_string_utf16 on a string");

    // An ASCII string is exactly `utf16_len` bytes. `written == utf16_len` alone doesn't prove
    // the string was not truncated (`"éa"` fills 2 bytes with `é`), the ASCII check does.
    let mut buf = Vec::with_capacity(utf16_len + 1);
    let written = read(&mut buf);
    // SAFETY: N-API initialized the first `written` bytes.
    unsafe { buf.set_len(written) };
    if written != utf16_len || !buf.is_ascii() {
        // A UTF-16 unit takes at most 3 UTF-8 bytes (a lone surrogate becomes U+FFFD).
        buf = Vec::with_capacity(utf16_len * 3 + 1);
        let written = read(&mut buf);
        // SAFETY: N-API initialized the first `written` bytes.
        unsafe { buf.set_len(written) };
        buf.shrink_to_fit();
    }
    // SAFETY: N-API writes valid UTF-8 (lone surrogates are replaced) and only truncates on a
    // character boundary; neon's own `JsString::value` relies on the same.
    unsafe { String::from_utf8_unchecked(buf) }
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
