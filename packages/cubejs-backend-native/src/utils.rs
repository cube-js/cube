use neon::prelude::*;

#[inline(always)]
pub fn call_method<'a>(
    cx: &mut impl Context<'a>,
    this: Handle<'a, JsFunction>,
    method_name: &str,
    args: impl IntoIterator<Item = Handle<'a, JsValue>>,
) -> JsResult<'a, JsValue> {
    let method: Handle<JsFunction> = this.get(cx, method_name)?.downcast_or_throw(cx)?;
    method.call(cx, this, args)
}

#[inline(always)]
pub fn bind_method<'a>(
    cx: &mut impl Context<'a>,
    fn_value: Handle<'a, JsFunction>,
    this: Handle<'a, JsValue>,
) -> JsResult<'a, JsValue> {
    call_method(cx, fn_value, "bind", vec![this])
}
