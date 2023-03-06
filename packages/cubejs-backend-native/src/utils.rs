use neon::prelude::*;

#[inline(always)]
pub fn call_method<'a, AS>(
    cx: &mut impl Context<'a>,
    this: Handle<'a, JsFunction>,
    method_name: &str,
    args: AS,
) -> JsResult<'a, JsValue>
where
    AS: AsRef<[Handle<'a, JsValue>]>,
{
    let method: Handle<JsFunction> = this.get(cx, method_name)?;
    method.call(cx, this, args)
}

#[inline(always)]
pub fn bind_method<'a>(
    cx: &mut impl Context<'a>,
    fn_value: Handle<'a, JsFunction>,
    this: Handle<'a, JsValue>,
) -> JsResult<'a, JsValue> {
    call_method(cx, fn_value, "bind", [this])
}
