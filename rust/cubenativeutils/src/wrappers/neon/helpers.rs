use neon::prelude::*;
pub fn js_class_name<'cx, C: Context<'cx>>(cx: &mut C, v: &Handle<'cx, JsValue>) -> Option<String> {
    v.downcast::<JsObject, _>(cx).ok().and_then(|obj| {
        obj.get::<JsFunction, _, _>(cx, "constructor")
            .ok()
            .and_then(|ctor| {
                ctor.get::<JsString, _, _>(cx, "name")
                    .ok()
                    .map(|name| name.value(cx))
            })
    })
}
