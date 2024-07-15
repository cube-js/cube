use super::object::NeonObject;
use crate::wrappers::object::NativeObject;
use crate::wrappers::object::NativeObjectHolder;
use cubesql::CubeError;
use neon::prelude::*;

pub trait NeonSerialize {
    fn to_native<'a, C: Context<'a>>(&self, cx: &mut C) -> Resulr<Rc<dyn NativeObject>, CubeError>;
}

impl NeonSerialize for String {
    fn to_native<'a, C: Context<'a>>(&self, cx: &mut C) -> Rc<dyn NativeObject> {
        Rc::
    }
    fn to_neon<'a, C: Context<'a>>(&self, &mut s: S) -> NeonResult<Handle<'a, JsValue>> {
        s.serialize(self);
        Ok(cx.string(self).upcast::<JsValue>())
    }
}

impl NeonSerialize for i64 {
    fn to_neon<'a, C: Context<'a>>(&self, cx: &mut C) -> NeonResult<Handle<'a, JsValue>> {
        Ok(cx.number(self.to_owned() as f64).upcast::<JsValue>())
    }
}

impl NeonSerialize for i32 {
    fn to_neon<'a, C: Context<'a>>(&self, cx: &mut C) -> NeonResult<Handle<'a, JsValue>> {
        Ok(cx.number(self.to_owned() as f64).upcast::<JsValue>())
    }
}

impl NeonSerialize for u64 {
    fn to_neon<'a, C: Context<'a>>(&self, cx: &mut C) -> NeonResult<Handle<'a, JsValue>> {
        Ok(cx.number(self.to_owned() as f64).upcast::<JsValue>())
    }
}

impl NeonSerialize for u32 {
    fn to_neon<'a, C: Context<'a>>(&self, cx: &mut C) -> NeonResult<Handle<'a, JsValue>> {
        Ok(cx.number(self.to_owned() as f64).upcast::<JsValue>())
    }
}

impl NeonSerialize for f64 {
    fn to_neon<'a, C: Context<'a>>(&self, cx: &mut C) -> NeonResult<Handle<'a, JsValue>> {
        Ok(cx.number(self.to_owned()).upcast::<JsValue>())
    }
}

impl NeonSerialize for f32 {
    fn to_neon<'a, C: Context<'a>>(&self, cx: &mut C) -> NeonResult<Handle<'a, JsValue>> {
        Ok(cx.number(self.to_owned() as f64).upcast::<JsValue>())
    }
}

impl NeonSerialize for bool {
    fn to_neon<'a, C: Context<'a>>(&self, cx: &mut C) -> NeonResult<Handle<'a, JsValue>> {
        Ok(cx.boolean(self.to_owned()).upcast::<JsValue>())
    }
}

impl<T: NativeObjectHolder> NeonSerialize for T {
    fn to_neon<'a, C: Context<'a>>(&self, cx: &mut C) -> NeonResult<Handle<'a, JsValue>> {
        Ok(self
            .get_native_object()
            .get_object()
            .to_inner(cx)
            .upcast::<JsValue>())
    }
}
