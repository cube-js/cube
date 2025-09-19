use super::*;
use super::{
    inner_types::NeonInnerTypes,
    object::{
        base_types::*, neon_array::NeonArray, neon_function::NeonFunction, neon_struct::NeonStruct,
        NeonObject,
    },
    ContextWrapper,
};
use crate::CubeError;
use crate::{
    wrappers::{
        context::NativeContext, functions_args_def::FunctionArgsDef, object::NativeObject,
        object_handle::NativeObjectHandle, NativeContextHolder,
    },
    CubeErrorCauseType,
};
use neon::prelude::*;
use std::{
    cell::RefCell,
    marker::PhantomData,
    panic::{catch_unwind, resume_unwind, AssertUnwindSafe},
    rc::{Rc, Weak},
};

pub struct ContextHolder<C: Context<'static>> {
    context: Weak<RefCell<ContextWrapper<C>>>,
}

pub(super) type NeonFuncInnerTypes = NeonInnerTypes<FunctionContext<'static>>;

impl<C: Context<'static>> ContextHolder<C> {
    pub(super) fn new(context: Weak<RefCell<ContextWrapper<C>>>) -> Self {
        Self { context }
    }

    pub fn with_context<T, F>(&self, f: F) -> Result<T, CubeError>
    where
        F: FnOnce(&mut C) -> T,
    {
        if let Some(context) = self.context.upgrade() {
            let mut cx = context.borrow_mut();
            let res = cx.with_context(f);
            Ok(res)
        } else {
            Err(CubeError::internal(
                "Call to neon context outside of its lifetime".to_string(),
            ))
        }
    }
}

impl<C: Context<'static> + 'static> NativeContext<NeonInnerTypes<C>> for ContextHolder<C> {
    fn boolean(&self, v: bool) -> Result<NeonBoolean<C>, CubeError> {
        let obj = NeonObject::new(
            self.clone(),
            self.with_context(|cx| cx.boolean(v).upcast())?,
        )?;
        obj.into_boolean()
    }

    fn string(&self, v: String) -> Result<NeonString<C>, CubeError> {
        let obj = NeonObject::new(self.clone(), self.with_context(|cx| cx.string(v).upcast())?)?;
        obj.into_string()
    }

    fn number(&self, v: f64) -> Result<NeonNumber<C>, CubeError> {
        let obj = NeonObject::new(self.clone(), self.with_context(|cx| cx.number(v).upcast())?)?;
        obj.into_number()
    }

    fn undefined(&self) -> Result<NativeObjectHandle<NeonInnerTypes<C>>, CubeError> {
        Ok(NativeObjectHandle::new(NeonObject::new(
            self.clone(),
            self.with_context(|cx| cx.undefined().upcast())?,
        )?))
    }

    fn null(&self) -> Result<NativeObjectHandle<NeonInnerTypes<C>>, CubeError> {
        Ok(NativeObjectHandle::new(NeonObject::new(
            self.clone(),
            self.with_context(|cx| cx.null().upcast())?,
        )?))
    }

    fn empty_array(&self) -> Result<NeonArray<C>, CubeError> {
        let obj = NeonObject::new(
            self.clone(),
            self.with_context(|cx| cx.empty_array().upcast())?,
        )?;
        obj.into_array()
    }

    fn empty_struct(&self) -> Result<NeonStruct<C>, CubeError> {
        let obj = NeonObject::new(
            self.clone(),
            self.with_context(|cx| cx.empty_object().upcast())?,
        )?;
        obj.into_struct()
    }
    fn to_string_fn(&self, result: String) -> Result<NeonFunction<C>, CubeError> {
        let obj = NeonObject::new(
            self.clone(),
            self.with_context(|cx| -> Result<_, CubeError> {
                let func = JsFunction::new(cx, move |mut c| Ok(c.string(result.clone())))?;
                Ok(func.upcast())
            })??,
        )?;
        obj.into_function()
    }
    fn make_function<In, Rt, F: FunctionArgsDef<NeonFuncInnerTypes, In, Rt> + 'static>(
        &self,
        f: F,
    ) -> Result<NeonFunction<C>, CubeError> {
        let f = Rc::new(f);
        let obj = NeonObject::new(
            self.clone(),
            self.with_context(|cx| -> Result<_, CubeError> {
                let func = JsFunction::new(cx, move |cx| neon_guarded_funcion_call(cx, f.clone()))?;
                Ok(func.upcast())
            })??,
        )?;
        obj.into_function()
    }
    fn proxy<
        F: Fn(
            NativeContextHolder<NeonInnerTypes<C>>,
            NativeObjectHandle<NeonInnerTypes<C>>,
            String,
        ) -> Result<NativeObjectHandle<NeonInnerTypes<C>>, CubeError>,
    >(
        &self,
        target: Option<NativeObjectHandle<NeonInnerTypes<C>>>,
        get_fn: F,
    ) -> Result<NativeObjectHandle<NeonInnerTypes<C>>, CubeError> {
        todo!()
    }
}

impl<C: Context<'static>> Clone for ContextHolder<C> {
    fn clone(&self) -> Self {
        Self {
            context: self.context.clone(),
        }
    }
}
/*
 * use neon::prelude::*;

/// get(target, property, receiver)
fn trap_get(mut cx: FunctionContext) -> JsResult<JsValue> {
    let target = cx.argument::<JsObject>(0)?;
    let prop   = cx.argument::<JsValue>(1)?;
    let recv   = cx.argument::<JsValue>(2)?;

    // пример: если prop — строка, дергаем ваш Rust-обработчик
    if let Ok(js_str) = prop.downcast::<JsString, _>(&mut cx) {
        let name = js_str.value(&mut cx);

        // ... ваша логика ...
        // вернуть что-то «своё»
        // return Ok(cx.string(format!("prop: {}", name)).upcast());

        // или пробросить к Reflect.get для дефолтного поведения:
        let reflect = cx.global()
            .get::<JsObject, _, _>(&mut cx, "Reflect")?;
        let reflect_get = reflect
            .get::<JsFunction, _, _>(&mut cx, "get")?;
        let v = reflect_get
            .call(&mut cx, reflect.upcast(), vec![target.upcast(), prop.upcast(), recv])?;
        return Ok(v);
    }

    // по умолчанию — Reflect.get
    let reflect = cx.global().get::<JsObject, _, _>(&mut cx, "Reflect")?;
    let reflect_get = reflect.get::<JsFunction, _, _>(&mut cx, "get")?;
    reflect_get.call(&mut cx, reflect.upcast(), vec![target.upcast(), prop, recv])
}

fn make_proxy(mut cx: FunctionContext) -> JsResult<JsObject> {
    // Proxy конструктор
    let proxy_ctor = cx
        .global()
        .get::<JsFunction, _, _>(&mut cx, "Proxy")?;

    // target можно принять аргументом или создать здесь
    let target = JsObject::new(&mut cx);

    // handler = { get: trap_get, ... }
    let handler = JsObject::new(&mut cx);
    let get_fn = JsFunction::new(&mut cx, trap_get)?;
    handler.set(&mut cx, "get", get_fn)?;

    // new Proxy(target, handler)
    let proxy_val = proxy_ctor.construct(
        &mut cx,
        vec![target.upcast::<JsValue>(), handler.upcast::<JsValue>()],
    )?;

    Ok(proxy_val.downcast_or_throw::<JsObject, _>(&mut cx)?)
}

// export из модуля
register_module!(mut cx, {
    cx.export_function("makeProxy", make_proxy)
});
 * */
