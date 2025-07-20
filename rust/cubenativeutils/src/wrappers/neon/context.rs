use super::{
    inner_types::NeonInnerTypes,
    object::{
        base_types::*, neon_array::NeonArray, neon_function::NeonFunction, neon_struct::NeonStruct,
        NeonObject,
    },
};
use crate::wrappers::{
    context::NativeContext, object::NativeObject, object_handle::NativeObjectHandle,
};
use cubesql::CubeError;
use neon::prelude::*;
use std::{
    cell::RefCell,
    marker::PhantomData,
    panic::{catch_unwind, resume_unwind, AssertUnwindSafe},
    rc::{Rc, Weak},
};

pub trait NoenContextLifetimeExpand<'cx> {
    type ExpandedResult: Context<'static>;
    fn expand_lifetime(self) -> Self::ExpandedResult;
}

impl<'cx> NoenContextLifetimeExpand<'cx> for FunctionContext<'cx> {
    type ExpandedResult = FunctionContext<'static>;
    fn expand_lifetime(self) -> Self::ExpandedResult {
        unsafe { std::mem::transmute::<FunctionContext<'cx>, FunctionContext<'static>>(self) }
    }
}

pub struct SafeCallFn<'a> {
    safe_fn: &'a Option<Handle<'static, JsFunction>>,
}

impl<'a> SafeCallFn<'a> {
    pub fn new(safe_fn: &'a Option<Handle<'static, JsFunction>>) -> Self {
        Self { safe_fn }
    }

    pub fn safe_call<C: Context<'static>, T: Value>(
        &self,
        cx: &mut C,
        func: &Handle<'static, JsFunction>,
        this: Handle<'static, T>,
        mut args: Vec<Handle<'static, JsValue>>,
    ) -> Result<Handle<'static, JsValue>, CubeError> {
        if let Some(safe_fn) = self.safe_fn {
            args.insert(0, this.upcast());

            args.insert(0, func.upcast());

            let res = safe_fn
                .call(cx, this, args)
                .map_err(|_| CubeError::internal(format!("Failed to call safe function")))?;
            let res = res.downcast::<JsObject, _>(cx).map_err(|_| {
                CubeError::internal(format!("Result of safe function call should be object"))
            })?;
            let result_field = res.get_value(cx, "result").map_err(|_| {
                CubeError::internal(format!(
                    "Failed wile get `result` field of safe call function result"
                ))
            })?;
            let err_field = res.get_value(cx, "error").map_err(|_| {
                CubeError::internal(format!(
                    "Failed wile get `error` field of safe call function result"
                ))
            })?;
            if !err_field.is_a::<JsUndefined, _>(cx) {
                let error_string = err_field.downcast::<JsString, _>(cx).map_err(|_| {
                    CubeError::internal(format!(
                        "Error in safe call function result should be string"
                    ))
                })?;
                Err(CubeError::internal(error_string.value(cx)))
            } else if !result_field.is_a::<JsUndefined, _>(cx) {
                Ok(result_field)
            } else {
                Err(CubeError::internal(format!(
                    "Safe call function should return object with result or error field"
                )))
            }
        } else {
            let res = func
                .call(cx, this, args)
                .map_err(|_| CubeError::internal(format!("Failed to call function")))?;
            Ok(res)
        }
    }
}

pub struct ContextWrapper<C: Context<'static>> {
    cx: C,
    safe_call_fn: Option<Handle<'static, JsFunction>>,
}

impl<C: Context<'static>> ContextWrapper<C> {
    pub fn new(cx: C) -> Rc<RefCell<Self>> {
        Rc::new(RefCell::new(Self {
            cx,
            safe_call_fn: None,
        }))
    }

    pub fn set_safe_call_fn(&mut self, fn_handle: Option<Handle<'static, JsFunction>>) {
        self.safe_call_fn = fn_handle;
    }

    pub fn with_context<T, F>(&mut self, f: F) -> T
    where
        F: FnOnce(&mut C) -> T,
    {
        f(&mut self.cx)
    }

    pub fn with_context_and_safe_fn<T, F>(&mut self, f: F) -> T
    where
        F: FnOnce(&mut C, SafeCallFn) -> T,
    {
        let safe_call_fn = SafeCallFn::new(&self.safe_call_fn);
        f(&mut self.cx, safe_call_fn)
    }

    pub fn get_context(&mut self) -> &mut C {
        &mut self.cx
    }
}

pub struct NeonContextGuard<'cx, C: Context<'cx> + NoenContextLifetimeExpand<'cx>> {
    context: Rc<RefCell<ContextWrapper<C::ExpandedResult>>>,
    lifetime: PhantomData<&'cx ()>,
}

impl<'cx, C: Context<'cx> + NoenContextLifetimeExpand<'cx> + 'cx> NeonContextGuard<'cx, C> {
    fn new(cx: C) -> Self {
        Self {
            context: ContextWrapper::new(cx.expand_lifetime()),
            lifetime: PhantomData,
        }
    }

    fn context_holder(&self) -> ContextHolder<C::ExpandedResult> {
        ContextHolder::new(Rc::downgrade(&self.context))
    }

    fn unwrap(self) {
        if Rc::strong_count(&self.context) > 0 {
            match Rc::try_unwrap(self.context) {
                Ok(_) => {}
                Err(_) => panic!("Guarded context have more then one reference"),
            }
        }
    }
}

pub fn neon_run_with_guarded_lifetime<'cx, C, T, F>(cx: C, func: F) -> T
where
    C: Context<'cx> + NoenContextLifetimeExpand<'cx> + 'cx,
    F: FnOnce(ContextHolder<C::ExpandedResult>) -> T,
{
    let guard = NeonContextGuard::new(cx);
    let context_holder = guard.context_holder();
    let res = catch_unwind(AssertUnwindSafe(|| func(context_holder)));
    guard.unwrap();

    match res {
        Ok(res) => res,
        Err(e) => resume_unwind(e),
    }
}

pub struct ContextHolder<C: Context<'static>> {
    context: Weak<RefCell<ContextWrapper<C>>>,
}

impl<C: Context<'static>> ContextHolder<C> {
    fn new(context: Weak<RefCell<ContextWrapper<C>>>) -> Self {
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

    pub fn with_context_and_safe_fn<T, F>(&self, f: F) -> Result<T, CubeError>
    where
        F: FnOnce(&mut C, SafeCallFn) -> T,
    {
        if let Some(context) = self.context.upgrade() {
            let mut cx = context.borrow_mut();
            let res = cx.with_context_and_safe_fn(f);
            Ok(res)
        } else {
            Err(CubeError::internal(format!(
                "Call to neon context outside of its lifetime"
            )))
        }
    }

    pub fn set_safe_call_fn(
        &self,
        f: Option<Handle<'static, JsFunction>>,
    ) -> Result<(), CubeError> {
        if let Some(context) = self.context.upgrade() {
            let mut cx = context.borrow_mut();
            cx.set_safe_call_fn(f);
            Ok(())
        } else {
            Err(CubeError::internal(format!(
                "Call to neon context outside of its lifetime"
            )))
        }
    }
}

impl<C: Context<'static> + 'static> NativeContext<NeonInnerTypes<C>> for ContextHolder<C> {
    fn boolean(&self, v: bool) -> Result<NeonBoolean<C>, CubeError> {
        let obj = NeonObject::new(
            self.clone(),
            self.with_context(|cx| cx.boolean(v).upcast())?,
        );
        obj.into_boolean()
    }

    fn string(&self, v: String) -> Result<NeonString<C>, CubeError> {
        let obj = NeonObject::new(self.clone(), self.with_context(|cx| cx.string(v).upcast())?);
        obj.into_string()
    }

    fn number(&self, v: f64) -> Result<NeonNumber<C>, CubeError> {
        let obj = NeonObject::new(self.clone(), self.with_context(|cx| cx.number(v).upcast())?);
        obj.into_number()
    }

    fn undefined(&self) -> Result<NativeObjectHandle<NeonInnerTypes<C>>, CubeError> {
        Ok(NativeObjectHandle::new(NeonObject::new(
            self.clone(),
            self.with_context(|cx| cx.undefined().upcast())?,
        )))
    }

    fn null(&self) -> Result<NativeObjectHandle<NeonInnerTypes<C>>, CubeError> {
        Ok(NativeObjectHandle::new(NeonObject::new(
            self.clone(),
            self.with_context(|cx| cx.null().upcast())?,
        )))
    }

    fn empty_array(&self) -> Result<NeonArray<C>, CubeError> {
        let obj = NeonObject::new(
            self.clone(),
            self.with_context(|cx| cx.empty_array().upcast())?,
        );
        obj.into_array()
    }

    fn empty_struct(&self) -> Result<NeonStruct<C>, CubeError> {
        let obj = NeonObject::new(
            self.clone(),
            self.with_context(|cx| cx.empty_object().upcast())?,
        );
        obj.into_struct()
    }
    fn to_string_fn(&self, result: String) -> Result<NeonFunction<C>, CubeError> {
        let obj = NeonObject::new(
            self.clone(),
            self.with_context(|cx| {
                JsFunction::new(cx, move |mut c| Ok(c.string(result.clone())))
                    .unwrap()
                    .upcast()
            })?,
        );
        obj.into_function()
    }
}

impl<C: Context<'static>> Clone for ContextHolder<C> {
    fn clone(&self) -> Self {
        Self {
            context: self.context.clone(),
        }
    }
}
