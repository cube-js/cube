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

pub struct ContextWrapper<C: Context<'static>> {
    cx: C,
}

impl<C: Context<'static>> ContextWrapper<C> {
    pub fn new(cx: C) -> Rc<RefCell<Self>> {
        Rc::new(RefCell::new(Self { cx }))
    }

    pub fn with_context<T, F>(&mut self, f: F) -> T
    where
        F: FnOnce(&mut C) -> T,
    {
        f(&mut self.cx)
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
