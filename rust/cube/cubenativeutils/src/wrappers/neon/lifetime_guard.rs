use super::object::NeonObject;
use super::*;
use crate::CubeError;
use crate::{
    wrappers::{functions_args_def::FunctionArgsDef, object_handle::NativeObjectHandle},
    CubeErrorCauseType,
};
use neon::prelude::*;
use std::{
    cell::RefCell,
    marker::PhantomData,
    panic::{catch_unwind, resume_unwind, AssertUnwindSafe},
    rc::Rc,
};

trait NoenContextLifetimeExpand<'cx> {
    type ExpandedResult: Context<'static>;
    fn expand_lifetime(self) -> Self::ExpandedResult;
}

impl<'cx> NoenContextLifetimeExpand<'cx> for FunctionContext<'cx> {
    type ExpandedResult = FunctionContext<'static>;
    fn expand_lifetime(self) -> Self::ExpandedResult {
        unsafe { std::mem::transmute::<FunctionContext<'cx>, FunctionContext<'static>>(self) }
    }
}

struct NeonContextGuard<'cx, C: Context<'cx> + NoenContextLifetimeExpand<'cx>> {
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

pub(super) fn neon_run_with_guarded_lifetime<F>(cx: FunctionContext, func: F) -> JsResult<JsValue>
where
    F: FnOnce(
        ContextHolder<FunctionContext<'static>>,
    ) -> Result<NativeObjectHandle<NeonFuncInnerTypes>, CubeError>,
{
    let guard = NeonContextGuard::new(cx);
    let context_holder = guard.context_holder();
    let res = catch_unwind(AssertUnwindSafe(|| {
        let res = func(context_holder.clone());
        res.map_or_else(
            |e| match e.cause {
                CubeErrorCauseType::User => {
                    context_holder
                        .with_context(|cx| {
                            let err = JsError::error(cx, e.message)?;
                            let name = cx.string("TesseractUserError");
                            err.set(cx, "name", name)?;
                            cx.throw(err)
                        })
                        .unwrap() // Context is dead → cannot safely work with Js, panic.
                }
                CubeErrorCauseType::Internal => {
                    context_holder
                        .with_context(|cx| cx.throw_error(e.message))
                        .unwrap() // Context is dead → cannot safely work with Js, panic.
                }
                CubeErrorCauseType::NeonThrow(throw) => Err(throw),
            },
            |res| {
                Ok(res.into_object().get_js_value().unwrap()) // Context is dead → cannot safely work with Js, panic
            },
        )
    }));

    guard.unwrap();
    match res {
        Ok(res) => res,
        Err(e) => resume_unwind(e),
    }
}

pub fn neon_guarded_funcion_call<In, Rt, F: FunctionArgsDef<NeonFuncInnerTypes, In, Rt>>(
    cx: FunctionContext,
    func: F,
) -> JsResult<JsValue> {
    neon_run_with_guarded_lifetime(cx, move |neon_context_holder| {
        let args = neon_context_holder
            .with_context(|cx| -> Result<_, CubeError> {
                let mut args = vec![];
                for i in 0..F::args_len() {
                    args.push(cx.argument::<JsValue>(i)?);
                }
                Ok(args)
            })??
            .into_iter()
            .map(|arg| -> Result<_, CubeError> {
                Ok(NativeObjectHandle::new(NeonObject::new(
                    neon_context_holder.clone(),
                    arg,
                )?))
            })
            .collect::<Result<Vec<_>, _>>()?;
        let context_holder = neon_context_holder.clone().into();

        func.call_func(context_holder, args)
    })
}
