use super::*;

use super::{
    inner_types::NeonInnerTypes,
    object::{
        base_types::*, neon_array::NeonArray, neon_function::NeonFunction, neon_struct::NeonStruct,
        NeonObject,
    },
    ContextWrapper,
};
use crate::wrappers::neon::object::IntoNeonObject;
use crate::wrappers::object::*;
use crate::wrappers::serializer::NativeSerialize;
use crate::wrappers::{
    context::NativeContext, functions_args_def::FunctionArgsDef, object::NativeObject,
    object_handle::NativeObjectHandle, NativeContextHolder,
};
use crate::CubeError;
use neon::prelude::*;
use std::{
    cell::RefCell,
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
            let mut cx = context.try_borrow_mut().map_err(|_| {
                CubeError::internal("Nested ContextHolder::with_context call: Neon context already mutably borrowed (re-entrant call)".to_string())
            })?;
            let res = cx.with_context(f);
            Ok(res)
        } else {
            Err(CubeError::internal(
                "Call to neon context outside of its lifetime".to_string(),
            ))
        }
    }

    fn proxy_get_trap<
        Ret: NativeSerialize<NeonInnerTypes<FunctionContext<'static>>>,
        F: Fn(
                NativeContextHolder<NeonInnerTypes<FunctionContext<'static>>>,
                NativeObjectHandle<NeonInnerTypes<FunctionContext<'static>>>,
                String,
            ) -> Result<Option<Ret>, CubeError>
            + 'static,
    >(
        &self,
        get_fn: F,
    ) -> Result<Handle<'static, JsFunction>, CubeError> {
        self.with_context(|cx| {
            let func = JsFunction::new(cx, move |cx| {
                neon_run_with_guarded_lifetime(cx, |func_context| {
                    let (target, prop, string_prop) =
                        func_context.with_context(|func_cx| -> Result<_, CubeError> {
                            let target = func_cx.argument::<JsObject>(0)?;
                            let prop = func_cx.argument::<JsValue>(1)?;
                            let string_prop =
                                if let Ok(string_prop) = prop.downcast::<JsString, _>(func_cx) {
                                    Some(string_prop.value(func_cx))
                                } else {
                                    None
                                };
                            Ok((target, prop, string_prop))
                        })??;

                    if let Some(string_prop) = string_prop {
                        let neon_target = NeonObject::new(func_context.clone(), target)?;
                        if let Some(res) =
                            get_fn(func_context.clone().into(), neon_target.into(), string_prop)?
                        {
                            return res.to_native(func_context.clone().into());
                        }
                    }
                    let res = func_context.with_context(|func_cx| -> Result<_, CubeError> {
                        let reflect = func_cx.global::<JsObject>("Reflect")?;
                        let null = func_cx.null();
                        let reflect_get = reflect.get::<JsFunction, _, _>(func_cx, "get")?;
                        let reflect_res = reflect_get.call(
                            func_cx,
                            null,
                            vec![target.upcast::<JsValue>(), prop.upcast::<JsValue>()],
                        )?;
                        Ok(reflect_res)
                    })??;

                    Ok(NativeObjectHandle::new(NeonObject::new(func_context, res)?))
                })
            })?;
            Ok(func)
        })?
    }
}

impl<C: Context<'static> + 'static> NativeContext<NeonInnerTypes<C>> for ContextHolder<C> {
    fn boolean(&self, v: bool) -> Result<NeonBoolean<C>, CubeError> {
        let obj = NeonObject::new(self.clone(), self.with_context(|cx| cx.boolean(v))?)?;
        obj.into_boolean()
    }

    fn string(&self, v: String) -> Result<NeonString<C>, CubeError> {
        let obj = NeonObject::new(self.clone(), self.with_context(|cx| cx.string(v))?)?;
        obj.into_string()
    }

    fn number(&self, v: f64) -> Result<NeonNumber<C>, CubeError> {
        let obj = NeonObject::new(self.clone(), self.with_context(|cx| cx.number(v))?)?;
        obj.into_number()
    }

    fn undefined(&self) -> Result<NativeObjectHandle<NeonInnerTypes<C>>, CubeError> {
        Ok(NativeObjectHandle::new(NeonObject::new(
            self.clone(),
            self.with_context(|cx| cx.undefined())?,
        )?))
    }

    fn null(&self) -> Result<NativeObjectHandle<NeonInnerTypes<C>>, CubeError> {
        Ok(NativeObjectHandle::new(NeonObject::new(
            self.clone(),
            self.with_context(|cx| cx.null())?,
        )?))
    }

    fn empty_array(&self) -> Result<NeonArray<C>, CubeError> {
        let obj = NeonObject::new(self.clone(), self.with_context(|cx| cx.empty_array())?)?;
        obj.into_array()
    }

    fn empty_struct(&self) -> Result<NeonStruct<C>, CubeError> {
        let obj = NeonObject::new(self.clone(), self.with_context(|cx| cx.empty_object())?)?;
        obj.into_struct()
    }
    fn to_string_fn(&self, result: String) -> Result<NeonFunction<C>, CubeError> {
        let obj = self
            .with_context(|cx| -> Result<_, CubeError> {
                let func = JsFunction::new(cx, move |mut c| Ok(c.string(result.clone())))?;
                Ok(func)
            })??
            .into_neon_object(self.clone())?;
        obj.into_function()
    }

    fn global(&self, name: &str) -> Result<NativeObjectHandle<NeonInnerTypes<C>>, CubeError> {
        let obj = self.with_context(|cx| cx.global::<JsValue>(name))??;
        Ok(obj.into_neon_object(self.clone())?.into())
    }

    fn make_function<In, Rt, F: FunctionArgsDef<NeonFuncInnerTypes, In, Rt> + 'static>(
        &self,
        f: F,
    ) -> Result<NeonFunction<C>, CubeError> {
        let f = Rc::new(f);
        let obj = self
            .with_context(|cx| -> Result<_, CubeError> {
                let func = JsFunction::new(cx, move |cx| neon_guarded_funcion_call(cx, f.clone()))?;
                Ok(func)
            })??
            .into_neon_object(self.clone())?;
        obj.into_function()
    }
    fn make_vararg_function<
        Rt: NativeSerialize<NeonInnerTypes<FunctionContext<'static>>>,
        F: Fn(
                NativeContextHolder<NeonInnerTypes<FunctionContext<'static>>>,
                Vec<NativeObjectHandle<NeonInnerTypes<FunctionContext<'static>>>>,
            ) -> Result<Rt, CubeError>
            + 'static,
    >(
        &self,
        f: F,
    ) -> Result<NeonFunction<C>, CubeError> {
        let f = Rc::new(f);
        let obj = self
            .with_context(|cx| {
                let func = JsFunction::new(cx, move |cx| {
                    neon_run_with_guarded_lifetime(cx, |function_context| {
                        let args = function_context
                            .with_context(|cx| -> Result<_, CubeError> {
                                let mut args = vec![];
                                for i in 0..cx.len() {
                                    args.push(cx.argument::<JsValue>(i)?);
                                }
                                Ok(args)
                            })??
                            .into_iter()
                            .map(|arg| -> Result<_, CubeError> {
                                Ok(NativeObjectHandle::new(NeonObject::new(
                                    function_context.clone(),
                                    arg,
                                )?))
                            })
                            .collect::<Result<Vec<_>, _>>()?;

                        let context_holder: NativeContextHolder<
                            NeonInnerTypes<FunctionContext<'static>>,
                        > = function_context.clone().into();
                        let res = f(context_holder.clone(), args)?;
                        res.to_native(context_holder.clone())
                    })
                });
                func
            })??
            .into_neon_object(self.clone())?;
        obj.into_function()
    }
    fn make_proxy<
        Ret: NativeSerialize<NeonInnerTypes<FunctionContext<'static>>>,
        F: Fn(
                NativeContextHolder<NeonInnerTypes<FunctionContext<'static>>>,
                NativeObjectHandle<NeonInnerTypes<FunctionContext<'static>>>,
                String,
            ) -> Result<Option<Ret>, CubeError>
            + 'static,
    >(
        &self,
        target: Option<NativeObjectHandle<NeonInnerTypes<C>>>,
        get_fn: F,
    ) -> Result<NativeObjectHandle<NeonInnerTypes<C>>, CubeError> {
        let get_trap = self.proxy_get_trap(get_fn)?;
        let target = if let Some(target) = target {
            target
        } else {
            NativeObjectHandle::new(self.empty_struct()?.into_object())
        };
        let neon_target = target.object_ref().get_js_value()?;
        let res = self.with_context(|cx| -> Result<_, CubeError> {
            let proxy = cx.global::<JsFunction>("Proxy")?;
            let handler = JsObject::new(cx);
            handler.set(cx, "get", get_trap)?;
            Ok(proxy.construct(cx, vec![neon_target, handler.upcast::<JsValue>()])?)
        })??;

        let res = NativeObjectHandle::new(NeonObject::new(self.clone(), res)?);

        Ok(res)
    }
}

impl<C: Context<'static> + 'static> From<ContextHolder<C>>
    for NativeContextHolder<NeonInnerTypes<C>>
{
    fn from(context: ContextHolder<C>) -> Self {
        Self::new(context)
    }
}

impl<C: Context<'static>> Clone for ContextHolder<C> {
    fn clone(&self) -> Self {
        Self {
            context: self.context.clone(),
        }
    }
}
