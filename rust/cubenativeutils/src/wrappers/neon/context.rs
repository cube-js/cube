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
use crate::wrappers::serializer::{NativeDeserialize, NativeSerialize};
use crate::wrappers::{
    context::NativeContext, functions_args_def::FunctionArgsDef, object::NativeObject,
    object_handle::NativeObjectHandle, NativeContextHolder,
};
use crate::wrappers::{NativeString, NativeStruct};
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
