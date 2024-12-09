//use super::object::NeonObject;
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
    cell::{RefCell, RefMut},
    marker::PhantomData,
    rc::{Rc, Weak},
};
pub struct ContextWrapper<'cx, C: Context<'cx>> {
    cx: C,
    lifetime: PhantomData<&'cx ()>,
}

impl<'cx, C: Context<'cx>> ContextWrapper<'cx, C> {
    pub fn new(cx: C) -> Rc<RefCell<Self>> {
        Rc::new(RefCell::new(Self {
            cx,
            lifetime: Default::default(),
        }))
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

pub struct ContextHolder<'cx, C: Context<'cx>> {
    context: Rc<RefCell<ContextWrapper<'cx, C>>>,
}

impl<'cx, C: Context<'cx> + 'cx> ContextHolder<'cx, C> {
    pub fn new(cx: C) -> Self {
        Self {
            context: ContextWrapper::new(cx),
        }
    }

    pub fn borrow_mut(&self) -> RefMut<ContextWrapper<'cx, C>> {
        self.context.borrow_mut()
    }

    pub fn with_context<T, F>(&self, f: F) -> T
    where
        F: FnOnce(&mut C) -> T,
    {
        let mut context = self.context.borrow_mut();
        context.with_context(f)
    }

    /* pub fn as_native_context_holder(&self) -> NativeContextHolder {
        NativeContextHolder::new(Box::new(self.clone()))
    } */

    pub fn weak(&self) -> WeakContextHolder<'cx, C> {
        WeakContextHolder {
            context: Rc::downgrade(&self.context),
        }
    }
}

impl<'cx, C: Context<'cx> + 'cx> NativeContext<NeonInnerTypes<'cx, C>> for ContextHolder<'cx, C> {
    fn boolean(&self, v: bool) -> NeonBoolean<'cx, C> {
        let obj = NeonObject::new(self.clone(), self.with_context(|cx| cx.boolean(v).upcast()));
        obj.into_boolean().unwrap()
    }

    fn string(&self, v: String) -> NeonString<'cx, C> {
        let obj = NeonObject::new(self.clone(), self.with_context(|cx| cx.string(v).upcast()));
        obj.into_string().unwrap()
    }

    fn number(&self, v: f64) -> NeonNumber<'cx, C> {
        let obj = NeonObject::new(self.clone(), self.with_context(|cx| cx.number(v).upcast()));
        obj.into_number().unwrap()
    }

    fn undefined(&self) -> NativeObjectHandle<NeonInnerTypes<'cx, C>> {
        NativeObjectHandle::new(NeonObject::new(
            self.clone(),
            self.with_context(|cx| cx.undefined().upcast()),
        ))
    }

    fn empty_array(&self) -> NeonArray<'cx, C> {
        let obj = NeonObject::new(
            self.clone(),
            self.with_context(|cx| cx.empty_array().upcast()),
        );
        obj.into_array().unwrap()
    }

    fn empty_struct(&self) -> NeonStruct<'cx, C> {
        let obj = NeonObject::new(
            self.clone(),
            self.with_context(|cx| cx.empty_object().upcast()),
        );
        obj.into_struct().unwrap()
    }
    fn to_string_fn(&self, result: String) -> NeonFunction<'cx, C> {
        let obj = NeonObject::new(
            self.clone(),
            self.with_context(|cx| {
                JsFunction::new(cx, move |mut c| Ok(c.string(result.clone())))
                    .unwrap()
                    .upcast()
            }),
        );
        obj.into_function().unwrap()
    }
}

impl<'cx, C: Context<'cx>> Clone for ContextHolder<'cx, C> {
    fn clone(&self) -> Self {
        Self {
            context: self.context.clone(),
        }
    }
}

pub struct WeakContextHolder<'cx, C: Context<'cx>> {
    context: Weak<RefCell<ContextWrapper<'cx, C>>>,
}

impl<'cx, C: Context<'cx>> WeakContextHolder<'cx, C> {
    pub fn try_upgrade<'a>(&'a self) -> Result<ContextHolder<'a, C>, CubeError>
    where
        'a: 'cx,
    {
        if let Some(context) = self.context.upgrade() {
            Ok(ContextHolder { context })
        } else {
            Err(CubeError::internal(format!("Neon context is not alive")))
        }
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
            Err(CubeError::internal(format!("Neon context is not alive")))
        }
    }
}

impl<'cx, C: Context<'cx>> Clone for WeakContextHolder<'cx, C> {
    fn clone(&self) -> Self {
        Self {
            context: self.context.clone(),
        }
    }
}
