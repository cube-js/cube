use super::object::NeonObject;
use crate::wrappers::context::{NativeContext, NativeContextHolder};
use crate::wrappers::object::NativeObject;
use cubesql::CubeError;
use neon::prelude::*;
use std::cell::{RefCell, RefMut};
use std::rc::{Rc, Weak};
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

pub struct ContextHolder<C: Context<'static>> {
    context: Rc<RefCell<ContextWrapper<C>>>,
}

impl<C: Context<'static> + 'static> ContextHolder<C> {
    pub fn new(cx: C) -> Self {
        Self {
            context: ContextWrapper::new(cx),
        }
    }

    pub fn borrow_mut(&self) -> RefMut<ContextWrapper<C>> {
        self.context.borrow_mut()
    }

    pub fn with_context<T, F>(&self, f: F) -> T
    where
        F: FnOnce(&mut C) -> T,
    {
        let mut context = self.context.borrow_mut();
        context.with_context(f)
    }

    pub fn as_native_context_holder(&self) -> NativeContextHolder {
        NativeContextHolder::new(Box::new(self.clone()))
    }

    pub fn weak(&self) -> WeakContextHolder<C> {
        WeakContextHolder {
            context: Rc::downgrade(&self.context),
        }
    }
}

impl<C: Context<'static> + 'static> NativeContext for ContextHolder<C> {
    fn boolean(&self, v: bool) -> Box<dyn crate::wrappers::object::NativeBoolean> {
        let obj = NeonObject::new(self.weak(), self.with_context(|cx| cx.boolean(v).upcast()));
        obj.into_boolean().unwrap()
    }

    fn string(&self, v: String) -> Box<dyn crate::wrappers::object::NativeString> {
        let obj = NeonObject::new(self.weak(), self.with_context(|cx| cx.string(v).upcast()));
        obj.into_string().unwrap()
    }

    fn number(&self, v: f64) -> Box<dyn crate::wrappers::object::NativeNumber> {
        let obj = NeonObject::new(self.weak(), self.with_context(|cx| cx.number(v).upcast()));
        obj.into_number().unwrap()
    }

    fn undefined(&self) -> Box<dyn crate::wrappers::object::NativeObject> {
        NeonObject::new(self.weak(), self.with_context(|cx| cx.undefined().upcast()))
    }

    fn empty_array(&self) -> Box<dyn crate::wrappers::object::NativeArray> {
        let obj = NeonObject::new(
            self.weak(),
            self.with_context(|cx| cx.empty_array().upcast()),
        );
        obj.into_array().unwrap()
    }

    fn empty_struct(&self) -> Box<dyn crate::wrappers::object::NativeStruct> {
        let obj = NeonObject::new(
            self.weak(),
            self.with_context(|cx| cx.empty_object().upcast()),
        );
        obj.into_struct().unwrap()
    }
}

impl<C: Context<'static>> Clone for ContextHolder<C> {
    fn clone(&self) -> Self {
        Self {
            context: self.context.clone(),
        }
    }
}

pub struct WeakContextHolder<C: Context<'static>> {
    context: Weak<RefCell<ContextWrapper<C>>>,
}

impl<C: Context<'static>> WeakContextHolder<C> {
    pub fn try_upgrade(&self) -> Result<ContextHolder<C>, CubeError> {
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

impl<C: Context<'static>> Clone for WeakContextHolder<C> {
    fn clone(&self) -> Self {
        Self {
            context: self.context.clone(),
        }
    }
}
