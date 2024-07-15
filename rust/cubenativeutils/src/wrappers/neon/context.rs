use neon::prelude::*;
use std::cell::{RefCell, RefMut};
use std::marker::PhantomData;
use std::rc::Rc;
pub struct ContextWrapper<C: Context<'static>> {
    cx: C,
}

impl<C: Context<'static>> ContextWrapper<C> {
    pub fn new(cx: C) -> Rc<RefCell<Self>> {
        Rc::new(RefCell::new(Self { cx }))
    }

    pub fn call<T, F>(&mut self, f: F) -> T
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

impl<C: Context<'static>> ContextHolder<C> {
    pub fn new(cx: C) -> Self {
        Self {
            context: ContextWrapper::new(cx),
        }
    }

    pub fn borrow_mut(&self) -> RefMut<ContextWrapper<C>> {
        self.context.borrow_mut()
    }
}
impl<C: Context<'static>> Clone for ContextHolder<C> {
    fn clone(&self) -> Self {
        Self {
            context: self.context.clone(),
        }
    }
}
