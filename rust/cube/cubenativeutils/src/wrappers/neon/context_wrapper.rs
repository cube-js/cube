use neon::prelude::*;
use std::{cell::RefCell, rc::Rc};

pub(super) struct ContextWrapper<C: Context<'static>> {
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
}
