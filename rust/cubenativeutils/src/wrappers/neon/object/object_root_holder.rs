use crate::wrappers::neon::context::{ContextHolder, SafeCallFn};
use cubesql::CubeError;
use neon::prelude::*;
use std::mem::MaybeUninit;
use std::rc::Rc;

#[derive(Clone)]
pub struct ObjectNeonTypeHolder<C: Context<'static>, V: Object + 'static> {
    context: ContextHolder<C>,
    value: Option<Rc<Root<V>>>,
}
impl<C: Context<'static> + 'static, V: Object + 'static> ObjectNeonTypeHolder<C, V> {
    pub fn new(context: ContextHolder<C>, object: Handle<'static, V>) -> Result<Self, CubeError> {
        let value = context.with_context(|cx| object.root(cx))?;
        Ok(Self {
            context,
            value: Some(Rc::new(value)),
        })
    }

    pub fn get_context(&self) -> ContextHolder<C> {
        self.context.clone()
    }

    fn value_ref(&self) -> &Root<V> {
        // Invariant: `self.value` must always be `Some` between construction and `Drop`.
        // If it's `None` here, it means the object is in an invalid state (e.g. accessed during destruction),
        // which is a bug. `unwrap()` is used to enforce this contract by panicking early.
        self.value.as_ref().unwrap()
    }

    pub fn map_neon_object<T, F>(&self, f: F) -> Result<T, CubeError>
    where
        F: FnOnce(&mut C, &Handle<'static, V>) -> T,
    {
        self.context.with_context(|cx| {
            let object = self.value_ref().to_inner(cx);
            f(cx, &object)
        })
    }

    pub fn map_neon_object_with_safe_call_fn<T, F>(&self, f: F) -> Result<T, CubeError>
    where
        F: FnOnce(&mut C, &Handle<'static, V>, SafeCallFn) -> T,
    {
        self.context.with_context_and_safe_fn(|cx, safe_call_fn| {
            let object = self.value_ref().to_inner(cx);
            f(cx, &object, safe_call_fn)
        })
    }
}

impl<C: Context<'static>, V: Object + 'static> Drop for ObjectNeonTypeHolder<C, V> {
    fn drop(&mut self) {
        if let Some(value) = self.value.take() {
            if let Ok(value) = Rc::try_unwrap(value) {
                let res = self.context.with_context(|cx| {
                    value.drop(cx);
                });
                if let Err(e) = res {
                    log::error!("Error while dropping Neon Root: {}", e)
                }
            }
        }
    }
}
