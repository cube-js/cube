use crate::wrappers::neon::context::ContextHolder;
use crate::CubeError;
use neon::prelude::*;
use std::rc::Rc;

pub struct ObjectNeonTypeHolder<C: Context<'static>, V: Object + 'static> {
    context: ContextHolder<C>,
    value: Option<Rc<Root<V>>>,
}
impl<C: Context<'static> + 'static, V: Object + 'static> ObjectNeonTypeHolder<C, V> {
    pub fn new(context: ContextHolder<C>, object: Handle<'static, V>, cx: &mut C) -> Self {
        let value = object.root(cx);
        Self {
            context,
            value: Some(Rc::new(value)),
        }
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
        F: FnOnce(&mut C, &Handle<'static, V>) -> NeonResult<T>,
    {
        Ok(self.context.with_context(|cx| {
            let object = self.value_ref().to_inner(cx);
            f(cx, &object)
        })??)
    }

    pub fn clone_to_context<CC: Context<'static> + 'static>(
        &self,
        context: &ContextHolder<CC>,
    ) -> ObjectNeonTypeHolder<CC, V> {
        ObjectNeonTypeHolder {
            context: context.clone(),
            value: self.value.clone(),
        }
    }
}

impl<C: Context<'static>, V: Object + 'static> Drop for ObjectNeonTypeHolder<C, V> {
    fn drop(&mut self) {
        if let Some(value) = self.value.take() {
            if let Ok(value) = Rc::try_unwrap(value) {
                // Attempt to drop Root with context for immediate cleanup.
                // If context is no longer valid (e.g., callback outlived its scope),
                // Root will be safely dropped via N-API's deferred cleanup mechanism.
                let _ = self.context.with_context(|cx| {
                    value.drop(cx);
                });
            }
        }
    }
}

impl<C: Context<'static>, V: Object + 'static> Clone for ObjectNeonTypeHolder<C, V> {
    fn clone(&self) -> Self {
        Self {
            context: self.context.clone(),
            value: self.value.clone(),
        }
    }
}
