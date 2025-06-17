use super::RootHolder;
use crate::wrappers::neon::context::{ContextHolder, SafeCallFn};
use cubesql::CubeError;
use neon::prelude::*;
use std::rc::Rc;

pub struct NeonObject<C: Context<'static> + 'static> {
    root_holder: RootHolder<C>,
}

impl<C: Context<'static> + 'static> NeonObject<C> {
    pub fn new(
        context: ContextHolder<C>,
        object: Handle<'static, JsValue>,
    ) -> Result<Self, CubeError> {
        let root_holder = RootHolder::new(context.clone(), object)?;
        Ok(Self { root_holder })
    }

    pub fn form_root(root: RootHolder<C>) -> Self {
        Self { root_holder: root }
    }
}
