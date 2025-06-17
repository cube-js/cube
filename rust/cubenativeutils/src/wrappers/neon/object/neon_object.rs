use super::RootHolder;
use crate::wrappers::neon::context::{ContextHolder, SafeCallFn};
use crate::wrappers::object::NativeObject;
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

/* impl<C: Context<'static> + 'static> NativeObject<NeonInnerTypes<C>> for NeonObject<C> {
    fn get_context(&self) -> ContextHolder<C> {
        self.root_holder.ge.clone()
    }

    fn into_struct(self) -> Result<NeonStruct<C>, CubeError> {
        let obj = self.downcast_with_err_msg::<JsObject>("NeonObject is not the JsObject")?;
        Ok(NeonStruct::new(obj))
    }
    fn into_function(self) -> Result<NeonFunction<C>, CubeError> {
        let obj = self.downcast_with_err_msg::<JsFunction>("NeonObject is not the JsArray")?;
        Ok(NeonFunction::new(obj))
    }
    fn into_array(self) -> Result<NeonArray<C>, CubeError> {
        let obj = self.downcast_with_err_msg::<JsArray>("NeonObject is not the JsArray")?;
        Ok(NeonArray::new(obj))
    }
    fn into_string(self) -> Result<NeonString<C>, CubeError> {
        let obj = self.downcast_with_err_msg::<JsString>("NeonObject is not the JsString")?;
        Ok(NeonString::new(obj))
    }
    fn into_number(self) -> Result<NeonNumber<C>, CubeError> {
        let obj = self.downcast_with_err_msg::<JsNumber>("NeonObject is not the JsNumber")?;
        Ok(NeonNumber::new(obj))
    }
    fn into_boolean(self) -> Result<NeonBoolean<C>, CubeError> {
        let obj = self.downcast_with_err_msg::<JsBoolean>("NeonObject is not the JsBoolean")?;
        Ok(NeonBoolean::new(obj))
    }

    fn is_null(&self) -> Result<bool, CubeError> {
        self.is_a::<JsNull>()
    }

    fn is_undefined(&self) -> Result<bool, CubeError> {
        self.is_a::<JsUndefined>()
    }
} */
