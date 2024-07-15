use crate::wrappers::neon::context::ContextHolder;
use crate::wrappers::object::NativeObject;
use cubesql::CubeError;
use neon::prelude::*;
use std::any::Any;
use std::cell::RefCell;
use std::mem::transmute;
use std::mem::ManuallyDrop;
use std::rc::Rc;
use std::sync::Arc;
use tokio::sync::oneshot;

#[derive(Clone)]
pub struct NeonObject<C: Context<'static>> {
    context: ContextHolder<C>,
    object: Handle<'static, JsValue>,
}

impl<C: Context<'static>> NeonObject<C> {
    pub fn new(context: ContextHolder<C>, object: Handle<'static, JsValue>) -> Rc<Self> {
        Rc::new(Self { context, object })
    }

    pub fn get_object(&self) -> Handle<'static, JsValue> {
        self.object.clone()
    }
}

impl<C: Context<'static> + 'static> NativeObject for NeonObject<C> {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn call(
        &self,
        method: &str,
        args: Vec<Rc<dyn NativeObject>>,
    ) -> Result<Rc<dyn NativeObject>, CubeError> {
        let neon_args = args
            .into_iter()
            .map(|arg| {
                if let Some(arg) = arg.as_any().downcast_ref::<Self>() {
                    Ok(arg.get_object())
                } else {
                    Err(CubeError::internal(format!(
                        "All arguments must be neon objects"
                    )))
                }
            })
            .collect::<Result<Vec<_>, _>>()?;

        let mut mut_obj = self.context.borrow_mut();
        let cx = mut_obj.get_context();
        let this = self
            .object
            .downcast::<JsObject, _>(cx)
            .map_err(|_| CubeError::internal(format!("Neon object is not JsObject")))?;
        let neon_method = this
            .get::<JsFunction, _, _>(cx, method)
            .map_err(|_| CubeError::internal(format!("Method `{}` not found", method)))?;
        let neon_result = neon_method
            .call(cx, this, neon_args)
            .map_err(|_| CubeError::internal(format!("Failed to call method `{}`", method)))?;
        let result = NeonObject::new(self.context.clone(), neon_result);
        Ok(result)
    }
}
