use super::{NeonObject, ObjectNeonTypeHolder};
use crate::wrappers::{
    neon::{context::ContextHolder, inner_types::NeonInnerTypes},
    object_handle::NativeObjectHandle,
};
use crate::CubeError;
use neon::prelude::*;
use std::rc::Rc;

trait NeonProxyCall<C: Context<'static> + 'static> {
    fn call_get(
        &self,
        context: C,
        object: NeonObject<C>,
        property: &str,
    ) -> Result<NativeObjectHandle<NeonInnerTypes<C>>, CubeError>;
}

struct NeonProxyFunc<
    C: Context<'static> + 'static,
    F: Fn(C, NeonObject<C>, &str) -> Result<NativeObjectHandle<NeonInnerTypes<C>>, CubeError>,
> {
    get_fn: F,
    _marker: std::marker::PhantomData<C>,
}

impl<
        C: Context<'static> + 'static,
        F: Fn(C, NeonObject<C>, &str) -> Result<NativeObjectHandle<NeonInnerTypes<C>>, CubeError>,
    > NeonProxyCall<C> for NeonProxyFunc<C, F>
{
    fn call_get(
        &self,
        context: C,
        object: NeonObject<C>,
        property: &str,
    ) -> Result<NativeObjectHandle<NeonInnerTypes<C>>, CubeError> {
        (self.get_fn)(context, object, property)
    }
}

pub struct NeonProxy<C: Context<'static>> {
    object: ObjectNeonTypeHolder<C, JsObject>,
    //object: ObjectNeonTypeHolder<C, JsBox<Rc<dyn NeonProxyCall<C>>>>,
}

impl<C: Context<'static>> NeonProxy<C> {
    pub fn new(context: ContextHolder<C>, func: Rc<dyn NeonProxyCall<C>>) -> Self {
        todo!();
    }
}
