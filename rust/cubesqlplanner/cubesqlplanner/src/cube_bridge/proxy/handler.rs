use crate::cube_bridge::base_tools::BaseTools;

use super::Proxy;
use crate::cube_bridge::evaluator::CubeEvaluator;
use cubenativeutils::wrappers::context::NativeFinalize;
use cubenativeutils::wrappers::inner_types::InnerTypes;
use cubenativeutils::wrappers::serializer::{
    NativeDeserialize, NativeDeserializer, NativeSerialize,
};
use cubenativeutils::wrappers::{
    object::NativeBox, NativeContextHolder, NativeFunction, NativeObjectHandle, NativeString,
    NativeStruct, NativeType,
};
use cubenativeutils::CubeError;
use serde::Deserialize;
use std::any::Any;
use std::cell::{RefCell, RefMut};
use std::marker::PhantomData;
use std::rc::Rc;

pub trait ProxyHandler {
    fn as_any(self: Rc<Self>) -> Rc<dyn Any>;
}

pub struct NativeProxyHandler<IT: InnerTypes> {
    collector_box: NativeObjectHandle<IT>,
}

impl<IT: InnerTypes> ProxyHandler for NativeProxyHandler<IT> {
    fn as_any(self: Rc<Self>) -> Rc<dyn Any> {
        self
    }
}

impl<IT: InnerTypes> NativeProxyHandler<IT> {
    pub fn new<T: NativeFinalize + 'static>(collector_box: impl NativeBox<IT, T>) -> Rc<Self> {
        Rc::new(Self {
            collector_box: NativeObjectHandle::new_from_type(collector_box),
        })
    }
}

impl<IT: InnerTypes> NativeSerialize<IT> for NativeProxyHandler<IT> {
    fn to_native(
        &self,
        _context: Rc<NativeContextHolder<IT>>,
    ) -> Result<NativeObjectHandle<IT>, CubeError> {
        Ok(self.collector_box.clone())
    }
}

pub trait ProxyHandlerFunction {
    fn as_any(self: Rc<Self>) -> Rc<dyn Any>;
}

pub struct NativeProxyHandlerFunction<IT: InnerTypes> {
    function: NativeObjectHandle<IT>,
}

impl<IT: InnerTypes> ProxyHandlerFunction for NativeProxyHandlerFunction<IT> {
    fn as_any(self: Rc<Self>) -> Rc<dyn Any> {
        self
    }
}

impl<IT: InnerTypes> NativeProxyHandlerFunction<IT> {
    pub fn new(function: IT::Function) -> Rc<Self> {
        Rc::new(Self {
            function: NativeObjectHandle::new_from_type(function),
        })
    }
}

impl<IT: InnerTypes> NativeSerialize<IT> for NativeProxyHandlerFunction<IT> {
    fn to_native(
        &self,
        _context: Rc<NativeContextHolder<IT>>,
    ) -> Result<NativeObjectHandle<IT>, CubeError> {
        Ok(self.function.clone())
    }
}
