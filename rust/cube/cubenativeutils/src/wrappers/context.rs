use std::any::Any;

use super::FunctionArgsDef;
use super::{inner_types::InnerTypes, object_handle::NativeObjectHandle};
use crate::wrappers::serializer::NativeSerialize;
use crate::CubeError;

pub trait NativeContext<IT: InnerTypes>: Clone {
    fn boolean(&self, v: bool) -> Result<IT::Boolean, CubeError>;
    fn string(&self, v: String) -> Result<IT::String, CubeError>;
    fn number(&self, v: f64) -> Result<IT::Number, CubeError>;
    fn undefined(&self) -> Result<NativeObjectHandle<IT>, CubeError>;
    fn null(&self) -> Result<NativeObjectHandle<IT>, CubeError>;
    fn empty_array(&self) -> Result<IT::Array, CubeError>;
    fn empty_struct(&self) -> Result<IT::Struct, CubeError>;
    fn to_string_fn(&self, result: String) -> Result<IT::Function, CubeError>;
    fn global(&self, name: &str) -> Result<NativeObjectHandle<IT>, CubeError>;
    fn make_function<In, Rt, F: FunctionArgsDef<IT::FunctionIT, In, Rt> + 'static>(
        &self,
        f: F,
    ) -> Result<IT::Function, CubeError>;
    fn make_vararg_function<
        Rt: NativeSerialize<IT::FunctionIT>,
        F: Fn(
                NativeContextHolder<IT::FunctionIT>,
                Vec<NativeObjectHandle<IT::FunctionIT>>,
            ) -> Result<Rt, CubeError>
            + 'static,
    >(
        &self,
        f: F,
    ) -> Result<IT::Function, CubeError>;
    fn make_proxy<
        Ret: NativeSerialize<IT::FunctionIT>,
        F: Fn(
                NativeContextHolder<IT::FunctionIT>,
                NativeObjectHandle<IT::FunctionIT>,
                String,
            ) -> Result<Option<Ret>, CubeError>
            + 'static,
    >(
        &self,
        target: Option<NativeObjectHandle<IT>>,
        get_fn: F,
    ) -> Result<NativeObjectHandle<IT>, CubeError>;
}

pub trait NativeContextHolderRef: 'static {
    fn as_any(&self) -> &dyn Any;
}

#[derive(Clone)]
pub struct NativeContextHolder<IT: InnerTypes> {
    context: IT::Context,
}

impl<IT: InnerTypes> NativeContextHolder<IT> {
    pub fn new(context: IT::Context) -> Self {
        Self { context }
    }
    pub fn context(&self) -> &IT::Context {
        &self.context
    }
    pub fn boolean(&self, v: bool) -> Result<IT::Boolean, CubeError> {
        self.context.boolean(v)
    }
    pub fn string(&self, v: String) -> Result<IT::String, CubeError> {
        self.context.string(v)
    }
    pub fn number(&self, v: f64) -> Result<IT::Number, CubeError> {
        self.context.number(v)
    }
    pub fn undefined(&self) -> Result<NativeObjectHandle<IT>, CubeError> {
        self.context.undefined()
    }
    pub fn null(&self) -> Result<NativeObjectHandle<IT>, CubeError> {
        self.context.null()
    }
    pub fn empty_array(&self) -> Result<IT::Array, CubeError> {
        self.context.empty_array()
    }
    pub fn global(&self, name: &str) -> Result<NativeObjectHandle<IT>, CubeError> {
        self.context.global(name)
    }
    pub fn empty_struct(&self) -> Result<IT::Struct, CubeError> {
        self.context.empty_struct()
    }
    #[allow(dead_code)]
    pub fn to_string_fn(&self, result: String) -> Result<IT::Function, CubeError> {
        self.context.to_string_fn(result)
    }

    pub fn as_holder_ref(&self) -> &dyn NativeContextHolderRef {
        self
    }
    pub fn make_function<In, Rt, F: FunctionArgsDef<IT::FunctionIT, In, Rt> + 'static>(
        &self,
        f: F,
    ) -> Result<IT::Function, CubeError> {
        self.context.make_function(f)
    }
    pub fn make_vararg_function<
        Rt: NativeSerialize<IT::FunctionIT>,
        F: Fn(
                NativeContextHolder<IT::FunctionIT>,
                Vec<NativeObjectHandle<IT::FunctionIT>>,
            ) -> Result<Rt, CubeError>
            + 'static,
    >(
        &self,
        f: F,
    ) -> Result<IT::Function, CubeError> {
        self.context.make_vararg_function(f)
    }
    pub fn make_proxy<
        Ret: NativeSerialize<IT::FunctionIT>,
        F: Fn(
                NativeContextHolder<IT::FunctionIT>,
                NativeObjectHandle<IT::FunctionIT>,
                String,
            ) -> Result<Option<Ret>, CubeError>
            + 'static,
    >(
        &self,
        target: Option<NativeObjectHandle<IT>>,
        get_fn: F,
    ) -> Result<NativeObjectHandle<IT>, CubeError> {
        self.context.make_proxy(target, get_fn)
    }
}

impl<IT> NativeContextHolderRef for NativeContextHolder<IT>
where
    IT: InnerTypes + 'static,
    NativeContextHolder<IT>: 'static,
{
    fn as_any(&self) -> &dyn Any {
        self
    }
}
