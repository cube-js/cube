use super::{inner_types::InnerTypes, object_handle::NativeObjectHandle};
use cubesql::CubeError;

pub trait NativeContext<IT: InnerTypes>: Clone {
    fn boolean(&self, v: bool) -> Result<IT::Boolean, CubeError>;
    fn string(&self, v: String) -> Result<IT::String, CubeError>;
    fn number(&self, v: f64) -> Result<IT::Number, CubeError>;
    fn undefined(&self) -> Result<NativeObjectHandle<IT>, CubeError>;
    fn empty_array(&self) -> Result<IT::Array, CubeError>;
    fn empty_struct(&self) -> Result<IT::Struct, CubeError>;
    //fn boxed<T: 'static>(&self, value: T) -> impl NativeBox<IT, T>;
    fn to_string_fn(&self, result: String) -> Result<IT::Function, CubeError>;
}

#[derive(Clone)]
pub struct NativeContextHolder<IT: InnerTypes> {
    context: IT::Context,
}

impl<IT: InnerTypes> NativeContextHolder<IT> {
    pub fn new(context: IT::Context) -> Self {
        Self { context }
    }
    pub fn context(&self) -> &impl NativeContext<IT> {
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
    pub fn empty_array(&self) -> Result<IT::Array, CubeError> {
        self.context.empty_array()
    }
    pub fn empty_struct(&self) -> Result<IT::Struct, CubeError> {
        self.context.empty_struct()
    }
    #[allow(dead_code)]
    pub fn to_string_fn(&self, result: String) -> Result<IT::Function, CubeError> {
        self.context.to_string_fn(result)
    }
}
