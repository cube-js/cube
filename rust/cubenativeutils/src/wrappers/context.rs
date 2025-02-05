use super::{inner_types::InnerTypes, object_handle::NativeObjectHandle};
use cubesql::CubeError;
use std::any::Any;
use std::rc::Rc;

pub trait NativeContext<IT: InnerTypes>: Clone {
    fn boolean(&self, v: bool) -> Result<IT::Boolean, CubeError>;
    fn string(&self, v: String) -> Result<IT::String, CubeError>;
    fn number(&self, v: f64) -> Result<IT::Number, CubeError>;
    fn undefined(&self) -> Result<NativeObjectHandle<IT>, CubeError>;
    fn empty_array(&self) -> Result<IT::Array, CubeError>;
    fn empty_struct(&self) -> Result<IT::Struct, CubeError>;
    fn to_string_fn(&self, result: String) -> Result<IT::Function, CubeError>;

    //IMPORTANT NOTE: Using of any native args in callback (as well as any native objects created
    //with NativeContextHolder passed to callback) outside of callback will cause error from
    //runtime lifetime check
    fn function<F>(&self, function: F) -> Result<IT::Function, CubeError>
    where
        F: Fn(
                Rc<NativeContextHolder<IT::FunctionIT>>,
                Vec<NativeObjectHandle<IT::FunctionIT>>,
            ) -> Result<NativeObjectHandle<IT::FunctionIT>, CubeError>
            + 'static;

    fn boxed<T: NativeFinalize + 'static>(
        &self,
        object: T,
    ) -> Result<impl NativeBox<IT, T>, CubeError>;
    fn global(&self, name: &str) -> Result<NativeObjectHandle<IT>, CubeError>;
}

//Top level reference to ContextHolder for using in top level interfaces. Should be downcaster to
//specific context for use
pub trait NativeContextHolderRef {
    fn as_any(self: Rc<Self>) -> Rc<dyn Any>;
}

#[derive(Clone)]
pub struct NativeContextHolder<IT: InnerTypes> {
    context: IT::Context,
}

impl<IT: InnerTypes> NativeContextHolder<IT> {
    pub fn new(context: IT::Context) -> Rc<Self> {
        Rc::new(Self { context })
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
    pub fn empty_array(&self) -> Result<IT::Array, CubeError> {
        self.context.empty_array()
    }
    pub fn empty_struct(&self) -> Result<IT::Struct, CubeError> {
        self.context.empty_struct()
    }
    pub fn to_string_fn(&self, result: String) -> Result<IT::Function, CubeError> {
        self.context.to_string_fn(result)
    }
    pub fn function<F>(&self, function: F) -> Result<IT::Function, CubeError>
    where
        F: Fn(
                Rc<NativeContextHolder<IT::FunctionIT>>,
                Vec<NativeObjectHandle<IT::FunctionIT>>,
            ) -> Result<NativeObjectHandle<IT::FunctionIT>, CubeError>
            + 'static,
    {
        self.context.function(function)
    }
    pub fn boxed<T: NativeFinalize + 'static>(
        &self,
        object: T,
    ) -> Result<impl NativeBox<IT, T> + '_, CubeError> {
        self.context.boxed(object)
    }
    pub fn global(&self, name: &str) -> Result<NativeObjectHandle<IT>, CubeError> {
        self.context.global(name)
    }
    pub fn as_context_ref(self: &Rc<Self>) -> Rc<dyn NativeContextHolderRef> {
        self.clone()
    }
}

impl<IT: InnerTypes> NativeContextHolderRef for NativeContextHolder<IT> {
    fn as_any(self: Rc<Self>) -> Rc<dyn Any> {
        self.clone()
    }
}

//FIXME For now we don't allow js calls on finalize, so it's only to clean rust resources
pub trait NativeFinalize: Sized {
    fn finalize(self) {}
}

impl<T: NativeFinalize> NativeFinalize for std::rc::Rc<T> {
    fn finalize(self) {
        if let Ok(v) = std::rc::Rc::try_unwrap(self) {
            v.finalize();
        }
    }
}

impl<T: NativeFinalize> NativeFinalize for std::cell::RefCell<T> {
    fn finalize(self) {
        self.into_inner().finalize();
    }
}
