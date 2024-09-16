use super::inner_types::InnerTypes;
use super::object_handle::NativeObjectHandle;

pub trait NativeContext<IT: InnerTypes>: Clone {
    fn boolean(&self, v: bool) -> IT::Boolean;
    fn string(&self, v: String) -> IT::String;
    fn number(&self, v: f64) -> IT::Number;
    fn undefined(&self) -> NativeObjectHandle<IT>;
    fn empty_array(&self) -> IT::Array;
    fn empty_struct(&self) -> IT::Struct;
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
    pub fn boolean(&self, v: bool) -> IT::Boolean {
        self.context.boolean(v)
    }
    pub fn string(&self, v: String) -> IT::String {
        self.context.string(v)
    }
    pub fn number(&self, v: f64) -> IT::Number {
        self.context.number(v)
    }
    pub fn undefined(&self) -> NativeObjectHandle<IT> {
        self.context.undefined()
    }
    pub fn empty_array(&self) -> IT::Array {
        self.context.empty_array()
    }
    pub fn empty_struct(&self) -> IT::Struct {
        self.context.empty_struct()
    }
}
