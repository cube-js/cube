use super::object::{
    NativeArray, NativeBoolean, NativeNumber, NativeObject, NativeString, NativeStruct,
};
pub trait NativeContext: ContextBoxedClone {
    fn boolean(&self, v: bool) -> Box<dyn NativeBoolean>;
    fn string(&self, v: String) -> Box<dyn NativeString>;
    fn number(&self, v: f64) -> Box<dyn NativeNumber>;
    fn undefined(&self) -> Box<dyn NativeObject>;
    fn empty_array(&self) -> Box<dyn NativeArray>;
    fn empty_struct(&self) -> Box<dyn NativeStruct>;
}

pub struct NativeContextHolder {
    context: Box<dyn NativeContext>,
}

impl NativeContextHolder {
    pub fn new(context: Box<dyn NativeContext>) -> Self {
        Self { context }
    }
    pub fn context(&self) -> &Box<dyn NativeContext> {
        &self.context
    }
    pub fn boolean(&self, v: bool) -> Box<dyn NativeBoolean> {
        self.context.boolean(v)
    }
    pub fn string(&self, v: String) -> Box<dyn NativeString> {
        self.context.string(v)
    }
    pub fn number(&self, v: f64) -> Box<dyn NativeNumber> {
        self.context.number(v)
    }
    pub fn undefined(&self) -> Box<dyn NativeObject> {
        self.context.undefined()
    }
    pub fn empty_array(&self) -> Box<dyn NativeArray> {
        self.context.empty_array()
    }
    pub fn empty_struct(&self) -> Box<dyn NativeStruct> {
        self.context.empty_struct()
    }
}

impl Clone for NativeContextHolder {
    fn clone(&self) -> Self {
        NativeContextHolder {
            context: self.context.boxed_clone(),
        }
    }
}

pub trait ContextBoxedClone {
    fn boxed_clone(&self) -> Box<dyn NativeContext>;
}

impl<T: NativeContext + Clone + 'static> ContextBoxedClone for T {
    fn boxed_clone(&self) -> Box<dyn NativeContext> {
        Box::new(self.clone())
    }
}
