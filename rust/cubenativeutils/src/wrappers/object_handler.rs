use super::context::NativeContextHolder;
use super::object::{
    NativeArray, NativeBoolean, NativeNumber, NativeObject, NativeString, NativeStruct,
};
use cubesql::CubeError;

pub struct NativeObjectHandler {
    object: Box<dyn NativeObject>,
}

impl NativeObjectHandler {
    pub fn new(object: Box<dyn NativeObject>) -> Self {
        Self { object }
    }

    pub fn object_ref(&self) -> &Box<dyn NativeObject> {
        &self.object
    }

    pub fn downcast_object_ref<T: NativeObject + 'static>(&self) -> Option<&T> {
        self.object.as_any().downcast_ref()
    }

    pub fn into_object(self) -> Box<dyn NativeObject> {
        self.object
    }

    pub fn downcast_object<T: NativeObject + 'static>(self) -> Result<Box<T>, CubeError> {
        self.object
            .into_any()
            .downcast::<T>()
            .map_err(|_| CubeError::internal("Unable to downcast object".to_string()))
    }

    #[allow(dead_code)]
    pub fn into_struct(self) -> Result<Box<dyn NativeStruct>, CubeError> {
        self.object.into_struct()
    }
    #[allow(dead_code)]
    pub fn into_array(self) -> Result<Box<dyn NativeArray>, CubeError> {
        self.object.into_array()
    }
    #[allow(dead_code)]
    pub fn into_string(self) -> Result<Box<dyn NativeString>, CubeError> {
        self.object.into_string()
    }
    #[allow(dead_code)]
    pub fn into_number(self) -> Result<Box<dyn NativeNumber>, CubeError> {
        self.object.into_number()
    }
    #[allow(dead_code)]
    pub fn into_boolean(self) -> Result<Box<dyn NativeBoolean>, CubeError> {
        self.object.into_boolean()
    }
    #[allow(dead_code)]
    pub fn to_struct(&self) -> Result<Box<dyn NativeStruct>, CubeError> {
        self.object.boxed_clone().into_struct()
    }
    #[allow(dead_code)]
    pub fn to_array(&self) -> Result<Box<dyn NativeArray>, CubeError> {
        self.object.boxed_clone().into_array()
    }
    #[allow(dead_code)]
    pub fn to_string(&self) -> Result<Box<dyn NativeString>, CubeError> {
        self.object.boxed_clone().into_string()
    }
    #[allow(dead_code)]
    pub fn to_number(&self) -> Result<Box<dyn NativeNumber>, CubeError> {
        self.object.boxed_clone().into_number()
    }
    #[allow(dead_code)]
    pub fn to_boolean(&self) -> Result<Box<dyn NativeBoolean>, CubeError> {
        self.object.boxed_clone().into_boolean()
    }
    pub fn is_null(&self) -> bool {
        self.object.is_null()
    }
    pub fn is_undefined(&self) -> bool {
        self.object.is_undefined()
    }

    pub fn get_context(&self) -> Result<NativeContextHolder, CubeError> {
        self.object.get_context()
    }
}

impl Clone for NativeObjectHandler {
    fn clone(&self) -> Self {
        Self {
            object: self.object.boxed_clone(),
        }
    }
}
