use super::context::NativeContextHolder;
use super::object_handle::NativeObjectHandle;
use cubesql::CubeError;
use std::any::Any;

pub trait NativeObject: NativeBoxedClone {
    fn as_any(&self) -> &dyn Any;
    fn into_any(self: Box<Self>) -> Box<dyn Any>;

    fn get_context(&self) -> Result<NativeContextHolder, CubeError>;

    fn into_struct(self: Box<Self>) -> Result<Box<dyn NativeStruct>, CubeError>;
    fn into_array(self: Box<Self>) -> Result<Box<dyn NativeArray>, CubeError>;
    fn into_string(self: Box<Self>) -> Result<Box<dyn NativeString>, CubeError>;
    fn into_number(self: Box<Self>) -> Result<Box<dyn NativeNumber>, CubeError>;
    fn into_boolean(self: Box<Self>) -> Result<Box<dyn NativeBoolean>, CubeError>;
    fn is_null(&self) -> bool;
    fn is_undefined(&self) -> bool;
}

pub trait NativeType {
    fn into_object(self: Box<Self>) -> Box<dyn NativeObject>;
    fn get_object(&self) -> Box<dyn NativeObject>;
}

pub trait NativeArray: NativeType {
    fn len(&self) -> Result<u32, CubeError>;
    fn to_vec(&self) -> Result<Vec<NativeObjectHandle>, CubeError>;
    fn set(&self, index: u32, value: NativeObjectHandle) -> Result<bool, CubeError>;
    fn get(&self, index: u32) -> Result<NativeObjectHandle, CubeError>;
}

pub trait NativeStruct: NativeType {
    fn get_field(&self, field_name: &str) -> Result<NativeObjectHandle, CubeError>;
    fn set_field(&self, field_name: &str, value: NativeObjectHandle) -> Result<bool, CubeError>;
    fn get_own_property_names(&self) -> Result<Vec<NativeObjectHandle>, CubeError>;

    fn call_method(
        &self,
        method: &str,
        args: Vec<NativeObjectHandle>,
    ) -> Result<NativeObjectHandle, CubeError>;
}

pub trait NativeFunction: NativeType {
    fn call(&self, args: Vec<NativeObjectHandle>) -> Result<NativeObjectHandle, CubeError>;
}

pub trait NativeString: NativeType {
    fn value(&self) -> Result<String, CubeError>;
}

pub trait NativeNumber: NativeType {
    fn value(&self) -> Result<f64, CubeError>;
}

pub trait NativeBoolean: NativeType {
    fn value(&self) -> Result<bool, CubeError>;
}

pub trait NativeBoxedClone {
    fn boxed_clone(&self) -> Box<dyn NativeObject>;
}

impl<T: NativeObject + Clone + 'static> NativeBoxedClone for T {
    fn boxed_clone(&self) -> Box<dyn NativeObject> {
        Box::new(self.clone())
    }
}
