use super::{inner_types::InnerTypes, object_handle::NativeObjectHandle};
use cubesql::CubeError;

pub trait NativeObject<IT: InnerTypes>: Clone {
    fn get_context(&self) -> IT::Context;

    fn into_struct(self) -> Result<IT::Struct, CubeError>;
    fn into_array(self) -> Result<IT::Array, CubeError>;
    fn into_string(self) -> Result<IT::String, CubeError>;
    fn into_number(self) -> Result<IT::Number, CubeError>;
    fn into_boolean(self) -> Result<IT::Boolean, CubeError>;
    fn into_function(self) -> Result<IT::Function, CubeError>;
    fn is_null(&self) -> Result<bool, CubeError>;
    fn is_undefined(&self) -> Result<bool, CubeError>;
}

pub trait NativeType<IT: InnerTypes> {
    fn into_object(self) -> IT::Object;
}

pub trait NativeArray<IT: InnerTypes>: NativeType<IT> {
    fn len(&self) -> Result<u32, CubeError>;
    fn to_vec(&self) -> Result<Vec<NativeObjectHandle<IT>>, CubeError>;
    fn set(&self, index: u32, value: NativeObjectHandle<IT>) -> Result<bool, CubeError>;
    fn get(&self, index: u32) -> Result<NativeObjectHandle<IT>, CubeError>;
}

pub trait NativeStruct<IT: InnerTypes>: NativeType<IT> {
    fn get_field(&self, field_name: &str) -> Result<NativeObjectHandle<IT>, CubeError>;
    fn set_field(&self, field_name: &str, value: NativeObjectHandle<IT>)
        -> Result<bool, CubeError>;
    fn has_field(&self, field_name: &str) -> Result<bool, CubeError>;
    fn get_own_property_names(&self) -> Result<Vec<NativeObjectHandle<IT>>, CubeError>;

    fn call_method(
        &self,
        method: &str,
        args: Vec<NativeObjectHandle<IT>>,
    ) -> Result<NativeObjectHandle<IT>, CubeError>;
}

pub trait NativeFunction<IT: InnerTypes>: NativeType<IT> {
    fn call(&self, args: Vec<NativeObjectHandle<IT>>) -> Result<NativeObjectHandle<IT>, CubeError>;
    fn definition(&self) -> Result<String, CubeError>;
    fn args_names(&self) -> Result<Vec<String>, CubeError>;
}

pub trait NativeString<IT: InnerTypes>: NativeType<IT> {
    fn value(&self) -> Result<String, CubeError>;
}

pub trait NativeNumber<IT: InnerTypes>: NativeType<IT> {
    fn value(&self) -> Result<f64, CubeError>;
}

pub trait NativeBoolean<IT: InnerTypes>: NativeType<IT> {
    fn value(&self) -> Result<bool, CubeError>;
}

pub trait NativeBox<IT: InnerTypes, T: 'static>: NativeType<IT> {
    fn deref_value(&self) -> &T;
}
