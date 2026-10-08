use super::{inner_types::InnerTypes, object_handle::NativeObjectHandle, NativeRustHandle};
use crate::CubeError;

pub trait NativeObject<IT: InnerTypes>: Clone {
    fn get_context(&self) -> IT::Context;

    fn into_struct(self) -> Result<IT::Struct, CubeError>;
    fn into_array(self) -> Result<IT::Array, CubeError>;
    fn into_string(self) -> Result<IT::String, CubeError>;
    fn into_number(self) -> Result<IT::Number, CubeError>;
    fn into_boolean(self) -> Result<IT::Boolean, CubeError>;
    fn into_function(self) -> Result<IT::Function, CubeError>;
    fn into_rust_box(self) -> Result<IT::RustBox, CubeError>;
    fn is_null(&self) -> Result<bool, CubeError>;
    fn is_undefined(&self) -> Result<bool, CubeError>;
    fn into_typed(self) -> Result<NativeTypedObject<IT>, CubeError>;
    fn clone_to_context(&self, context: &IT::Context) -> Self;
    fn clone_to_function_context(
        &self,
        context: &<IT::FunctionIT as InnerTypes>::Context,
    ) -> <IT::FunctionIT as InnerTypes>::Object;
}

pub enum NativeTypedObject<IT: InnerTypes> {
    Null,
    Undefined,
    Boolean(IT::Boolean),
    Number(IT::Number),
    String(IT::String),
    Array(IT::Array),
    Struct(IT::Struct),
    Function(IT::Function),
    RustBox(IT::RustBox),
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
    /// Own enumerable string-keyed properties with their values, read in one pass.
    fn entries(&self) -> Result<Vec<(String, NativeObjectHandle<IT>)>, CubeError>;
    /// `entries` restricted to `fields`; values of other properties are not read.
    ///
    /// Undeclared keys never reach the serde visitor, so a struct deserialized through this
    /// (`deserialize_struct`) can't honour `#[serde(deny_unknown_fields)]`.
    fn entries_for_fields(
        &self,
        fields: &'static [&'static str],
    ) -> Result<Vec<(&'static str, NativeObjectHandle<IT>)>, CubeError>;
    fn set_field(&self, field_name: &str, value: NativeObjectHandle<IT>)
        -> Result<bool, CubeError>;
    fn has_field(&self, field_name: &str) -> Result<bool, CubeError>;

    fn call_method(
        &self,
        method: &str,
        args: Vec<NativeObjectHandle<IT>>,
    ) -> Result<NativeObjectHandle<IT>, CubeError>;
}

pub trait NativeFunction<IT: InnerTypes>: NativeType<IT> {
    fn call(&self, args: Vec<NativeObjectHandle<IT>>) -> Result<NativeObjectHandle<IT>, CubeError>;
    fn construct(
        &self,
        args: Vec<NativeObjectHandle<IT>>,
    ) -> Result<NativeObjectHandle<IT>, CubeError>;
    fn definition(&self) -> Result<String, CubeError>;
    fn args_names(&self) -> Result<Vec<String>, CubeError>;
}

pub trait NativeString<IT: InnerTypes>: NativeType<IT> {
    fn value(&self) -> Result<String, CubeError>;
    fn into_value(self) -> Result<String, CubeError>
    where
        Self: Sized,
    {
        self.value()
    }
}

pub trait NativeNumber<IT: InnerTypes>: NativeType<IT> {
    fn value(&self) -> Result<f64, CubeError>;
}

pub trait NativeBoolean<IT: InnerTypes>: NativeType<IT> {
    fn value(&self) -> Result<bool, CubeError>;
}

pub trait NativeRustBox<IT: InnerTypes>: NativeType<IT> {
    fn handle(&self) -> &NativeRustHandle;
}

pub trait NativeProxy<IT: InnerTypes> {
    fn get(&self, property_name: &str) -> Result<IT::Object, CubeError>;
}
