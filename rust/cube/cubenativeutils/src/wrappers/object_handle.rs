use super::{inner_types::InnerTypes, object::NativeObject};
use super::{
    NativeBoolean, NativeContextHolder, NativeContextHolderRef, NativeNumber, NativeString,
    NativeStruct,
};
use crate::CubeError;

#[derive(Clone)]
pub struct NativeObjectHandle<IT: InnerTypes> {
    object: IT::Object,
}

impl<IT: InnerTypes> NativeObjectHandle<IT> {
    pub fn new(object: IT::Object) -> Self {
        Self { object }
    }

    pub fn object_ref(&self) -> &IT::Object {
        &self.object
    }

    pub fn into_object(self) -> IT::Object {
        self.object
    }

    pub fn into_struct(self) -> Result<IT::Struct, CubeError> {
        self.object.into_struct()
    }
    pub fn into_function(self) -> Result<IT::Function, CubeError> {
        self.object.into_function()
    }
    pub fn into_array(self) -> Result<IT::Array, CubeError> {
        self.object.into_array()
    }
    pub fn into_string(self) -> Result<IT::String, CubeError> {
        self.object.into_string()
    }
    pub fn into_number(self) -> Result<IT::Number, CubeError> {
        self.object.into_number()
    }
    pub fn into_boolean(self) -> Result<IT::Boolean, CubeError> {
        self.object.into_boolean()
    }
    pub fn to_struct(&self) -> Result<IT::Struct, CubeError> {
        self.object.clone().into_struct()
    }
    pub fn to_function(&self) -> Result<IT::Function, CubeError> {
        self.object.clone().into_function()
    }
    pub fn to_array(&self) -> Result<IT::Array, CubeError> {
        self.object.clone().into_array()
    }
    pub fn to_string(&self) -> Result<IT::String, CubeError> {
        self.object.clone().into_string()
    }
    pub fn to_number(&self) -> Result<IT::Number, CubeError> {
        self.object.clone().into_number()
    }
    pub fn to_boolean(&self) -> Result<IT::Boolean, CubeError> {
        self.object.clone().into_boolean()
    }
    pub fn is_null(&self) -> Result<bool, CubeError> {
        self.object.is_null()
    }
    pub fn is_undefined(&self) -> Result<bool, CubeError> {
        self.object.is_undefined()
    }
    pub fn get_context(&self) -> IT::Context {
        self.object.get_context()
    }

    pub fn convert_to_string(&self) -> Result<String, CubeError> {
        if let Ok(str) = self.to_string() {
            return str.value();
        }
        if self.is_null()? {
            return Ok("".to_string());
        }
        // Mirror JS template-literal coercion (`String(value)`) for primitives —
        // these never expose a struct-side `toString`, so the struct fallback
        // below would error otherwise.
        if let Ok(n) = self.to_number() {
            return Ok(n.value()?.to_string());
        }
        if let Ok(b) = self.to_boolean() {
            return Ok(b.value()?.to_string());
        }
        self.to_struct()?
            .call_method("toString", vec![])?
            .into_string()?
            .value()
    }

    pub fn try_clone_to_context_ref(
        &self,
        context_ref: &dyn NativeContextHolderRef,
    ) -> Result<Self, CubeError> {
        if let Some(context_holder) = context_ref
            .as_any()
            .downcast_ref::<NativeContextHolder<IT>>()
        {
            Ok(Self::new(
                self.object.clone_to_context(context_holder.context()),
            ))
        } else {
            Err(CubeError::internal(format!("wrong context reference type")))
        }
    }

    pub fn clone_to_function_context_ref(
        &self,
        context_ref: &dyn NativeContextHolderRef,
    ) -> Result<NativeObjectHandle<IT::FunctionIT>, CubeError> {
        if let Some(context_holder) = context_ref
            .as_any()
            .downcast_ref::<NativeContextHolder<IT::FunctionIT>>()
        {
            Ok(NativeObjectHandle::new(
                self.object
                    .clone_to_function_context(context_holder.context()),
            ))
        } else {
            Err(CubeError::internal(format!("wrong context reference type")))
        }
    }
}
