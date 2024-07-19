use super::NeonObject;
use crate::wrappers::object::{
    NativeBoolean, NativeBoxedClone, NativeNumber, NativeObject, NativeString, NativeType,
};
use crate::wrappers::object_handle::NativeObjectHandle;
use cubesql::CubeError;
use neon::prelude::*;

pub struct NeonString<C: Context<'static>> {
    object: Box<NeonObject<C>>,
}

impl<C: Context<'static>> NeonString<C> {
    pub fn new(object: Box<NeonObject<C>>) -> Box<Self> {
        Box::new(Self { object })
    }
}

impl<C: Context<'static> + 'static> NativeType for NeonString<C> {
    fn into_object(self: Box<Self>) -> Box<dyn NativeObject> {
        self.object
    }
    fn get_object(&self) -> Box<dyn NativeObject> {
        self.object.boxed_clone()
    }
}

impl<C: Context<'static> + 'static> NativeString for NeonString<C> {
    fn value(&self) -> Result<String, CubeError> {
        self.object
            .map_downcast_neon_object::<JsString, _, _>(|cx, object| Ok(object.value(cx)))
    }
}

pub struct NeonNumber<C: Context<'static>> {
    object: Box<NeonObject<C>>,
}

impl<C: Context<'static> + 'static> NativeType for NeonNumber<C> {
    fn into_object(self: Box<Self>) -> Box<dyn NativeObject> {
        self.object
    }
    fn get_object(&self) -> Box<dyn NativeObject> {
        self.object.boxed_clone()
    }
}

impl<C: Context<'static>> NeonNumber<C> {
    pub fn new(object: Box<NeonObject<C>>) -> Box<Self> {
        Box::new(Self { object })
    }
}

impl<C: Context<'static> + 'static> NativeNumber for NeonNumber<C> {
    fn value(&self) -> Result<f64, CubeError> {
        self.object
            .map_downcast_neon_object::<JsNumber, _, _>(|cx, object| Ok(object.value(cx)))
    }
}

pub struct NeonBoolean<C: Context<'static>> {
    object: Box<NeonObject<C>>,
}

impl<C: Context<'static> + 'static> NativeType for NeonBoolean<C> {
    fn into_object(self: Box<Self>) -> Box<dyn NativeObject> {
        self.object
    }
    fn get_object(&self) -> Box<dyn NativeObject> {
        self.object.boxed_clone()
    }
}

impl<C: Context<'static>> NeonBoolean<C> {
    pub fn new(object: Box<NeonObject<C>>) -> Box<Self> {
        Box::new(Self { object })
    }
}

impl<C: Context<'static> + 'static> NativeBoolean for NeonBoolean<C> {
    fn value(&self) -> Result<bool, CubeError> {
        self.object
            .map_downcast_neon_object::<JsBoolean, _, _>(|cx, object| Ok(object.value(cx)))
    }
}
