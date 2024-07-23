use super::NeonObject;
use crate::wrappers::neon::inner_types::NeonInnerTypes;

use crate::wrappers::object::{NativeBoolean, NativeNumber, NativeString, NativeType};
use cubesql::CubeError;
use neon::prelude::*;

pub struct NeonString<'cx, C: Context<'cx>> {
    object: NeonObject<'cx, C>,
}

impl<'cx, C: Context<'cx>> NeonString<'cx, C> {
    pub fn new(object: NeonObject<'cx, C>) -> Self {
        Self { object }
    }
}

impl<'cx, C: Context<'cx> + 'cx> NativeType<NeonInnerTypes<'cx, C>> for NeonString<'cx, C> {
    fn into_object(self) -> NeonObject<'cx, C> {
        self.object
    }
}

impl<'cx, C: Context<'cx> + 'cx> NativeString<NeonInnerTypes<'cx, C>> for NeonString<'cx, C> {
    fn value(&self) -> Result<String, CubeError> {
        self.object
            .map_downcast_neon_object::<JsString, _, _>(|cx, object| Ok(object.value(cx)))
    }
}

pub struct NeonNumber<'cx, C: Context<'cx>> {
    object: NeonObject<'cx, C>,
}

impl<'cx, C: Context<'cx>> NeonNumber<'cx, C> {
    pub fn new(object: NeonObject<'cx, C>) -> Self {
        Self { object }
    }
}

impl<'cx, C: Context<'cx> + 'cx> NativeType<NeonInnerTypes<'cx, C>> for NeonNumber<'cx, C> {
    fn into_object(self) -> NeonObject<'cx, C> {
        self.object
    }
}

impl<'cx, C: Context<'cx> + 'cx> NativeNumber<NeonInnerTypes<'cx, C>> for NeonNumber<'cx, C> {
    fn value(&self) -> Result<f64, CubeError> {
        self.object
            .map_downcast_neon_object::<JsNumber, _, _>(|cx, object| Ok(object.value(cx)))
    }
}

pub struct NeonBoolean<'cx, C: Context<'cx>> {
    object: NeonObject<'cx, C>,
}

impl<'cx, C: Context<'cx>> NeonBoolean<'cx, C> {
    pub fn new(object: NeonObject<'cx, C>) -> Self {
        Self { object }
    }
}

impl<'cx, C: Context<'cx> + 'cx> NativeType<NeonInnerTypes<'cx, C>> for NeonBoolean<'cx, C> {
    fn into_object(self) -> NeonObject<'cx, C> {
        self.object
    }
}

impl<'cx, C: Context<'cx> + 'cx> NativeBoolean<NeonInnerTypes<'cx, C>> for NeonBoolean<'cx, C> {
    fn value(&self) -> Result<bool, CubeError> {
        self.object
            .map_downcast_neon_object::<JsBoolean, _, _>(|cx, object| Ok(object.value(cx)))
    }
}
