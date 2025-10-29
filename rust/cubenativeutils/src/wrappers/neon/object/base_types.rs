use super::primitive_root_holder::*;
use super::{NeonObject, RootHolder};
use crate::wrappers::neon::inner_types::NeonInnerTypes;

use crate::wrappers::object::{NativeBoolean, NativeNumber, NativeString, NativeType};
use cubesql::CubeError;
use neon::prelude::*;

pub struct NeonString<C: Context<'static>> {
    holder: PrimitiveNeonTypeHolder<C, JsString>,
}

impl<C: Context<'static>> NeonString<C> {
    pub fn new(holder: PrimitiveNeonTypeHolder<C, JsString>) -> Self {
        Self { holder }
    }
}

impl<C: Context<'static> + 'static> NativeType<NeonInnerTypes<C>> for NeonString<C> {
    fn into_object(self) -> NeonObject<C> {
        let root_holder = RootHolder::from_typed(self.holder);
        NeonObject::form_root(root_holder)
    }
}

impl<C: Context<'static> + 'static> NativeString<NeonInnerTypes<C>> for NeonString<C> {
    fn value(&self) -> Result<String, CubeError> {
        self.holder
            .map_neon_object::<_, _>(|cx, object| Ok(object.value(cx)))?
    }
}

pub struct NeonNumber<C: Context<'static>> {
    holder: PrimitiveNeonTypeHolder<C, JsNumber>,
}

impl<C: Context<'static>> NeonNumber<C> {
    pub fn new(holder: PrimitiveNeonTypeHolder<C, JsNumber>) -> Self {
        Self { holder }
    }
}

impl<C: Context<'static> + 'static> NativeType<NeonInnerTypes<C>> for NeonNumber<C> {
    fn into_object(self) -> NeonObject<C> {
        let root_holder = RootHolder::from_typed(self.holder);
        NeonObject::form_root(root_holder)
    }
}

impl<C: Context<'static> + 'static> NativeNumber<NeonInnerTypes<C>> for NeonNumber<C> {
    fn value(&self) -> Result<f64, CubeError> {
        self.holder
            .map_neon_object::<_, _>(|cx, object| Ok(object.value(cx)))?
    }
}

pub struct NeonBoolean<C: Context<'static>> {
    holder: PrimitiveNeonTypeHolder<C, JsBoolean>,
}

impl<C: Context<'static>> NeonBoolean<C> {
    pub fn new(holder: PrimitiveNeonTypeHolder<C, JsBoolean>) -> Self {
        Self { holder }
    }
}

impl<C: Context<'static> + 'static> NativeType<NeonInnerTypes<C>> for NeonBoolean<C> {
    fn into_object(self) -> NeonObject<C> {
        let root_holder = RootHolder::from_typed(self.holder);
        NeonObject::form_root(root_holder)
    }
}

impl<C: Context<'static> + 'static> NativeBoolean<NeonInnerTypes<C>> for NeonBoolean<C> {
    fn value(&self) -> Result<bool, CubeError> {
        self.holder
            .map_neon_object::<_, _>(|cx, object| Ok(object.value(cx)))?
    }
}

/* pub struct NeonBox<C: Context<'static>, T: 'static> {
    object: NeonTypeHandle<C, JsBox<T>>,
    _marker: PhantomData<T>,
}

impl<C: Context<'static>, T: 'static> NeonBox<C, T> {
    pub fn new(object: NeonTypeHandle<C, JsBox<T>>) -> Self {
        Self {
            object,
            _marker: PhantomData,
        }
    }
}

impl<C: Context<'static> + 'static, T: 'static> NativeType<NeonInnerTypes<C>> for NeonBox<C, T> {
    fn into_object(self) -> NeonObject<C> {
        self.object.upcast()
    }
}

impl<C: Context<'static> + 'static, T: 'static> NativeBox<NeonInnerTypes<C>, T> for NeonBox<C, T> {
    fn deref_value(&self) -> &T {
        self.object.get_object_ref().deref()
    }
} */
