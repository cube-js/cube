use super::{NeonObject, NeonTypeHandle};
use crate::wrappers::NativeObjectHandle;
use crate::wrappers::{
    context::NativeFinalize,
    inner_types::InnerTypes,
    neon::{context::ContextHolder, inner_types::NeonInnerTypes},
};
use std::cell::RefCell;
use std::marker::PhantomData;
use std::ops::DerefMut;

use crate::wrappers::object::{
    NativeBoolean, NativeBox, NativeNumber, NativeRoot, NativeString, NativeType,
};
use cubesql::CubeError;
use neon::prelude::*;

pub struct NeonString<C: Context<'static>> {
    object: NeonTypeHandle<C, JsString>,
}

impl<C: Context<'static>> NeonString<C> {
    pub fn new(object: NeonTypeHandle<C, JsString>) -> Self {
        Self { object }
    }
}

impl<C: Context<'static> + 'static> NativeType<NeonInnerTypes<C>> for NeonString<C> {
    fn into_object(self) -> NeonObject<C> {
        self.object.upcast()
    }
}

impl<C: Context<'static> + 'static> NativeString<NeonInnerTypes<C>> for NeonString<C> {
    fn value(&self) -> Result<String, CubeError> {
        self.object
            .map_neon_object::<_, _>(|cx, object| Ok(object.value(cx)))?
    }
}

pub struct NeonNumber<C: Context<'static>> {
    object: NeonTypeHandle<C, JsNumber>,
}

impl<C: Context<'static>> NeonNumber<C> {
    pub fn new(object: NeonTypeHandle<C, JsNumber>) -> Self {
        Self { object }
    }
}

impl<C: Context<'static> + 'static> NativeType<NeonInnerTypes<C>> for NeonNumber<C> {
    fn into_object(self) -> NeonObject<C> {
        self.object.upcast()
    }
}

impl<C: Context<'static> + 'static> NativeNumber<NeonInnerTypes<C>> for NeonNumber<C> {
    fn value(&self) -> Result<f64, CubeError> {
        self.object
            .map_neon_object::<_, _>(|cx, object| Ok(object.value(cx)))?
    }
}

pub struct NeonBoolean<C: Context<'static>> {
    object: NeonTypeHandle<C, JsBoolean>,
}

impl<C: Context<'static>> NeonBoolean<C> {
    pub fn new(object: NeonTypeHandle<C, JsBoolean>) -> Self {
        Self { object }
    }
}

impl<C: Context<'static> + 'static> NativeType<NeonInnerTypes<C>> for NeonBoolean<C> {
    fn into_object(self) -> NeonObject<C> {
        self.object.upcast()
    }
}

impl<C: Context<'static> + 'static> NativeBoolean<NeonInnerTypes<C>> for NeonBoolean<C> {
    fn value(&self) -> Result<bool, CubeError> {
        self.object
            .map_neon_object::<_, _>(|cx, object| Ok(object.value(cx)))?
    }
}

pub struct NeonBoxWrapper<T: 'static + NativeFinalize> {
    pub obj: T,
}

impl<T: 'static + NativeFinalize> NeonBoxWrapper<T> {
    pub fn new(obj: T) -> Self {
        Self { obj }
    }
}

impl<T: NativeFinalize> Finalize for NeonBoxWrapper<T> {
    fn finalize<'a, C: Context<'a>>(self, _: &mut C) {
        self.obj.finalize();
    }
}

pub struct NeonBox<C: Context<'static>, T: 'static + NativeFinalize> {
    object: NeonTypeHandle<C, JsBox<NeonBoxWrapper<T>>>,
    _marker: PhantomData<T>,
}

impl<C: Context<'static>, T: 'static + NativeFinalize> NeonBox<C, T> {
    pub fn new(object: NeonTypeHandle<C, JsBox<NeonBoxWrapper<T>>>) -> Self {
        Self {
            object,
            _marker: PhantomData::default(),
        }
    }
}

impl<C: Context<'static> + 'static, T: 'static + NativeFinalize> NativeType<NeonInnerTypes<C>>
    for NeonBox<C, T>
{
    fn into_object(self) -> NeonObject<C> {
        self.object.upcast()
    }
}

impl<C: Context<'static> + 'static, T: 'static + NativeFinalize> NativeBox<NeonInnerTypes<C>, T>
    for NeonBox<C, T>
{
    fn deref_value(&self) -> &T {
        &self.object.get_object_ref().obj
    }
}

pub struct NeonRoot {
    object: RefCell<Option<Root<JsObject>>>,
}

impl NeonRoot {
    pub fn try_new<C: Context<'static> + 'static>(
        context_holder: ContextHolder<C>,
        object: NeonTypeHandle<C, JsObject>,
    ) -> Result<Self, CubeError> {
        let obj = context_holder.with_context(|cx| object.get_object().root(cx))?;
        Ok(Self {
            object: RefCell::new(Some(obj)),
        })
    }
}

impl<C: Context<'static> + 'static> NativeRoot<NeonInnerTypes<C>> for NeonRoot {
    fn to_inner(
        &self,
        cx: &ContextHolder<C>,
    ) -> Result<NativeObjectHandle<NeonInnerTypes<C>>, CubeError> {
        if let Some(object) = &self.object.borrow().as_ref() {
            let res = cx.with_context(|cx| object.to_inner(cx))?;
            let res = NeonObject::new(cx.clone(), res.upcast());
            Ok(NativeObjectHandle::new(res))
        } else {
            Err(CubeError::internal("Root object is dropped".to_string()))
        }
    }

    fn drop_root(&self, cx: &ContextHolder<C>) -> Result<(), CubeError> {
        let mut object = self.object.borrow_mut();
        let object = std::mem::take(object.deref_mut());
        if let Some(object) = object {
            cx.with_context(|cx| object.drop(cx))
        } else {
            Ok(())
        }
    }
}
