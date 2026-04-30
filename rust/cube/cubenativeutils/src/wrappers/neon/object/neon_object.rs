use super::{
    base_types::{NeonBoolean, NeonNumber, NeonString},
    neon_array::NeonArray,
    neon_function::NeonFunction,
    neon_struct::NeonStruct,
    RootHolder,
};
use crate::wrappers::object::NativeObject;
use crate::wrappers::{
    neon::{context::ContextHolder, inner_types::NeonInnerTypes},
    NativeObjectHandle,
};
use crate::CubeError;
use neon::prelude::*;

pub(crate) trait IntoNeonObject<C: Context<'static> + 'static> {
    fn into_neon_object(self, context: ContextHolder<C>) -> Result<NeonObject<C>, CubeError>;
}

impl<C: Context<'static> + 'static, V: Value> IntoNeonObject<C> for Handle<'static, V> {
    fn into_neon_object(self, context: ContextHolder<C>) -> Result<NeonObject<C>, CubeError> {
        NeonObject::new(context, self)
    }
}

pub struct NeonObject<C: Context<'static> + 'static> {
    root_holder: RootHolder<C>,
}

impl<C: Context<'static> + 'static> NeonObject<C> {
    pub fn new<V: Value>(
        context: ContextHolder<C>,
        object: Handle<'static, V>,
    ) -> Result<Self, CubeError> {
        let root_holder = RootHolder::new(context.clone(), object)?;
        Ok(Self { root_holder })
    }

    pub fn form_root(root: RootHolder<C>) -> Self {
        Self { root_holder: root }
    }

    pub fn get_js_value(&self) -> Result<Handle<'static, JsValue>, CubeError> {
        match &self.root_holder {
            RootHolder::Null(v) => v.map_neon_object(|_cx, obj| Ok(obj.upcast())),
            RootHolder::Undefined(v) => v.map_neon_object(|_cx, obj| Ok(obj.upcast())),
            RootHolder::Boolean(v) => v.map_neon_object(|_cx, obj| Ok(obj.upcast())),
            RootHolder::Number(v) => v.map_neon_object(|_cx, obj| Ok(obj.upcast())),
            RootHolder::String(v) => v.map_neon_object(|_cx, obj| Ok(obj.upcast())),
            RootHolder::Array(v) => v.map_neon_object(|_cx, obj| Ok(obj.upcast())),
            RootHolder::Function(v) => v.map_neon_object(|_cx, obj| Ok(obj.upcast())),
            RootHolder::Struct(v) => v.map_neon_object(|_cx, obj| Ok(obj.upcast())),
        }
    }

    pub fn is_a<U: Value>(&self) -> Result<bool, CubeError> {
        let obj = self.get_js_value()?;
        self.root_holder
            .get_context()
            .with_context(|cx| obj.is_a::<U, _>(cx))
    }

    pub fn is_null(&self) -> bool {
        matches!(self.root_holder, RootHolder::Null(_))
    }

    pub fn is_undefined(&self) -> bool {
        matches!(self.root_holder, RootHolder::Undefined(_))
    }
}

impl<C: Context<'static> + 'static> NativeObject<NeonInnerTypes<C>> for NeonObject<C> {
    fn get_context(&self) -> ContextHolder<C> {
        self.root_holder.get_context()
    }

    fn into_struct(self) -> Result<NeonStruct<C>, CubeError> {
        let obj_holder = self.root_holder.into_struct()?;
        Ok(NeonStruct::new(obj_holder))
    }
    fn into_function(self) -> Result<NeonFunction<C>, CubeError> {
        let obj_holder = self.root_holder.into_function()?;
        Ok(NeonFunction::new(obj_holder))
    }
    fn into_array(self) -> Result<NeonArray<C>, CubeError> {
        let obj_holder = self.root_holder.into_array()?;
        Ok(NeonArray::new(obj_holder))
    }
    fn into_string(self) -> Result<NeonString<C>, CubeError> {
        let holder = self.root_holder.into_string()?;
        Ok(NeonString::new(holder))
    }
    fn into_number(self) -> Result<NeonNumber<C>, CubeError> {
        let holder = self.root_holder.into_number()?;
        Ok(NeonNumber::new(holder))
    }
    fn into_boolean(self) -> Result<NeonBoolean<C>, CubeError> {
        let holder = self.root_holder.into_boolean()?;
        Ok(NeonBoolean::new(holder))
    }

    fn is_null(&self) -> Result<bool, CubeError> {
        Ok(self.is_null())
    }

    fn is_undefined(&self) -> Result<bool, CubeError> {
        Ok(self.is_undefined())
    }

    fn clone_to_context(&self, context: &ContextHolder<C>) -> Self {
        Self {
            root_holder: self.root_holder.clone_to_context(context),
        }
    }

    fn clone_to_function_context(
        &self,
        context: &ContextHolder<FunctionContext<'static>>,
    ) -> NeonObject<FunctionContext<'static>> {
        NeonObject {
            root_holder: self.root_holder.clone_to_context(context),
        }
    }
}

impl<C: Context<'static> + 'static> Clone for NeonObject<C> {
    fn clone(&self) -> Self {
        Self {
            root_holder: self.root_holder.clone(),
        }
    }
}

impl<C: Context<'static> + 'static> From<NeonObject<C>> for NativeObjectHandle<NeonInnerTypes<C>> {
    fn from(object: NeonObject<C>) -> Self {
        Self::new(object)
    }
}
