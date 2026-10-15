use super::{ObjectNeonTypeHolder, PrimitiveNeonTypeHolder};
use crate::wrappers::neon::context::ContextHolder;
use crate::wrappers::rust_handle::NativeRustHandle;
use crate::CubeError;
use neon::prelude::*;
use neon::sys::bindings as napi;
pub trait Upcast<C: Context<'static> + 'static> {
    fn upcast(self) -> RootHolder<C>;
}

macro_rules! impl_upcast {
    ($($holder:ty => $variant:ident),+ $(,)?) => {
        $(
            impl<C: Context<'static> + 'static> Upcast<C> for $holder {
                fn upcast(self) -> RootHolder<C> {
                    RootHolder::$variant(self)
                }
            }
        )+
    };
}

macro_rules! define_into_method {
    ($method_name:ident, $variant:ident, $holder_type:ty, $error_msg:expr) => {
        pub fn $method_name(self) -> Result<$holder_type, CubeError> {
            match self {
                Self::$variant(v) => Ok(v),
                _ => Err(CubeError::internal($error_msg.to_string())),
            }
        }
    };
}

impl_upcast!(
    PrimitiveNeonTypeHolder<C, JsNull> => Null,
    PrimitiveNeonTypeHolder<C, JsUndefined> => Undefined,
    PrimitiveNeonTypeHolder<C, JsBoolean> => Boolean,
    PrimitiveNeonTypeHolder<C, JsNumber> => Number,
    PrimitiveNeonTypeHolder<C, JsString> => String,
    ObjectNeonTypeHolder<C, JsArray> => Array,
    ObjectNeonTypeHolder<C, JsFunction> => Function,
    ObjectNeonTypeHolder<C, JsObject> => Struct,
    ObjectNeonTypeHolder<C, JsBox<NativeRustHandle>> => RustBox,
);

pub enum RootHolder<C: Context<'static> + 'static> {
    Null(PrimitiveNeonTypeHolder<C, JsNull>),
    Undefined(PrimitiveNeonTypeHolder<C, JsUndefined>),
    Boolean(PrimitiveNeonTypeHolder<C, JsBoolean>),
    Number(PrimitiveNeonTypeHolder<C, JsNumber>),
    String(PrimitiveNeonTypeHolder<C, JsString>),
    Array(ObjectNeonTypeHolder<C, JsArray>),
    Function(ObjectNeonTypeHolder<C, JsFunction>),
    Struct(ObjectNeonTypeHolder<C, JsObject>),
    RustBox(ObjectNeonTypeHolder<C, JsBox<NativeRustHandle>>),
}

impl<C: Context<'static> + 'static> RootHolder<C> {
    pub fn new<V: Value>(
        context: ContextHolder<C>,
        value: Handle<'static, V>,
    ) -> Result<Self, CubeError> {
        context
            .clone()
            .with_context(|cx| Self::new_in(cx, &context, value.upcast()))?
    }

    /// Same as `new`, for callers already inside `with_context`, which is not re-entrant.
    pub fn new_in(
        cx: &mut C,
        context: &ContextHolder<C>,
        value: Handle<'static, JsValue>,
    ) -> Result<Self, CubeError> {
        let raw = value.to_raw();
        // One `napi_typeof` instead of an `is_a` probe (each its own `napi_typeof`) per
        // candidate type plus another check in `downcast`.
        let mut value_type = napi::ValueType::Undefined;
        // SAFETY: `cx` is the live context the handle was created in, and `value_type` is a valid
        // out-pointer.
        unsafe { napi::typeof_value(cx.to_raw(), raw, &mut value_type) }
            .map_err(|status| CubeError::internal(format!("napi_typeof failed: {status:?}")))?;

        // SAFETY (all `from_raw` calls below): `raw` is valid for `'static` as `value` is, and
        // `value_type` (plus `is_a` for arrays) proves it has the type it is wrapped as.
        let holder = match value_type {
            napi::ValueType::Undefined => Self::Undefined(PrimitiveNeonTypeHolder::new(
                context.clone(),
                unsafe { JsUndefined::from_raw(&*cx, raw) },
                cx,
            )),
            napi::ValueType::Null => Self::Null(PrimitiveNeonTypeHolder::new(
                context.clone(),
                unsafe { JsNull::from_raw(&*cx, raw) },
                cx,
            )),
            napi::ValueType::Boolean => Self::Boolean(PrimitiveNeonTypeHolder::new(
                context.clone(),
                unsafe { JsBoolean::from_raw(&*cx, raw) },
                cx,
            )),
            napi::ValueType::Number => Self::Number(PrimitiveNeonTypeHolder::new(
                context.clone(),
                unsafe { JsNumber::from_raw(&*cx, raw) },
                cx,
            )),
            napi::ValueType::String => Self::String(PrimitiveNeonTypeHolder::new(
                context.clone(),
                unsafe { JsString::from_raw(&*cx, raw) },
                cx,
            )),
            napi::ValueType::Function => Self::Function(ObjectNeonTypeHolder::new(
                context.clone(),
                unsafe { JsFunction::from_raw(&*cx, raw) },
                cx,
            )),
            napi::ValueType::Object if value.is_a::<JsArray, _>(cx) => {
                Self::Array(ObjectNeonTypeHolder::new(
                    context.clone(),
                    unsafe { JsArray::from_raw(&*cx, raw) },
                    cx,
                ))
            }
            napi::ValueType::Object => Self::Struct(ObjectNeonTypeHolder::new(
                context.clone(),
                unsafe { JsObject::from_raw(&*cx, raw) },
                cx,
            )),
            // A box's type tag is checked by neon's downcast, so it isn't built from `raw`.
            napi::ValueType::External => match value.downcast::<JsBox<NativeRustHandle>, _>(cx) {
                Ok(boxed) => Self::RustBox(ObjectNeonTypeHolder::new(context.clone(), boxed, cx)),
                Err(_) => return Err(Self::unsupported(value_type)),
            },
            napi::ValueType::Symbol | napi::ValueType::BigInt => {
                return Err(Self::unsupported(value_type))
            }
        };
        Ok(holder)
    }

    // By type: coercing the value to a string throws for a Symbol.
    fn unsupported(value_type: napi::ValueType) -> CubeError {
        CubeError::internal(format!("Unsupported JsValue of type {value_type:?}"))
    }

    pub fn from_typed<T: Upcast<C>>(typed_holder: T) -> Self {
        T::upcast(typed_holder)
    }

    pub fn get_context(&self) -> ContextHolder<C> {
        match self {
            Self::Null(v) => v.get_context(),
            Self::Undefined(v) => v.get_context(),
            Self::Boolean(v) => v.get_context(),
            Self::Number(v) => v.get_context(),
            Self::String(v) => v.get_context(),
            Self::Array(v) => v.get_context(),
            Self::Function(v) => v.get_context(),
            Self::Struct(v) => v.get_context(),
            Self::RustBox(v) => v.get_context(),
        }
    }

    define_into_method!(into_null, Null, PrimitiveNeonTypeHolder<C, JsNull>, "Object is not the Null object");
    define_into_method!(into_undefined, Undefined, PrimitiveNeonTypeHolder<C, JsUndefined>, "Object is not the Undefined object");
    define_into_method!(into_boolean, Boolean, PrimitiveNeonTypeHolder<C, JsBoolean>, "Object is not the Boolean object");
    define_into_method!(into_number, Number, PrimitiveNeonTypeHolder<C, JsNumber>, "Object is not the Number object");
    define_into_method!(into_string, String, PrimitiveNeonTypeHolder<C, JsString>, "Object is not the String object");
    define_into_method!(into_array, Array, ObjectNeonTypeHolder<C, JsArray>, "Object is not the Array object");
    define_into_method!(into_function, Function, ObjectNeonTypeHolder<C, JsFunction>, "Object is not the Function object");
    define_into_method!(into_struct, Struct, ObjectNeonTypeHolder<C, JsObject>, "Object is not the Struct object");
    define_into_method!(into_rust_box, RustBox, ObjectNeonTypeHolder<C, JsBox<NativeRustHandle>>, "Object is not a Rust box");

    pub fn clone_to_context<CC: Context<'static> + 'static>(
        &self,
        context: &ContextHolder<CC>,
    ) -> RootHolder<CC> {
        match self {
            Self::Null(v) => RootHolder::Null(v.clone_to_context(context)),
            Self::Undefined(v) => RootHolder::Undefined(v.clone_to_context(context)),
            Self::Boolean(v) => RootHolder::Boolean(v.clone_to_context(context)),
            Self::Number(v) => RootHolder::Number(v.clone_to_context(context)),
            Self::String(v) => RootHolder::String(v.clone_to_context(context)),
            Self::Array(v) => RootHolder::Array(v.clone_to_context(context)),
            Self::Function(v) => RootHolder::Function(v.clone_to_context(context)),
            Self::Struct(v) => RootHolder::Struct(v.clone_to_context(context)),
            Self::RustBox(v) => RootHolder::RustBox(v.clone_to_context(context)),
        }
    }
}

impl<C: Context<'static> + 'static> Clone for RootHolder<C> {
    fn clone(&self) -> Self {
        match self {
            Self::Null(v) => Self::Null(v.clone()),
            Self::Undefined(v) => Self::Undefined(v.clone()),
            Self::Boolean(v) => Self::Boolean(v.clone()),
            Self::Number(v) => Self::Number(v.clone()),
            Self::String(v) => Self::String(v.clone()),
            Self::Array(v) => Self::Array(v.clone()),
            Self::Function(v) => Self::Function(v.clone()),
            Self::Struct(v) => Self::Struct(v.clone()),
            Self::RustBox(v) => Self::RustBox(v.clone()),
        }
    }
}
