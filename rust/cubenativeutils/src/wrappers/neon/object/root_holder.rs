use super::{ObjectNeonTypeHolder, PrimitiveNeonTypeHolder};
use crate::wrappers::neon::context::ContextHolder;
use crate::CubeError;
use neon::prelude::*;
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

macro_rules! match_js_value_type {
    ($context:expr, $value:expr, $cx:expr, {
       $($variant:ident => $js_type:ty => $holder_type:ident),+ $(,)?
    }) => {
        $(
            if $value.is_a::<$js_type, _>($cx) {
                let downcasted = $value
                    .downcast::<$js_type, _>($cx)
                    .map_err(|_| CubeError::internal("Downcast error".to_string()))?;
                return Ok(RootHolder::$variant($holder_type::new(
                    $context.clone(),
                    downcasted,
                    $cx
                )));
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
}

impl<C: Context<'static> + 'static> RootHolder<C> {
    pub fn new<V: Value>(
        context: ContextHolder<C>,
        value: Handle<'static, V>,
    ) -> Result<Self, CubeError> {
        context.with_context(|cx| {
            match_js_value_type!(context, value, cx, {
                Null => JsNull => PrimitiveNeonTypeHolder,
                Undefined => JsUndefined => PrimitiveNeonTypeHolder,
                Boolean => JsBoolean => PrimitiveNeonTypeHolder,
                Number => JsNumber => PrimitiveNeonTypeHolder,
                String => JsString => PrimitiveNeonTypeHolder,
                Array => JsArray => ObjectNeonTypeHolder,
                Function => JsFunction => ObjectNeonTypeHolder,
                Struct => JsObject => ObjectNeonTypeHolder,
            });

            Err(CubeError::internal(format!(
                "Unsupported JsValue: {}",
                value.to_string(cx)?.value(cx)
            )))
        })?
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
        }
    }
}
