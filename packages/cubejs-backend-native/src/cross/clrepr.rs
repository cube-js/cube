#[cfg(feature = "python")]
use crate::cross::clrepr_python::PythonRef;
#[cfg(feature = "python")]
use crate::cross::py_in_js::{
    cl_repr_py_function_wrapper, BoxedJsPyFunctionWrapper, JsPyFunctionWrapper,
};
#[cfg(feature = "python")]
use crate::utils::bind_method;
use neon::prelude::*;
use neon::result::Throw;
use neon::types::JsDate;
#[cfg(feature = "python")]
use std::cell::RefCell;
use std::collections::hash_map::{IntoIter, Iter, Keys};
use std::collections::HashMap;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub enum CLReprObjectKind {
    Object,
    KWargs,
}

#[derive(Clone)]
pub struct CLReprObject(pub(crate) HashMap<String, CLRepr>, CLReprObjectKind);

impl Default for CLReprObject {
    fn default() -> Self {
        Self::new(CLReprObjectKind::Object)
    }
}

impl CLReprObject {
    pub fn new(kind: CLReprObjectKind) -> Self {
        Self(HashMap::new(), kind)
    }

    pub fn get(&self, key: &str) -> Option<&CLRepr> {
        self.0.get(key)
    }

    pub fn insert(&mut self, key: String, value: CLRepr) -> Option<CLRepr> {
        self.0.insert(key, value)
    }

    pub fn iter(&self) -> Iter<String, CLRepr> {
        self.0.iter()
    }

    pub fn keys(&self) -> Keys<'_, String, CLRepr> {
        self.0.keys()
    }
}

impl IntoIterator for CLReprObject {
    type Item = (String, CLRepr);
    type IntoIter = IntoIter<String, CLRepr>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

impl std::fmt::Debug for CLReprObject {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Debug::fmt(&self.0, f)
    }
}

impl std::fmt::Display for CLReprObject {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Debug::fmt(&self.0, f)
    }
}

#[derive(Debug)]
pub enum CLReprKind {
    String,
    Bool,
    Float,
    Int,
    Tuple,
    Array,
    Object,
    JsFunction,
    #[cfg(feature = "python")]
    #[allow(unused)]
    PythonRef,
    Null,
}

#[derive(Debug, Clone)]
pub enum StringType {
    Normal,
    #[allow(unused)]
    Safe,
}

/// Cross language representation is abstraction to transfer values between
/// JavaScript and Python across Rust. Converting between two different languages requires
/// to use Context which is available on the call (one for python and one for js), which result as
/// blocking.
#[derive(Debug, Clone)]
pub enum CLRepr {
    String(String, StringType),
    Bool(bool),
    Float(f64),
    Int(i64),
    #[allow(dead_code)]
    Tuple(Vec<CLRepr>),
    Array(Vec<CLRepr>),
    Object(CLReprObject),
    JsFunction(Arc<Root<JsFunction>>),
    #[cfg(feature = "python")]
    PythonRef(PythonRef),
    Null,
}

impl CLRepr {
    pub fn is_kwarg(&self) -> bool {
        match self {
            CLRepr::Object(obj) => matches!(obj.1, CLReprObjectKind::KWargs),
            _ => false,
        }
    }
}

impl std::fmt::Display for CLRepr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(&self, f)
    }
}

#[cfg(feature = "python")]
struct IntoJsContext {
    parent_key_name: Option<String>,
}

impl CLRepr {
    pub fn is_null(&self) -> bool {
        matches!(self, CLRepr::Null)
    }

    pub fn downcast_to_object(self) -> CLReprObject {
        match self {
            CLRepr::Object(obj) => obj,
            _ => panic!("downcast_to_object rejected, actual: {:?}", self.kind()),
        }
    }

    #[allow(unused)]
    pub fn kind(&self) -> CLReprKind {
        match self {
            CLRepr::String(_, _) => CLReprKind::String,
            CLRepr::Bool(_) => CLReprKind::Bool,
            CLRepr::Float(_) => CLReprKind::Float,
            CLRepr::Int(_) => CLReprKind::Int,
            CLRepr::Tuple(_) => CLReprKind::Tuple,
            CLRepr::Array(_) => CLReprKind::Array,
            CLRepr::Object(_) => CLReprKind::Object,
            CLRepr::JsFunction(_) => CLReprKind::JsFunction,
            #[cfg(feature = "python")]
            CLRepr::PythonRef(_) => CLReprKind::PythonRef,
            CLRepr::Null => CLReprKind::Null,
        }
    }

    /// Convert javascript value to CLRepr
    pub fn from_js_ref<'a, C: Context<'a>>(
        from: Handle<'a, JsValue>,
        cx: &mut C,
    ) -> Result<Self, Throw> {
        if from.is_a::<JsString, _>(cx) {
            let v = from.downcast_or_throw::<JsString, _>(cx)?;
            Ok(CLRepr::String(v.value(cx), StringType::Normal))
        } else if from.is_a::<JsArray, _>(cx) {
            let v = from.downcast_or_throw::<JsArray, _>(cx)?;

            let mut r = Vec::with_capacity(v.len(cx) as usize);

            for i in 0..v.len(cx) {
                let el = v.get(cx, i)?;

                let circular_reference = from.strict_equals(cx, el);
                if circular_reference {
                    #[cfg(debug_assertions)]
                    log::warn!("Circular referenced array detected");

                    continue;
                }

                r.push(Self::from_js_ref(el, cx)?)
            }

            Ok(CLRepr::Array(r))
        } else if from.is_a::<JsObject, _>(cx) {
            let mut obj = CLReprObject::new(CLReprObjectKind::Object);

            let v = from.downcast_or_throw::<JsObject, _>(cx)?;
            let properties = v.get_own_property_names(cx)?;
            for i in 0..properties.len(cx) {
                let property: Handle<JsString> = properties.get(cx, i)?;
                let property_val = v.get_value(cx, property)?;

                let circular_reference = from.strict_equals(cx, property_val);
                if circular_reference {
                    #[cfg(debug_assertions)]
                    log::warn!("Circular referenced object detected");

                    continue;
                }

                obj.insert(property.value(cx), Self::from_js_ref(property_val, cx)?);
            }

            Ok(CLRepr::Object(obj))
        } else if from.is_a::<JsBoolean, _>(cx) {
            let v = from.downcast_or_throw::<JsBoolean, _>(cx)?;
            Ok(CLRepr::Bool(v.value(cx)))
        } else if from.is_a::<JsNumber, _>(cx) {
            let v = from.downcast_or_throw::<JsNumber, _>(cx)?.value(cx);

            if v == (v as i64) as f64 {
                Ok(CLRepr::Int(v as i64))
            } else {
                Ok(CLRepr::Float(v))
            }
        } else if from.is_a::<JsNull, _>(cx) || from.is_a::<JsUndefined, _>(cx) {
            Ok(CLRepr::Null)
        } else if from.is_a::<JsPromise, _>(cx) {
            cx.throw_error("Unsupported conversion from JsPromise to CLRepr")?
        } else if from.is_a::<JsDate, _>(cx) {
            cx.throw_error("Unsupported conversion from JsDate to CLRepr")?
        } else if from.is_a::<JsFunction, _>(cx) {
            let fun = from.downcast_or_throw::<JsFunction, _>(cx)?;
            let fun_root = fun.root(cx);

            Ok(CLRepr::JsFunction(Arc::new(fun_root)))
        } else {
            #[cfg(feature = "python")]
            if from.is_a::<BoxedJsPyFunctionWrapper, _>(cx) {
                let ref_wrap = from.downcast_or_throw::<BoxedJsPyFunctionWrapper, _>(cx)?;
                let fun = ref_wrap.borrow().get_fun().clone();

                return Ok(CLRepr::PythonRef(PythonRef::PyFunction(fun)));
            }

            cx.throw_error(format!("Unsupported conversion from {:?} to CLRepr", from))
        }
    }

    fn into_js_impl<'a, C: Context<'a>>(
        from: CLRepr,
        cx: &mut C,
        #[cfg(feature = "python")] tcx: IntoJsContext,
    ) -> JsResult<'a, JsValue> {
        Ok(match from {
            CLRepr::String(v, _) => cx.string(v).upcast(),
            CLRepr::Bool(v) => cx.boolean(v).upcast(),
            CLRepr::Float(v) => cx.number(v).upcast(),
            CLRepr::Int(v) => cx.number(v as f64).upcast(),
            CLRepr::Tuple(arr) | CLRepr::Array(arr) => {
                let r = cx.empty_array();

                for (k, v) in arr.into_iter().enumerate() {
                    let vv = Self::into_js_impl(
                        v,
                        cx,
                        #[cfg(feature = "python")]
                        IntoJsContext {
                            parent_key_name: None,
                        },
                    )?;
                    r.set(cx, k as u32, vv)?;
                }

                r.upcast()
            }
            CLRepr::Object(obj) => {
                let r = cx.empty_object();

                for (k, v) in obj.into_iter() {
                    let r_k = cx.string(k.clone());
                    let r_v = Self::into_js_impl(
                        v,
                        cx,
                        #[cfg(feature = "python")]
                        IntoJsContext {
                            parent_key_name: Some(k),
                        },
                    )?;

                    r.set(cx, r_k, r_v)?;
                }

                r.upcast()
            }
            #[cfg(feature = "python")]
            CLRepr::PythonRef(py_ref) => match py_ref {
                PythonRef::PyFunction(py_fn) => {
                    let wrapper = JsPyFunctionWrapper::new(py_fn, tcx.parent_key_name);
                    let obj_this = cx.boxed(RefCell::new(wrapper)).upcast::<JsValue>();

                    let cl_repr_fn = JsFunction::new(cx, cl_repr_py_function_wrapper)?;
                    let binded_fun = bind_method(cx, cl_repr_fn, obj_this)?;

                    binded_fun.upcast()
                }
                PythonRef::PyExternalFunction(py_fn) => {
                    let wrapper = JsPyFunctionWrapper::new(py_fn, tcx.parent_key_name);
                    let external_obj = cx.boxed(RefCell::new(wrapper)).upcast::<JsValue>();

                    external_obj.upcast()
                }
                PythonRef::PyObject(_) => {
                    return cx.throw_error("Unable to represent PyObject in JS")
                }
            },
            CLRepr::Null => cx.undefined().upcast(),
            CLRepr::JsFunction(fun) => {
                let unwrapper_fun =
                    Arc::try_unwrap(fun).expect("Unable to unwrap Arc on Root<JsFunction>");

                unwrapper_fun.into_inner(cx).upcast()
            }
        })
    }

    pub fn into_js<'a, C: Context<'a>>(self, cx: &mut C) -> JsResult<'a, JsValue> {
        Self::into_js_impl(
            self,
            cx,
            #[cfg(feature = "python")]
            IntoJsContext {
                parent_key_name: None,
            },
        )
    }
}
