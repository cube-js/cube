use crate::python::runtime::py_runtime;
use crate::utils::bind_method;
use neon::prelude::*;
use neon::result::Throw;
use neon::types::JsDate;
use pyo3::exceptions::{PyNotImplementedError, PyTypeError};
use pyo3::types::{PyBool, PyComplex, PyDate, PyDict, PyFloat, PyFrame, PyFunction, PyInt, PyList, PySequence, PySet, PyString, PyTraceback, PyTuple};
use pyo3::{AsPyPointer, IntoPy, Py, PyAny, PyErr, PyObject, Python, ToPyObject};
use std::cell::RefCell;
use std::collections::hash_map::{IntoIter, Iter, Keys};
use std::collections::HashMap;
use std::sync::Arc;

#[derive(Clone)]
pub struct CLReprObject(pub(crate) HashMap<String, CLRepr>);

impl CLReprObject {
    pub fn new() -> Self {
        Self(HashMap::new())
    }

    pub fn get(&self, key: &str) -> Option<&CLRepr> {
        self.0.get(key)
    }

    pub fn insert(&mut self, key: String, value: CLRepr) -> Option<CLRepr> {
        self.0.insert(key, value)
    }

    pub fn into_iter(self) -> IntoIter<String, CLRepr> {
        self.0.into_iter()
    }

    pub fn iter(&self) -> Iter<String, CLRepr> {
        self.0.iter()
    }

    pub fn keys(&self) -> Keys<'_, String, CLRepr> {
        self.0.keys()
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

#[allow(unused)]
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
    PyObject,
    PyFunction,
    PyExternalFunction,
    Null,
}

/// Cross language representation is abstraction to transfer values between
/// JavaScript and Python across Rust. Converting between two different languages requires
/// to use Context which is available on the call (one for python and one for js), which result as
/// blocking.
#[derive(Debug, Clone)]
pub enum CLRepr {
    String(String),
    Bool(bool),
    Float(f64),
    Int(i64),
    Tuple(Vec<CLRepr>),
    Array(Vec<CLRepr>),
    Object(CLReprObject),
    JsFunction(Arc<Root<JsFunction>>),
    PyObject(Py<PyAny>),
    PyFunction(Py<PyFunction>),
    /// Special type to transfer functions through JavaScript
    /// In JS it's an external object. It's not the same as Function.
    PyExternalFunction(Py<PyFunction>),
    Null,
}

impl std::fmt::Display for CLRepr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(&self, f)
    }
}

pub struct JsPyFunctionWrapper {
    fun: Py<PyFunction>,
    _fun_name: Option<String>,
}

impl Finalize for JsPyFunctionWrapper {}

pub type BoxedJsPyFunctionWrapper = JsBox<RefCell<JsPyFunctionWrapper>>;

fn cl_repr_py_function_wrapper(mut cx: FunctionContext) -> JsResult<JsPromise> {
    #[cfg(build = "debug")]
    trace!("cl_repr_py_function_wrapper {}", _fun_name);

    let (deferred, promise) = cx.promise();

    let this = cx
        .this()
        .downcast_or_throw::<BoxedJsPyFunctionWrapper, _>(&mut cx)?;

    let mut arguments = Vec::with_capacity(cx.len() as usize);

    for arg_idx in 0..cx.len() {
        arguments.push(CLRepr::from_js_ref(
            cx.argument::<JsValue>(arg_idx)?,
            &mut cx,
        )?);
    }

    let py_method = this.borrow().fun.clone();
    let py_runtime = match py_runtime() {
        Ok(r) => r,
        Err(err) => return cx.throw_error(format!("Unable to init python runtime: {:?}", err)),
    };
    py_runtime.call_async_with_promise_callback(py_method, arguments, deferred);

    Ok(promise)
}

struct IntoJsContext {
    parent_key_name: Option<String>,
}

impl CLRepr {
    pub fn is_null(&self) -> bool {
        match self {
            CLRepr::Null => true,
            _ => false,
        }
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
            CLRepr::String(_) => CLReprKind::String,
            CLRepr::Bool(_) => CLReprKind::Bool,
            CLRepr::Float(_) => CLReprKind::Float,
            CLRepr::Int(_) => CLReprKind::Int,
            CLRepr::Tuple(_) => CLReprKind::Tuple,
            CLRepr::Array(_) => CLReprKind::Array,
            CLRepr::Object(_) => CLReprKind::Object,
            CLRepr::JsFunction(_) => CLReprKind::JsFunction,
            CLRepr::PyObject(_) => CLReprKind::PyObject,
            CLRepr::PyFunction(_) => CLReprKind::PyFunction,
            CLRepr::PyExternalFunction(_) => CLReprKind::PyExternalFunction,
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
            Ok(CLRepr::String(v.value(cx)))
        } else if from.is_a::<JsArray, _>(cx) {
            let v = from.downcast_or_throw::<JsArray, _>(cx)?;
            let el = v.to_vec(cx)?;

            let mut r = Vec::with_capacity(el.len());

            for el in el {
                r.push(Self::from_js_ref(el, cx)?)
            }

            Ok(CLRepr::Array(r))
        } else if from.is_a::<JsObject, _>(cx) {
            let mut obj = CLReprObject::new();

            let v = from.downcast_or_throw::<JsObject, _>(cx)?;
            let properties = v.get_own_property_names(cx)?;
            for i in 0..properties.len(cx) {
                let property: Handle<JsString> = properties.get(cx, i)?;
                let property_val = v.get_value(cx, property)?;

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
        } else if from.is_a::<BoxedJsPyFunctionWrapper, _>(cx) {
            let ref_wrap = from.downcast_or_throw::<BoxedJsPyFunctionWrapper, _>(cx)?;
            let fun = ref_wrap.borrow().fun.clone();

            Ok(CLRepr::PyFunction(fun))
        } else if from.is_a::<JsFunction, _>(cx) {
            let fun = from.downcast_or_throw::<JsFunction, _>(cx)?;
            let fun_root = fun.root(cx);

            Ok(CLRepr::JsFunction(Arc::new(fun_root)))
        } else {
            cx.throw_error(format!("Unsupported conversion from {:?} to CLRepr", from))
        }
    }

    /// Convert python value to CLRepr
    pub fn from_python_ref(v: &PyAny) -> Result<Self, PyErr> {
        if v.is_none() {
            return Ok(Self::Null);
        }

        return Ok(if v.get_type().is_subclass_of::<PyString>()? {
            Self::String(v.to_string())
        } else if v.get_type().is_subclass_of::<PyBool>()? {
            Self::Bool(v.downcast::<PyBool>()?.is_true())
        } else if v.get_type().is_subclass_of::<PyFloat>()? {
            let f = v.downcast::<PyFloat>()?;
            Self::Float(f.value())
        } else if v.get_type().is_subclass_of::<PyInt>()? {
            let i: i64 = v.downcast::<PyInt>()?.extract()?;
            Self::Int(i)
        } else if v.get_type().is_subclass_of::<PyDict>()? {
            let d = v.downcast::<PyDict>()?;
            let mut obj = CLReprObject::new();

            for (k, v) in d.iter() {
                if k.get_type().is_subclass_of::<PyString>()? {
                    let key_str = k.downcast::<PyString>()?;

                    obj.insert(key_str.to_string(), Self::from_python_ref(v)?);
                }
            }

            Self::Object(obj)
        } else if v.get_type().is_subclass_of::<PyList>()? {
            let l = v.downcast::<PyList>()?;
            let mut r = Vec::with_capacity(l.len());

            for v in l.iter() {
                r.push(Self::from_python_ref(v)?);
            }

            Self::Array(r)
        } else if v.get_type().is_subclass_of::<PySet>()? {
            let l = v.downcast::<PySet>()?;
            let mut r = Vec::with_capacity(l.len());

            for v in l.iter() {
                r.push(Self::from_python_ref(v)?);
            }

            Self::Array(r)
        } else if v.get_type().is_subclass_of::<PyTuple>()? {
            let l = v.downcast::<PyTuple>()?;
            let mut r = Vec::with_capacity(l.len());

            for v in l.iter() {
                r.push(Self::from_python_ref(v)?);
            }

            Self::Tuple(r)
        } else if v.get_type().is_subclass_of::<PyFunction>()? {
            let fun: Py<PyFunction> = v.downcast::<PyFunction>()?.into();

            Self::PyFunction(fun)
        } else if v.get_type().is_subclass_of::<PyComplex>()? {
            return Err(PyErr::new::<PyTypeError, _>(format!(
                "Unable to represent PyComplex type as CLR from Python"
            )));
        } else if v.get_type().is_subclass_of::<PyDate>()? {
            return Err(PyErr::new::<PyTypeError, _>(format!(
                "Unable to represent PyDate type as CLR from Python"
            )));
        } else if v.get_type().is_subclass_of::<PyFrame>()? {
            return Err(PyErr::new::<PyTypeError, _>(format!(
                "Unable to represent PyFrame type as CLR from Python"
            )));
        } else if v.get_type().is_subclass_of::<PyTraceback>()? {
            return Err(PyErr::new::<PyTypeError, _>(format!(
                "Unable to represent PyTraceback type as CLR from Python"
            )));
        } else {
            let is_sequence = unsafe { pyo3::ffi::PySequence_Check(v.as_ptr()) == 1 };
            if is_sequence {
                return Err(PyErr::new::<PyTypeError, _>(format!(
                    "Unable to represent PyTraceback type as CLR from Python"
                )));
            }

            Self::PyObject(v.into())
        });
    }

    fn into_js_impl<'a, C: Context<'a>>(
        from: CLRepr,
        cx: &mut C,
        tcx: IntoJsContext,
    ) -> JsResult<'a, JsValue> {
        Ok(match from {
            CLRepr::String(v) => cx.string(v).upcast(),
            CLRepr::Bool(v) => cx.boolean(v).upcast(),
            CLRepr::Float(v) => cx.number(v).upcast(),
            CLRepr::Int(v) => cx.number(v as f64).upcast(),
            CLRepr::Tuple(arr) | CLRepr::Array(arr) => {
                let r = cx.empty_array();

                for (k, v) in arr.into_iter().enumerate() {
                    let vv = Self::into_js_impl(
                        v,
                        cx,
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
                        IntoJsContext {
                            parent_key_name: Some(k),
                        },
                    )?;

                    r.set(cx, r_k, r_v)?;
                }

                r.upcast()
            }
            CLRepr::PyFunction(py_fn) => {
                let wrapper = JsPyFunctionWrapper {
                    fun: py_fn,
                    _fun_name: tcx.parent_key_name,
                };
                let obj_this = cx.boxed(RefCell::new(wrapper)).upcast::<JsValue>();

                let cl_repr_fn = JsFunction::new(cx, cl_repr_py_function_wrapper)?;
                let binded_fun = bind_method(cx, cl_repr_fn, obj_this)?;

                binded_fun.upcast()
            }
            CLRepr::PyExternalFunction(py_fn) => {
                let wrapper = JsPyFunctionWrapper {
                    fun: py_fn,
                    _fun_name: tcx.parent_key_name,
                };
                let external_obj = cx.boxed(RefCell::new(wrapper)).upcast::<JsValue>();

                external_obj.upcast()
            }
            CLRepr::Null => cx.undefined().upcast(),
            CLRepr::JsFunction(fun) => {
                let unwrapper_fun =
                    Arc::try_unwrap(fun).expect("Unable to unwrap Arc on Root<JsFunction>");

                unwrapper_fun.into_inner(cx).upcast()
            }
            CLRepr::PyObject(_) => {
                return cx.throw_error(format!("Unable to represent PyObject in JS"))
            }
        })
    }

    pub fn into_js<'a, C: Context<'a>>(self, cx: &mut C) -> JsResult<'a, JsValue> {
        Self::into_js_impl(
            self,
            cx,
            IntoJsContext {
                parent_key_name: None,
            },
        )
    }

    pub fn into_py_impl(from: CLRepr, py: Python) -> Result<PyObject, PyErr> {
        Ok(match from {
            CLRepr::String(v) => PyString::new(py, &v).to_object(py),
            CLRepr::Bool(v) => PyBool::new(py, v).to_object(py),
            CLRepr::Float(v) => PyFloat::new(py, v).to_object(py),
            CLRepr::Int(v) => {
                let py_int: &PyInt = unsafe { py.from_owned_ptr(pyo3::ffi::PyLong_FromLong(v)) };

                py_int.to_object(py)
            }
            CLRepr::Array(arr) => {
                let mut elements = Vec::with_capacity(arr.len());

                for el in arr.into_iter() {
                    elements.push(Self::into_py_impl(el, py)?);
                }

                PyList::new(py, elements).to_object(py)
            }
            CLRepr::Tuple(arr) => {
                let mut elements = Vec::with_capacity(arr.len());

                for el in arr.into_iter() {
                    elements.push(Self::into_py_impl(el, py)?);
                }

                PyTuple::new(py, elements).to_object(py)
            }
            CLRepr::Object(obj) => {
                let r = PyDict::new(py);

                for (k, v) in obj.into_iter() {
                    r.set_item(k, Self::into_py_impl(v, py)?)?;
                }

                r.to_object(py)
            }
            CLRepr::Null => py.None(),
            CLRepr::PyExternalFunction(_) => {
                return Err(PyErr::new::<PyNotImplementedError, _>(
                    "Unable to represent PyExternalFunction in Python",
                ))
            }
            CLRepr::JsFunction(_) => {
                return Err(PyErr::new::<PyNotImplementedError, _>(
                    "Unable to represent JsFunction in Python",
                ))
            }
            CLRepr::PyFunction(_) => {
                return Err(PyErr::new::<PyNotImplementedError, _>(
                    "Unable to represent PyFunction in Python",
                ))
            }
            CLRepr::PyObject(_) => {
                return Err(PyErr::new::<PyNotImplementedError, _>(
                    "Unable to represent PyObject in Python",
                ))
            }
        })
    }

    pub fn into_py(self, py: Python) -> Result<PyObject, PyErr> {
        Self::into_py_impl(self, py)
    }
}
