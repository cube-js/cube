use neon::prelude::*;
use neon::result::Throw;
use neon::types::JsDate;
use pyo3::exceptions::PyTypeError;
use pyo3::types::{PyBool, PyDict, PyFloat, PyInt, PyList, PyString};
use pyo3::{PyAny, PyErr, PyObject, Python, ToPyObject};
use std::collections::HashMap;

#[derive(Debug)]
pub enum CLRepr {
    String(String),
    Bool(bool),
    Float(f64),
    Int(i64),
    Array(Vec<CLRepr>),
    Object(HashMap<String, CLRepr>),
    Null,
}

impl CLRepr {
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
            let mut obj = HashMap::new();

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
            let v = from.downcast_or_throw::<JsNumber, _>(cx)?;
            Ok(CLRepr::Float(v.value(cx)))
        } else if from.is_a::<JsNull, _>(cx) || from.is_a::<JsUndefined, _>(cx) {
            Ok(CLRepr::Null)
        } else if from.is_a::<JsPromise, _>(cx) {
            cx.throw_error("Unsupported conversion from JsPromise to Py")?
        } else if from.is_a::<JsFunction, _>(cx) {
            cx.throw_error("Unsupported conversion from JsFunction to Py")?
        } else if from.is_a::<JsDate, _>(cx) {
            cx.throw_error("Unsupported conversion from JsDate to Py")?
        } else {
            cx.throw_error(format!("Unsupported conversion from {:?} to Py", from))
        }
    }

    pub fn from_python_ref(v: &PyAny) -> Result<Self, PyErr> {
        if !v.is_none() {
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
                let mut obj = HashMap::new();

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
            } else {
                return Err(PyErr::new::<PyTypeError, _>(format!(
                    "Unable to represent {} type as CLR from Python",
                    v.get_type(),
                )));
            });
        }

        Ok(Self::Null)
    }

    fn into_js_impl<'a, C: Context<'a>>(from: CLRepr, cx: &mut C) -> JsResult<'a, JsValue> {
        Ok(match from {
            CLRepr::String(v) => cx.string(v).upcast(),
            CLRepr::Bool(v) => cx.boolean(v).upcast(),
            CLRepr::Float(v) => cx.number(v).upcast(),
            CLRepr::Int(v) => cx.number(v as f64).upcast(),
            CLRepr::Array(arr) => {
                let r = cx.empty_array();

                for (k, v) in arr.into_iter().enumerate() {
                    let vv = Self::into_js_impl(v, cx)?;
                    r.set(cx, k as u32, vv)?;
                }

                r.upcast()
            }
            CLRepr::Object(obj) => {
                let r = cx.empty_object();

                for (k, v) in obj.into_iter() {
                    let r_k = cx.string(k);
                    let r_v = Self::into_js_impl(v, cx)?;

                    r.set(cx, r_k, r_v)?;
                }

                r.upcast()
            }
            CLRepr::Null => cx.undefined().upcast(),
        })
    }

    pub fn into_js<'a, C: Context<'a>>(self, mut cx: C) -> JsResult<'a, JsValue> {
        Self::into_js_impl(self, &mut cx)
    }

    pub fn into_py_impl(from: CLRepr, py: Python) -> Result<PyObject, PyErr> {
        Ok(match from {
            CLRepr::String(v) => PyString::new(py, &v).to_object(py),
            CLRepr::Bool(v) => PyBool::new(py, v).to_object(py),
            CLRepr::Float(v) => PyFloat::new(py, v).to_object(py),
            CLRepr::Int(v) => {
                // TODO(ovr): FIX ME
                PyFloat::new(py, v as f64).to_object(py)
            }
            CLRepr::Array(arr) => {
                let mut elements = Vec::with_capacity(arr.len());

                for el in arr.into_iter() {
                    elements.push(Self::into_py_impl(el, py)?);
                }

                PyList::new(py, elements).to_object(py)
            }
            CLRepr::Object(obj) => {
                let r = PyDict::new(py);

                for (k, v) in obj.into_iter() {
                    r.set_item(k, Self::into_py_impl(v, py)?)?;
                }

                r.to_object(py)
            }
            CLRepr::Null => py.None(),
        })
    }

    pub fn into_py(self, py: Python) -> Result<PyObject, PyErr> {
        Self::into_py_impl(self, py)
    }
}
