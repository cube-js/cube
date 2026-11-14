use crate::cross::clrepr::CLReprObject;
use crate::cross::{CLRepr, CLReprObjectKind, StringType};
use crate::python::utils::PyAnyHelpers;
use pyo3::exceptions::{PyException, PyNotImplementedError, PyTypeError};
use pyo3::types::{
    PyAnyMethods, PyBool, PyBoolMethods, PyComplex, PyDate, PyDateTime, PyDelta, PyDict,
    PyDictMethods, PyFloat, PyFloatMethods, PyFrame, PyFrozenSet, PyFunction, PyInt, PyList,
    PyListMethods, PySet, PySetMethods, PyString, PyTraceback, PyTuple, PyTupleMethods,
    PyTypeMethods,
};
use pyo3::{Bound, Py, PyAny, PyErr, Python};

#[derive(Debug)]
pub enum PythonRef {
    PyObject(Py<PyAny>),
    PyFunction(Py<PyFunction>),
    /// Special type to transfer functions through JavaScript
    /// In JS it's an external object.
    PyExternalFunction(Py<PyFunction>),
}

impl Clone for PythonRef {
    fn clone(&self) -> Self {
        Python::attach(|py| match self {
            PythonRef::PyObject(obj) => PythonRef::PyObject(obj.clone_ref(py)),
            PythonRef::PyFunction(fun) => PythonRef::PyFunction(fun.clone_ref(py)),
            PythonRef::PyExternalFunction(fun) => PythonRef::PyExternalFunction(fun.clone_ref(py)),
        })
    }
}

impl CLRepr {
    /// Convert python value to CLRepr
    pub fn from_python_ref(v: &Bound<'_, PyAny>) -> Result<Self, PyErr> {
        if v.is_none() {
            return Ok(Self::Null);
        }

        Ok(if v.get_type().is_subclass_of::<PyString>()? {
            let string_type = if v.hasattr("is_safe")? {
                StringType::Safe
            } else {
                StringType::Normal
            };

            Self::String(v.str()?.to_string(), string_type)
        } else if v.get_type().is_subclass_of::<PyBool>()? {
            Self::Bool(v.cast::<PyBool>()?.is_true())
        } else if v.get_type().is_subclass_of::<PyFloat>()? {
            let f = v.cast::<PyFloat>()?;
            Self::Float(f.value())
        } else if v.get_type().is_subclass_of::<PyInt>()? {
            let i: i64 = v.cast::<PyInt>()?.extract()?;
            Self::Int(i)
        } else if v.get_type().is_subclass_of::<PyDict>()? {
            let d = v.cast::<PyDict>()?;
            let mut obj = CLReprObject::new(CLReprObjectKind::Object);

            for (k, v) in d.iter() {
                if k.get_type().is_subclass_of::<PyString>()? {
                    let key_str = k.cast::<PyString>()?;

                    obj.insert(key_str.to_string(), Self::from_python_ref(&v)?);
                }
            }

            Self::Object(obj)
        } else if v.get_type().is_subclass_of::<PyList>()? {
            let l = v.cast::<PyList>()?;
            let mut r = Vec::with_capacity(l.len());

            for v in l.iter() {
                r.push(Self::from_python_ref(&v)?);
            }

            Self::Array(r)
        } else if v.get_type().is_subclass_of::<PySet>()? {
            let l = v.cast::<PySet>()?;
            let mut r = Vec::with_capacity(l.len());

            for v in l.iter() {
                r.push(Self::from_python_ref(&v)?);
            }

            Self::Array(r)
        } else if v.get_type().is_subclass_of::<PyTuple>()? {
            let l = v.cast::<PyTuple>()?;
            let mut r = Vec::with_capacity(l.len());

            for v in l.iter() {
                r.push(Self::from_python_ref(&v)?);
            }

            Self::Tuple(r)
        } else if v.get_type().is_subclass_of::<PyFunction>()? {
            let fun: Py<PyFunction> = v.cast::<PyFunction>()?.clone().unbind();

            Self::PythonRef(PythonRef::PyFunction(fun))
        } else if v.get_type().is_subclass_of::<PyComplex>()? {
            return Err(PyErr::new::<PyTypeError, _>(
                "Unable to represent PyComplex type as CLR from Python".to_string(),
            ));
        } else if v.get_type().is_subclass_of::<PyDateTime>()? {
            return Err(PyErr::new::<PyTypeError, _>(
                "Unable to represent PyDateTime type as CLR from Python".to_string(),
            ));
        } else if v.get_type().is_subclass_of::<PyDate>()? {
            return Err(PyErr::new::<PyTypeError, _>(
                "Unable to represent PyDate type as CLR from Python".to_string(),
            ));
        } else if v.get_type().is_subclass_of::<PyFrozenSet>()? {
            let set = v.cast::<PyFrozenSet>()?;

            return Err(PyErr::new::<PyTypeError, _>(format!(
                "Unable to represent PyFrozenSet type as CLR from Python, value: {:?}",
                set
            )));
        } else if v.get_type().is_subclass_of::<PyException>()? {
            let exception = v.cast::<PyException>()?;

            return Err(PyErr::new::<PyTypeError, _>(format!(
                "Unable to represent PyException type as CLR from Python, value: {:?}",
                exception
            )));
        } else if v.get_type().is_subclass_of::<PyFrame>()? {
            let frame = v.cast::<PyFrame>()?;

            return Err(PyErr::new::<PyTypeError, _>(format!(
                "Unable to represent PyFrame type as CLR from Python, value: {:?}",
                frame
            )));
        } else if v.get_type().is_subclass_of::<PyTraceback>()? {
            let trb = v.cast::<PyTraceback>()?;

            return Err(PyErr::new::<PyTypeError, _>(format!(
                "Unable to represent PyTraceback type as CLR from Python, value: {:?}",
                trb
            )));
        } else if v.get_type().is_subclass_of::<PyDelta>()? {
            let delta = v.cast::<PyDelta>()?;

            return Err(PyErr::new::<PyTypeError, _>(format!(
                "Unable to represent PyDelta type as CLR from Python, value: {:?}",
                delta
            )));
        } else if v.is_sequence() {
            return Err(PyErr::new::<PyTypeError, _>(format!(
                "Unable to represent PySequence type as CLR from Python, value: {:?}",
                v
            )));
        } else {
            // Fallback to PyObject, it will lead to throw error in the JS side
            Self::PythonRef(PythonRef::PyObject(v.clone().unbind()))
        })
    }

    fn into_py_dict_impl(obj: CLReprObject, py: Python<'_>) -> Result<Bound<'_, PyDict>, PyErr> {
        let r = PyDict::new(py);

        for (k, v) in obj.into_iter() {
            r.set_item(k, Self::into_py_impl(v, py)?)?;
        }

        Ok(r)
    }

    fn into_py_impl(from: CLRepr, py: Python) -> Result<Py<PyAny>, PyErr> {
        Ok(match from {
            CLRepr::String(v, _) => PyString::new(py, &v).into_any().unbind(),
            CLRepr::Bool(v) => PyBool::new(py, v).to_owned().into_any().unbind(),
            CLRepr::Float(v) => PyFloat::new(py, v).into_any().unbind(),
            CLRepr::Int(v) => PyInt::new(py, v).into_any().unbind(),
            CLRepr::Array(arr) => {
                let mut elements = Vec::with_capacity(arr.len());

                for el in arr.into_iter() {
                    elements.push(Self::into_py_impl(el, py)?);
                }

                PyList::new(py, elements)?.into_any().unbind()
            }
            CLRepr::Tuple(arr) => {
                let mut elements = Vec::with_capacity(arr.len());

                for el in arr.into_iter() {
                    elements.push(Self::into_py_impl(el, py)?);
                }

                PyTuple::new(py, elements)?.into_any().unbind()
            }
            CLRepr::Object(obj) => {
                let r = Self::into_py_dict_impl(obj, py)?;

                r.into_any().unbind()
            }
            CLRepr::Null => py.None(),
            CLRepr::PythonRef(py_ref) => match py_ref {
                PythonRef::PyObject(_) => {
                    return Err(PyErr::new::<PyNotImplementedError, _>(
                        "Unable to represent PyObject in Python",
                    ))
                }
                PythonRef::PyFunction(_) => {
                    return Err(PyErr::new::<PyNotImplementedError, _>(
                        "Unable to represent PyFunction in Python",
                    ))
                }
                PythonRef::PyExternalFunction(_) => {
                    return Err(PyErr::new::<PyNotImplementedError, _>(
                        "Unable to represent PyExternalFunction in Python",
                    ))
                }
            },
            CLRepr::JsFunction(_) => {
                return Err(PyErr::new::<PyNotImplementedError, _>(
                    "Unable to represent JsFunction in Python",
                ))
            }
        })
    }

    pub fn into_py_dict(self, py: Python<'_>) -> Result<Bound<'_, PyDict>, PyErr> {
        Ok(match self {
            CLRepr::Object(obj) => Self::into_py_dict_impl(obj, py)?,
            other => {
                return Err(PyErr::new::<PyNotImplementedError, _>(format!(
                    "Unable to convert {:?} into PyDict",
                    other.kind()
                )))
            }
        })
    }

    pub fn into_py(self, py: Python) -> Result<Py<PyAny>, PyErr> {
        Self::into_py_impl(self, py)
    }
}
