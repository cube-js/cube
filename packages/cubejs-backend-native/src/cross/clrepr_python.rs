use crate::cross::clrepr::CLReprObject;
use crate::cross::{CLRepr, CLReprObjectKind, StringType};
use pyo3::exceptions::{PyNotImplementedError, PyTypeError};
use pyo3::types::{
    PyBool, PyComplex, PyDate, PyDict, PyFloat, PyFrame, PyFunction, PyInt, PyList, PySequence,
    PySet, PyString, PyTraceback, PyTuple,
};
use pyo3::{Py, PyAny, PyErr, PyObject, Python, ToPyObject};

#[derive(Debug, Clone)]
pub enum PythonRef {
    PyObject(PyObject),
    PyFunction(Py<PyFunction>),
    /// Special type to transfer functions through JavaScript
    /// In JS it's an external object. It's not the same as Function.
    PyExternalFunction(Py<PyFunction>),
}

impl CLRepr {
    /// Convert python value to CLRepr
    pub fn from_python_ref(v: &PyAny) -> Result<Self, PyErr> {
        if v.is_none() {
            return Ok(Self::Null);
        }

        Ok(if v.get_type().is_subclass_of::<PyString>()? {
            let string_type = if v.hasattr("is_safe")? {
                StringType::Safe
            } else {
                StringType::Normal
            };

            Self::String(v.to_string(), string_type)
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
            let mut obj = CLReprObject::new(CLReprObjectKind::Object);

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

            Self::PythonRef(PythonRef::PyFunction(fun))
        } else if v.get_type().is_subclass_of::<PyComplex>()? {
            return Err(PyErr::new::<PyTypeError, _>(
                "Unable to represent PyComplex type as CLR from Python".to_string(),
            ));
        } else if v.get_type().is_subclass_of::<PyDate>()? {
            return Err(PyErr::new::<PyTypeError, _>(
                "Unable to represent PyDate type as CLR from Python".to_string(),
            ));
        } else if v.get_type().is_subclass_of::<PyFrame>()? {
            let frame = v.downcast::<PyFrame>()?;

            return Err(PyErr::new::<PyTypeError, _>(format!(
                "Unable to represent PyFrame type as CLR from Python, value: {:?}",
                frame
            )));
        } else if v.get_type().is_subclass_of::<PyTraceback>()? {
            let trb = v.downcast::<PyTraceback>()?;

            return Err(PyErr::new::<PyTypeError, _>(format!(
                "Unable to represent PyTraceback type as CLR from Python, value: {:?}",
                trb
            )));
        } else {
            let is_sequence = unsafe { pyo3::ffi::PySequence_Check(v.as_ptr()) == 1 };
            if is_sequence {
                let seq = v.downcast::<PySequence>()?;

                return Err(PyErr::new::<PyTypeError, _>(format!(
                    "Unable to represent PySequence type as CLR from Python, value: {:?}",
                    seq
                )));
            }

            Self::PythonRef(PythonRef::PyObject(v.into()))
        })
    }

    fn into_py_dict_impl(obj: CLReprObject, py: Python) -> Result<&PyDict, PyErr> {
        let r = PyDict::new(py);

        for (k, v) in obj.into_iter() {
            r.set_item(k, Self::into_py_impl(v, py)?)?;
        }

        Ok(r)
    }

    fn into_py_impl(from: CLRepr, py: Python) -> Result<PyObject, PyErr> {
        Ok(match from {
            CLRepr::String(v, _) => PyString::new(py, &v).to_object(py),
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
                let r = Self::into_py_dict_impl(obj, py)?;

                r.to_object(py)
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

    pub fn into_py_dict(self, py: Python) -> Result<&PyDict, PyErr> {
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

    pub fn into_py(self, py: Python) -> Result<PyObject, PyErr> {
        Self::into_py_impl(self, py)
    }
}
