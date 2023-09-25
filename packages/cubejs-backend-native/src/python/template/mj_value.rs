use crate::python::cross::{CLRepr, CLReprKind, CLReprObject};
use crate::python::runtime::py_runtime;
use crate::tokio_runtime;
use minijinja as mj;
use minijinja::value::{Object, ObjectKind, SeqObject, StructObject, Value, ValueKind};
use pyo3::types::{PyFunction, PyTuple};
use pyo3::{AsPyPointer, Py, PyAny, PyErr, PyResult, Python};
use std::convert::TryInto;
use std::sync::Arc;
use log::error;
use pyo3::exceptions::PyNotImplementedError;

struct JinjaPythonObject {
    pub(crate) inner: Py<PyAny>,
    fields: Vec<Arc<str>>
}

impl std::fmt::Debug for JinjaPythonObject {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Debug::fmt(&self.inner, f)
    }
}

impl std::fmt::Display for JinjaPythonObject {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(&self.inner, f)
    }
}

impl Object for JinjaPythonObject {
    fn kind(&self) -> ObjectKind<'_> {
        ObjectKind::Struct(self)
    }

    fn call_method(
        &self,
        _state: &mj::State,
        name: &str,
        args: &[Value],
    ) -> Result<Value, mj::Error> {
        let obj_ref = &self.inner;

        let mut arguments = Vec::with_capacity(args.len());

        for arg in args {
            arguments.push(from_minijinja_value(arg)?);
        }

        let python_call_res = Python::with_gil(move |py| -> PyResult<CLRepr> {
            let mut args_tuple = Vec::with_capacity(args.len());

            for arg in arguments {
                args_tuple.push(arg.into_py(py)?);
            }

            let tuple = PyTuple::new(py, args_tuple);

            let call_res = obj_ref.call_method1(py, name, tuple)?;

            let is_coroutine = unsafe { pyo3::ffi::PyCoro_CheckExact(call_res.as_ptr()) == 1 };
            if is_coroutine {
                Err(PyErr::new::<PyNotImplementedError, _>(
                    "Calling async methods are not supported",
                ))
            } else {
                CLRepr::from_python_ref(
                    call_res.as_ref(py),
                )
            }
        });

        match python_call_res {
            Ok(r) => Ok(to_minijinja_value(r)),
            Err(err) =>  Err(mj::Error::new(
                minijinja::ErrorKind::InvalidOperation,
                format!(
                    "Error while calling method: {}",
                    err
                )
            ))
        }
    }

    fn call(&self, _state: &mj::State, _args: &[Value]) -> Result<Value, mj::Error> {
        // TODO(ovr): Implement it
        Err(mj::Error::new(
            minijinja::ErrorKind::InvalidOperation,
            "Unable to call on Py<Object>",
        ))
    }
}

impl StructObject for JinjaPythonObject {
    fn get_field(&self, name: &str) -> Option<Value> {
        let obj_ref = &self.inner;

        let res = Python::with_gil(move |py| -> PyResult<CLRepr> {
            let attr_name = obj_ref.getattr(py, name)?;

            CLRepr::from_python_ref(attr_name.as_ref(py))
        });

        match res {
            Ok(r) => Some(to_minijinja_value(r)),
            Err(err) => {
                error!("Error while getting field '{}': {}", name, err);

                None
            }
        }
    }

    fn fields(&self) -> Vec<Arc<str>> {
        self.fields.clone()
    }

    fn field_count(&self) -> usize {
        self.fields.len()
    }
}

struct JinjaPythonFunction {
    pub(crate) inner: Py<PyFunction>,
}

impl std::fmt::Debug for JinjaPythonFunction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Debug::fmt(&self.inner, f)
    }
}

impl std::fmt::Display for JinjaPythonFunction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(&self.inner, f)
    }
}

impl Object for JinjaPythonFunction {
    fn kind(&self) -> ObjectKind<'_> {
        ObjectKind::Plain
    }

    fn call_method(
        &self,
        _state: &mj::State,
        _name: &str,
        _args: &[Value],
    ) -> Result<Value, mj::Error> {
        Err(mj::Error::new(
            minijinja::ErrorKind::InvalidOperation,
            "Unable to call method on Py<Function>",
        ))
    }

    fn call(&self, _state: &mj::State, args: &[Value]) -> Result<Value, mj::Error> {
        let mut arguments = Vec::with_capacity(args.len());

        for arg in args {
            arguments.push(from_minijinja_value(arg)?);
        }

        let py_runtime = py_runtime()
            .map_err(|err| mj::Error::new(mj::ErrorKind::EvalBlock, format!("Error: {}", err)))?;
        let call_future = py_runtime.call_async(self.inner.clone(), arguments);

        let tokio = tokio_runtime()
            .map_err(|err| mj::Error::new(mj::ErrorKind::EvalBlock, format!("Error: {}", err)))?;
        match tokio.block_on(call_future) {
            Ok(r) => Ok(to_minijinja_value(r)),
            Err(err) => Err(mj::Error::new(
                mj::ErrorKind::EvalBlock,
                format!("Call error: {}", err),
            )),
        }
    }
}

impl StructObject for JinjaPythonFunction {
    fn get_field(&self, _name: &str) -> Option<Value> {
        None
    }
}

struct JinjaDictObject {
    pub(crate) inner: CLReprObject,
}

impl std::fmt::Debug for JinjaDictObject {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Debug::fmt(&self.inner, f)
    }
}

impl std::fmt::Display for JinjaDictObject {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(&self.inner, f)
    }
}

impl StructObject for JinjaDictObject {
    fn get_field(&self, name: &str) -> Option<Value> {
        self.inner.get(name).map(|v| to_minijinja_value(v.clone()))
    }

    fn fields(&self) -> Vec<Arc<str>> {
        self.inner.keys().map(|x| x.to_string().into()).collect()
    }

    fn field_count(&self) -> usize {
        self.inner.iter().len()
    }
}

impl Object for JinjaDictObject {
    fn kind(&self) -> ObjectKind<'_> {
        ObjectKind::Struct(self)
    }

    fn call(&self, _state: &mj::State, _args: &[Value]) -> Result<Value, mj::Error> {
        Err(mj::Error::new(
            minijinja::ErrorKind::InvalidOperation,
            "insecure call",
        ))
    }

    fn call_method(
        &self,
        _state: &mj::State,
        _name: &str,
        _args: &[Value],
    ) -> Result<Value, mj::Error> {
        Err(mj::Error::new(
            minijinja::ErrorKind::InvalidOperation,
            "insecure method call",
        ))
    }
}

#[derive(Debug)]
struct JinjaSequenceObject {
    pub(crate) inner: Vec<CLRepr>,
}

impl std::fmt::Display for JinjaSequenceObject {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "[")?;

        for element in &self.inner {
            write!(f, "{}", element)?;
        }

        write!(f, "]")
    }
}

impl SeqObject for JinjaSequenceObject {
    fn get_item(&self, idx: usize) -> Option<Value> {
        self.inner.get(idx).map(|v| to_minijinja_value(v.clone()))
    }

    fn item_count(&self) -> usize {
        self.inner.len()
    }
}

impl Object for JinjaSequenceObject {
    fn kind(&self) -> ObjectKind<'_> {
        ObjectKind::Seq(self)
    }

    fn call(&self, _state: &mj::State, _args: &[Value]) -> Result<Value, mj::Error> {
        Err(mj::Error::new(
            minijinja::ErrorKind::InvalidOperation,
            "insecure call",
        ))
    }

    fn call_method(
        &self,
        _state: &mj::State,
        _name: &str,
        _args: &[Value],
    ) -> Result<Value, mj::Error> {
        Err(mj::Error::new(
            minijinja::ErrorKind::InvalidOperation,
            "insecure method call",
        ))
    }
}

pub fn from_minijinja_value(from: &mj::value::Value) -> Result<CLRepr, mj::Error> {
    match from.kind() {
        ValueKind::Undefined | ValueKind::None => Ok(CLRepr::Null),
        ValueKind::Bool => Ok(CLRepr::Bool(from.is_true())),
        ValueKind::Number => {
            if let Ok(rv) = TryInto::<i64>::try_into(from.clone()) {
                Ok(CLRepr::Int(rv))
            } else if let Ok(rv) = TryInto::<f64>::try_into(from.clone()) {
                Ok(CLRepr::Float(rv))
            } else {
                Err(mj::Error::new(
                    mj::ErrorKind::InvalidOperation,
                    format!("Converting from {:?} to Python is not supported", from),
                ))
            }
        }
        ValueKind::String => {
            if from.is_safe() {
                // TODO: Danger?
                Ok(CLRepr::String(from.as_str().unwrap().to_string()))
            } else {
                Ok(CLRepr::String(from.as_str().unwrap().to_string()))
            }
        }
        ValueKind::Seq => {
            let seq = if let Some(seq) = from.as_seq() {
                seq
            } else {
                return Err(mj::Error::new(
                    mj::ErrorKind::InvalidOperation,
                    format!("Unable to convert Seq to Python"),
                ));
            };

            let mut arr = Vec::with_capacity(seq.item_count());

            for idx in 0..seq.item_count() {
                let v = if let Some(value) = seq.get_item(idx) {
                    from_minijinja_value(&value)?
                } else {
                    CLRepr::Null
                };

                arr.push(v)
            }

            Ok(CLRepr::Array(arr))
        }
        ValueKind::Map => {
            let mut obj = CLReprObject::new();

            for key in from.try_iter()? {
                let value = if let Ok(v) = from.get_item(&key) {
                    from_minijinja_value(&v)?
                } else {
                    CLRepr::Null
                };

                let key_str = if let Some(key) = key.as_str() {
                    key.to_string()
                } else {
                    return Err(mj::Error::new(
                        mj::ErrorKind::InvalidOperation,
                        format!(
                            "Unable to convert Map to Python object: key must be string, actual: {}",
                            key.kind()
                        ),
                    ));
                };

                obj.insert(key_str, value);
            }

            Ok(CLRepr::Object(obj))
        }
        other => Err(mj::Error::new(
            mj::ErrorKind::InvalidOperation,
            format!("Converting from {:?} to Python is not supported", other),
        )),
    }
}

pub fn to_minijinja_value(from: CLRepr) -> Value {
    match from {
        CLRepr::Tuple(inner) | CLRepr::Array(inner) => {
            Value::from_seq_object(JinjaSequenceObject { inner })
        }
        CLRepr::Object(inner) => Value::from_object(JinjaDictObject { inner }),
        CLRepr::String(v) => Value::from(v),
        CLRepr::Float(v) => Value::from(v),
        CLRepr::Int(v) => Value::from(v),
        CLRepr::Bool(v) => Value::from(v),
        CLRepr::Null => Value::from(()),
        CLRepr::PyExternalFunction(inner) | CLRepr::PyFunction(inner) => {
            Value::from_object(JinjaPythonFunction { inner })
        }
        CLRepr::PyObject(inner) => {
            Value::from_object(JinjaPythonObject { inner, fields: vec![] })
        }
        CLRepr::JsFunction(_) => panic!(
            "Converting from {:?} to minijinja::Value is not supported",
            CLReprKind::JsFunction
        ),
    }
}
