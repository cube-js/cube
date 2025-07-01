use crate::cross::*;
use crate::python::neon_py::format_python_error;
use crate::python::runtime::py_runtime;
use crate::python::{python_obj_call_sync, python_obj_method_call_sync};
use crate::template::mj_value::to_minijinja_value;
use crate::tokio_runtime;
use log::error;
use minijinja as mj;
use minijinja::value as mjv;
use minijinja::value::{Object, ObjectKind, StructObject, Value};
use pyo3::types::{PyDict, PyFunction};
use pyo3::{Py, PyObject, PyResult, Python};
use std::convert::TryInto;
use std::sync::Arc;

pub fn from_minijinja_value(from: &mjv::Value) -> Result<CLRepr, mj::Error> {
    match from.kind() {
        mjv::ValueKind::Undefined | mjv::ValueKind::None => Ok(CLRepr::Null),
        mjv::ValueKind::Bool => Ok(CLRepr::Bool(from.is_true())),
        mjv::ValueKind::Number => {
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
        mjv::ValueKind::String => Ok(CLRepr::String(
            from.as_str()
                .expect("ValueKind::String must return string from as_str()")
                .to_string(),
            if from.is_safe() {
                StringType::Safe
            } else {
                StringType::Normal
            },
        )),
        mjv::ValueKind::Seq => {
            let seq = if let Some(seq) = from.as_seq() {
                seq
            } else {
                return Err(mj::Error::new(
                    mj::ErrorKind::InvalidOperation,
                    "Unable to convert Seq to Python".to_string(),
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
        mjv::ValueKind::Map => {
            let mut obj = CLReprObject::new(if from.is_kwargs() {
                CLReprObjectKind::KWargs
            } else {
                CLReprObjectKind::Object
            });

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

pub struct JinjaPythonObject {
    pub(crate) inner: PyObject,
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

        match python_obj_method_call_sync(obj_ref, name, arguments) {
            Ok(r) => Ok(to_minijinja_value(r)),
            Err(err) => Err(mj::Error::new(
                minijinja::ErrorKind::InvalidOperation,
                format!("Error while calling method: {}", format_python_error(err)),
            )),
        }
    }

    fn call(&self, _state: &mj::State, args: &[Value]) -> Result<Value, mj::Error> {
        let obj_ref = &self.inner;

        let mut arguments = Vec::with_capacity(args.len());

        for arg in args {
            arguments.push(from_minijinja_value(arg)?);
        }

        match python_obj_call_sync(obj_ref, arguments) {
            Ok(r) => Ok(to_minijinja_value(r)),
            Err(err) => Err(mj::Error::new(
                minijinja::ErrorKind::InvalidOperation,
                format!("Error while calling method: {}", format_python_error(err)),
            )),
        }
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
        let obj_ref = &self.inner as &PyObject;

        Python::with_gil(|py| {
            let mut fields: Vec<Arc<str>> = vec![];

            match obj_ref.downcast::<PyDict>(py) {
                Ok(dict_ref) => {
                    for key in dict_ref.keys() {
                        fields.push(key.to_string().into());
                    }
                }
                Err(_err) => {
                    #[cfg(debug_assertions)]
                    log::trace!("Unable to extract PyDict: {:?}", _err)
                }
            }

            fields
        })
    }
}

pub struct JinjaPythonFunction {
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

        let py_runtime = py_runtime().map_err(|err| {
            mj::Error::new(
                mj::ErrorKind::EvalBlock,
                format!("Python runtime error: {}", err),
            )
        })?;

        let call_future = py_runtime.call_async(self.inner.clone(), arguments);

        let tokio = tokio_runtime().map_err(|err| {
            mj::Error::new(
                mj::ErrorKind::EvalBlock,
                format!("Tokio runtime error: {}", err),
            )
        })?;

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
