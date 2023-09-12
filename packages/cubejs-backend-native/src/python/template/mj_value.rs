use crate::python::cross::{CLRepr, CLReprKind, CLReprObject};
use crate::python::runtime::py_runtime;
use crate::tokio_runtime;
use minijinja as mj;
use minijinja::value::{Object, ObjectKind, SeqObject, StructObject, Value, ValueKind};
use pyo3::types::PyFunction;
use pyo3::Py;
use std::convert::TryInto;
use std::sync::Arc;

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

struct JinjaDynamicObject {
    pub(crate) inner: CLReprObject,
}

impl std::fmt::Debug for JinjaDynamicObject {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Debug::fmt(&self.inner, f)
    }
}

impl std::fmt::Display for JinjaDynamicObject {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(&self.inner, f)
    }
}

impl StructObject for JinjaDynamicObject {
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

impl Object for JinjaDynamicObject {
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
                let v = if let Ok(v) = from.get_item(&key) {
                    from_minijinja_value(&v)?
                } else {
                    CLRepr::Null
                };

                obj.insert(
                    key.as_str().expect("must be a string").to_string(),
                    v,
                );
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
        CLRepr::Array(inner) => Value::from_seq_object(JinjaSequenceObject { inner }),
        CLRepr::Object(inner) => Value::from_object(JinjaDynamicObject { inner }),
        CLRepr::String(v) => Value::from(v),
        CLRepr::Float(v) => Value::from(v),
        CLRepr::Int(v) => Value::from(v),
        CLRepr::Bool(v) => Value::from(v),
        CLRepr::Null => Value::from(()),
        CLRepr::PyExternalFunction(inner) | CLRepr::PyFunction(inner) => {
            Value::from_object(JinjaPythonFunction { inner })
        }
        CLRepr::JsFunction(_) => panic!(
            "Converting from {:?} to minijinja::Value is not supported",
            CLReprKind::JsFunction
        ),
    }
}
