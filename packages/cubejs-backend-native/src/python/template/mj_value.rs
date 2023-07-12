use crate::python::cross::{CLRepr, CLReprKind, CLReprObject};
use crate::python::runtime::py_runtime;
use crate::tokio_runtime;
use minijinja as mj;
use minijinja::value::{Object, ObjectKind, SeqObject, StructObject, Value};
use pyo3::types::PyFunction;
use pyo3::Py;
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
        if args.len() > 0 {
            return Err(mj::Error::new(
                mj::ErrorKind::EvalBlock,
                "Passing argument to python functions is not supported".to_string(),
            ));
        }

        let py_runtime = py_runtime()
            .map_err(|err| mj::Error::new(mj::ErrorKind::EvalBlock, format!("Error: {}", err)))?;
        let call_future = py_runtime.call_async(self.inner.clone(), vec![]);

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

    fn fields(&self) -> Vec<Arc<String>> {
        self.inner
            .iter()
            .map(|(k, _)| Arc::new(k.clone()))
            .collect()
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
