use crate::python::cross::{CLRepr, CLReprKind, CLReprObject};
use cubesql::CubeError;
use minijinja as mj;
use minijinja::value::{Object, ObjectKind, SeqObject, StructObject, Value, ValueKind};
use std::fmt::{Display, Formatter, Pointer};

#[derive(Debug)]
struct JinjaDynamicObject {
    pub(crate) inner: CLReprObject,
}

impl Display for JinjaDynamicObject {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(&self.inner, f)
    }
}

impl StructObject for JinjaDynamicObject {
    fn get_field(&self, name: &str) -> Option<Value> {
        self.inner.get(name).map(|v| to_minijinja_value(v.clone()))
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

impl Display for JinjaSequenceObject {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
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
        // TODO(ovr): fix me
        CLRepr::Null => panic!(
            "Converting from {:?} to minijinja::Value is not supported",
            CLReprKind::Null
        ),
        CLRepr::PyFunction(_) => panic!(
            "Converting from {:?} to minijinja::Value is not supported",
            CLReprKind::PyFunction
        ),
    }
}
