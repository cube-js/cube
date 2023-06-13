use crate::python::cross::{CLRepr, CLReprObject};
use cubesql::CubeError;
use minijinja as mj;
use minijinja::value::{Object, ObjectKind, SeqObject, StructObject, Value, ValueKind};
use std::fmt::{Display, Formatter};

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
        // TODO(ovr): Handle unwrap?
        self.inner
            .get(name)
            .map(|v| to_minijinja_value(v.clone()).unwrap())
    }
}

impl Object for JinjaDynamicObject {
    fn kind(&self) -> ObjectKind<'_> {
        ObjectKind::Struct(self)
    }

    fn call(&self, state: &mj::State, args: &[Value]) -> Result<Value, mj::Error> {
        Err(mj::Error::new(
            minijinja::ErrorKind::InvalidOperation,
            "insecure call",
        ))
    }

    fn call_method(
        &self,
        state: &mj::State,
        name: &str,
        args: &[Value],
    ) -> Result<Value, mj::Error> {
        Err(mj::Error::new(
            minijinja::ErrorKind::InvalidOperation,
            "insecure method call",
        ))
    }
}

pub fn to_minijinja_value(from: CLRepr) -> Result<Value, CubeError> {
    match from {
        CLRepr::Object(inner) => Ok(Value::from_object(JinjaDynamicObject { inner })),
        CLRepr::Float(v) => Ok(Value::from(v)),
        CLRepr::Int(v) => Ok(Value::from(v)),
        CLRepr::Bool(v) => Ok(Value::from(v)),
        other => Err(CubeError::internal(format!(
            "Converting from {:?} to minijinja::Value is not supported",
            other.kind()
        ))),
    }
}
