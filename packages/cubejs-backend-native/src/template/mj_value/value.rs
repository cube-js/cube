use crate::cross::{CLRepr, CLReprObject};
use crate::template::mj_value::to_minijinja_value;
use minijinja as mj;
use minijinja::value::{Object, ObjectKind, SeqObject, StructObject, Value};
use std::sync::Arc;

#[derive(Debug)]
pub struct JinjaSequenceObject {
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

pub struct JinjaDictObject {
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
