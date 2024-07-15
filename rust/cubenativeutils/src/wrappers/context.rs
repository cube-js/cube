use super::object::NativeObject;
use cubesql::CubeError;
use serde::Serialize;
use std::rc::Rc;
pub trait NativeContext {
    fn to_native_object<T: Serialize>(&self, v: &T) -> Result<Rc<dyn NativeObject>, CubeError>;
}
