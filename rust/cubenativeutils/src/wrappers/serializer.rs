use super::context::NativeContext;
use super::object::NeonObject;
use crate::wrappers::object::NativeObject;
use crate::wrappers::object::NativeObjectHolder;
use cubesql::CubeError;
use serde::Serialize;
use std::rc::Rc;

/* pub trait NativeSerialize {
    fn to_native(&self, cx: Rc<dyn NativeContext>) -> Result<Rc<dyn NativeObject>, CubeError>;
}

impl<T: Serialize> NativeSerialize for T {
    fn to_native(&self, cx: Rc<dyn NativeContext>) -> Result<Rc<dyn NativeObject>, CubeError> {
        cx.to_native_object(self)
    }
} */
