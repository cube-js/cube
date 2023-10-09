use neon::prelude::*;

use crate::cross::{CLRepr, CLReprObject};

pub struct CubePythonModel {
    functions: CLReprObject,
}

impl CubePythonModel {
    pub fn new(functions: CLReprObject) -> Self {
        Self { functions }
    }
}

impl Finalize for CubePythonModel {}

impl CubePythonModel {
    #[allow(clippy::wrong_self_convention)]
    pub fn to_object<'a, C: Context<'a>>(self, cx: &mut C) -> JsResult<'a, JsValue> {
        CLRepr::Object(self.functions).into_js(cx)
    }
}
