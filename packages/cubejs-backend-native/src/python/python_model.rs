use neon::prelude::*;

use crate::cross::{CLRepr, CLReprObject, CLReprObjectKind};

pub struct CubePythonModel {
    functions: CLReprObject,
    variables: CLReprObject,
    filters: CLReprObject,
}

impl CubePythonModel {
    pub fn new(functions: CLReprObject, variables: CLReprObject, filters: CLReprObject) -> Self {
        Self {
            functions,
            variables,
            filters,
        }
    }
}

impl Finalize for CubePythonModel {}

impl CubePythonModel {
    #[allow(clippy::wrong_self_convention)]
    pub fn to_object<'a, C: Context<'a>>(self, cx: &mut C) -> JsResult<'a, JsValue> {
        let mut obj = CLReprObject::new(CLReprObjectKind::Object);
        obj.insert("functions".to_string(), CLRepr::Object(self.functions));
        obj.insert("variables".to_string(), CLRepr::Object(self.variables));
        obj.insert("filters".to_string(), CLRepr::Object(self.filters));

        CLRepr::Object(obj).into_js(cx)
    }
}
