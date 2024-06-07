use crate::cross::CLRepr;
use crate::python::runtime::py_runtime;
use neon::prelude::*;
use pyo3::types::PyFunction;
use pyo3::Py;
use std::cell::RefCell;

pub struct JsPyFunctionWrapper {
    fun: Py<PyFunction>,
    _fun_name: Option<String>,
}

impl JsPyFunctionWrapper {
    pub fn new(fun: Py<PyFunction>, _fun_name: Option<String>) -> Self {
        Self { fun, _fun_name }
    }

    pub fn get_fun(&self) -> &Py<PyFunction> {
        &self.fun
    }
}

impl Finalize for JsPyFunctionWrapper {}

pub type BoxedJsPyFunctionWrapper = JsBox<RefCell<JsPyFunctionWrapper>>;

pub fn cl_repr_py_function_wrapper(mut cx: FunctionContext) -> JsResult<JsPromise> {
    #[cfg(build = "debug")]
    trace!("cl_repr_py_function_wrapper {}", _fun_name);

    let (deferred, promise) = cx.promise();

    let this = cx
        .this::<JsValue>()?
        .downcast_or_throw::<BoxedJsPyFunctionWrapper, _>(&mut cx)?;

    let mut arguments = Vec::with_capacity(cx.len() as usize);

    for arg_idx in 0..cx.len() {
        arguments.push(CLRepr::from_js_ref(
            cx.argument::<JsValue>(arg_idx)?,
            &mut cx,
        )?);
    }

    let py_method = this.borrow().fun.clone();
    let py_runtime = match py_runtime() {
        Ok(r) => r,
        Err(err) => return cx.throw_error(format!("Unable to init python runtime: {:?}", err)),
    };
    py_runtime.call_async_with_promise_callback(py_method, arguments, deferred);

    Ok(promise)
}
