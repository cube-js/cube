use crate::utils::bind_method;

use convert_case::{Case, Casing};
use cubesql::CubeError;
use log::error;
use neon::prelude::*;
use pyo3::exceptions::{PyNotImplementedError, PyTypeError};

use pyo3::types::{PyBool, PyFloat, PyFunction, PyInt, PyString};
use pyo3::{AsPyPointer, Py, PyAny, PyErr, PyResult, Python};

use crate::python::cross::CLRepr;
use std::cell::RefCell;
use std::collections::HashMap;

pub enum CubeConfigPyVariableValue {
    String(String),
    Number(f64),
    Bool(bool),
}

pub struct CubeConfigPy {
    dynamic_properties: Option<HashMap<String, CubeConfigPyVariableValue>>,
    query_rewrite: Option<Py<PyFunction>>,
    check_auth: Option<Py<PyFunction>>,
}

type BoxedCubeConfigPy = JsBox<RefCell<CubeConfigPy>>;

impl CubeConfigPy {
    pub fn new() -> Self {
        Self {
            dynamic_properties: Some(HashMap::new()),
            query_rewrite: None,
            check_auth: None,
        }
    }

    pub fn get_dynamic_attrs(&self) -> Vec<&'static str> {
        vec![
            "schema_path",
            "base_path",
            "web_sockets_base_path",
            "compiler_cache_size",
            "telemetry",
            "pg_sql_port",
            "cache_and_queue_driver",
            "allow_js_duplicate_props_in_schema",
            "process_subscriptions_interval",
        ]
    }

    pub fn get_query_rewrite(&self) -> Result<&Py<PyFunction>, CubeError> {
        if let Some(fun) = self.query_rewrite.as_ref() {
            Ok(fun)
        } else {
            Err(CubeError::internal(
                "Unable to reference query_rewrite, it's empty".to_string(),
            ))
        }
    }

    pub fn get_check_auth(&self) -> Result<&Py<PyFunction>, CubeError> {
        if let Some(fun) = self.check_auth.as_ref() {
            Ok(fun)
        } else {
            Err(CubeError::internal(
                "Unable to reference check_auth, it's empty".to_string(),
            ))
        }
    }

    pub fn apply_dynamic_functions(&mut self, config_module: &PyAny) -> PyResult<()> {
        self.query_rewrite = self.static_call_attr(config_module, "query_rewrite")?;
        self.check_auth = self.static_call_attr(config_module, "check_auth")?;

        Ok(())
    }

    pub fn static_call_attr<'a>(
        &mut self,
        config_module: &'a PyAny,
        key: &str,
    ) -> PyResult<Option<Py<PyFunction>>> {
        let v = config_module.getattr(&*key)?;
        if !v.is_none() {
            if v.get_type().is_subclass_of::<PyFunction>()? {
                let cb = v.downcast::<PyFunction>()?;

                let py: Py<PyFunction> = cb.into();
                return Ok(Some(py));
            } else {
                return Err(PyErr::new::<PyTypeError, _>(format!(
                    "Unsupported configuration type: {} for key: {}, must be a lambda",
                    v.get_type(),
                    key
                )));
            }
        }

        Ok(None)
    }

    pub fn dynamic_from_attr(&mut self, config_module: &PyAny, key: &str) -> PyResult<()> {
        let v = config_module.getattr(&*key)?;
        if !v.is_none() {
            let value = if v.get_type().is_subclass_of::<PyString>()? {
                CubeConfigPyVariableValue::String(v.to_string())
            } else if v.get_type().is_subclass_of::<PyBool>()? {
                CubeConfigPyVariableValue::Bool(v.downcast::<PyBool>()?.is_true())
            } else if v.get_type().is_subclass_of::<PyFloat>()? {
                let f = v.downcast::<PyFloat>()?;
                CubeConfigPyVariableValue::Number(f.value())
            } else if v.get_type().is_subclass_of::<PyInt>()? {
                let i: i64 = v.downcast::<PyInt>()?.extract()?;
                CubeConfigPyVariableValue::Number(i as f64)
            } else {
                return Err(PyErr::new::<PyTypeError, _>(format!(
                    "Unsupported configuration type: {} for key: {}",
                    v.get_type(),
                    key
                )));
            };

            let mut dynamic_properties = self.dynamic_properties.take().unwrap();
            dynamic_properties.insert(key.to_case(Case::Camel), value);

            self.dynamic_properties = Some(dynamic_properties);
        };

        Ok(())
    }
}

impl Finalize for CubeConfigPy {}

fn config_py_query_rewrite(mut cx: FunctionContext) -> JsResult<JsPromise> {
    #[cfg(build = "debug")]
    trace!("JsAsyncChannel.config_py_query_rewrite");

    let (deferred, promise) = cx.promise();
    let channel = cx.channel();

    let this = cx
        .this()
        .downcast_or_throw::<BoxedCubeConfigPy, _>(&mut cx)?;
    let query_arg = CLRepr::from_js_ref(cx.argument::<JsObject>(0)?.upcast(), &mut cx)?;
    let context_arg = CLRepr::from_js_ref(cx.argument::<JsObject>(1)?.upcast(), &mut cx)?;

    let py_method = match this.borrow().get_query_rewrite() {
        Ok(fun) => fun.clone(),
        Err(err) => return cx.throw_error(format!("{}", err)),
    };
    std::thread::spawn(move || {
        let res = Python::with_gil(|py| {
            let res = py_method.call1(py, (query_arg.into_py(py)?, context_arg.into_py(py)?))?;
            let is_coroutine = unsafe { pyo3::ffi::PyCoro_CheckExact(res.as_ptr()) == 1 };
            if is_coroutine {
                Err(PyErr::new::<PyNotImplementedError, _>(
                    "Async functions are not supported, unimplemented",
                ))
            } else {
                CLRepr::from_python_ref(res.as_ref(py))
            }
        });

        deferred.settle_with(&channel, move |mut cx| match res {
            Err(err) => {
                error!("Python error: {:?}", err);

                cx.throw_error(format!("Python error: {}", err))
            }
            Ok(r) => r.into_js(cx),
        });
    });

    Ok(promise)
}

fn config_py_check_auth(mut cx: FunctionContext) -> JsResult<JsPromise> {
    #[cfg(build = "debug")]
    trace!("JsAsyncChannel.config_py_check_auth");

    let (deferred, promise) = cx.promise();
    let channel = cx.channel();

    let this = cx
        .this()
        .downcast_or_throw::<BoxedCubeConfigPy, _>(&mut cx)?;

    let req_arg = CLRepr::from_js_ref(cx.argument::<JsObject>(0)?.upcast(), &mut cx)?;
    let authorization_arg = CLRepr::String(cx.argument::<JsString>(1)?.value(&mut cx));

    let py_method = match this.borrow().get_check_auth() {
        Ok(fun) => fun.clone(),
        Err(err) => return cx.throw_error(format!("{}", err)),
    };
    std::thread::spawn(move || {
        let res = Python::with_gil(|py| {
            let res =
                py_method.call1(py, (req_arg.into_py(py)?, &authorization_arg.into_py(py)?))?;
            let is_coroutine = unsafe { pyo3::ffi::PyCoro_CheckExact(res.as_ptr()) == 1 };
            if is_coroutine {
                Err(PyErr::new::<PyNotImplementedError, _>(
                    "Async functions are not supported, unimplemented",
                ))
            } else {
                CLRepr::from_python_ref(res.as_ref(py))
            }
        });
        deferred.settle_with(&channel, move |mut cx| match res {
            Err(err) => {
                error!("Python error: {:?}", err);

                cx.throw_error(format!("Python error: {}", err))
            }
            Ok(r) => r.into_js(cx),
        });
    });

    Ok(promise)
}

impl CubeConfigPy {
    #[allow(clippy::wrong_self_convention)]
    pub fn to_object<'a, C: Context<'a>>(mut self, cx: &mut C) -> JsResult<'a, JsObject> {
        let obj = cx.empty_object();

        let dynamic_properties = self.dynamic_properties.take().unwrap();
        for (k, v) in dynamic_properties.into_iter() {
            match v {
                CubeConfigPyVariableValue::String(v) => {
                    let js_val = JsString::new(cx, v);
                    obj.set(cx, &*k, js_val)?;
                }
                CubeConfigPyVariableValue::Number(v) => {
                    let js_val = JsNumber::new(cx, v);
                    obj.set(cx, &*k, js_val)?;
                }
                CubeConfigPyVariableValue::Bool(v) => {
                    let js_val = JsBoolean::new(cx, v);
                    obj.set(cx, &*k, js_val)?;
                }
            }
        }

        // before move
        let has_query_rewrite = self.query_rewrite.is_some();
        let has_check_auth = self.check_auth.is_some();
        // Pass JsAsyncChannel as this, because JsFunction cannot use closure (fn with move)
        let obj_this = cx.boxed(RefCell::new(self)).upcast::<JsValue>();

        if has_query_rewrite {
            let query_rewrite_fn = JsFunction::new(cx, config_py_query_rewrite)?;
            let query_rewrite = bind_method(cx, query_rewrite_fn, obj_this)?;
            obj.set(cx, "queryRewrite", query_rewrite)?;
        };

        if has_check_auth {
            let check_auth_fn = JsFunction::new(cx, config_py_check_auth)?;
            let check_auth = bind_method(cx, check_auth_fn, obj_this)?;
            obj.set(cx, "checkAuth", check_auth)?;
        };

        Ok(obj)
    }
}
